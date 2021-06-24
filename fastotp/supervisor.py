import os
import multiprocessing
import threading
import structlog
import queue
import arrow
import signal
import inspect

from collections import defaultdict
from .priorityqueue import MultiPocessingPriorityQueue
from .task import task_wrapper, Task, iotask_wrapper, cputask_wrapper  
from .service import ServiceMessage
from .errors import log_error

WORKER_CORES = os.environ.get('WORKER_CORES', max(multiprocessing.cpu_count() - 1, 1))
WORKER_THREADS_PER_CORE = os.environ.get('WORKER_THREADS_PER_CORE', 128)
SCHEDULER_QUEUE_PRIORITIES = os.environ.get('SCHEDULER_QUEUE_PRIORITIES', 10)

JOB_QUEUE = queue.PriorityQueue()

def setup_otp_supervisor(app, **kwargs):
    app.on_event("startup")(supervisor_initiator(**kwargs))

def supervisor_initiator(**kwargs):
    def wrapper():
        t = threading.Thread(target=supervisor, args=(JOB_QUEUE,), kwargs=kwargs)
        t.daemon = True
        t.start()
        return
    return wrapper

def get_new_logger():
    return structlog.get_logger()

def queue_service_request(service_name=None, priority=None, args=None, kwargs=None):
    JOB_QUEUE.put(ServiceMessage(service_name=service_name, priority=priority, args=args or (), kwargs=kwargs or {}))

def queue_task(func, args=None, kwargs=None, log_bindings=None, blocking_perc=16, priority=1):
    JOB_QUEUE.put(task_wrapper(func, args, kwargs, log_bindings, blocking_perc, None, priority))

def queue_iotask(func, args=None, kwargs=None, log_bindings=None, priority=1):
    JOB_QUEUE.put(iotask_wrapper(func, args, kwargs, log_bindings, priority))

def queue_cputask(func, args=None, kwargs=None, log_bindings=None, priority=1):
    JOB_QUEUE.put(cputask_wrapper(func, args, kwargs, log_bindings, priority))

def run_blocking_task(func, args=None, kwargs=None, log_bindings=None, blocking_perc=16, priority=1):
    started = arrow.utcnow()
    manager = multiprocessing.Manager()
    q = manager.Queue()
    JOB_QUEUE.put(task_wrapper(func, args, kwargs, log_bindings, blocking_perc, q, priority))
    result = q.get()
    if isinstance(result, Exception):
        raise result
    return result

class GracefulExit(Exception):
    pass

def signal_handler(signum, frame):
    raise GracefulExit()

def supervisor(jobs, worker_cores=None, worker_threads_per_core=None, services=None, extra_args=None, **kwargs):
    services = services or []
    extra_args = extra_args or {}
    log = get_new_logger().bind(type="service", service="otpsupervisor")
    log.info("Starting up OTP Scheduler")
    worker_cores = worker_cores or WORKER_CORES
    worker_threads_per_core = worker_threads_per_core or WORKER_THREADS_PER_CORE
    worker_service_assignments = defaultdict(list)
    worker_demand = defaultdict(int)
    service_map = {}

    log.info("Organizing Services")
    perc_used = 0.0
    w_iter = 0
    for service in services:
        if worker_demand[w_iter] + service.blocking_perc < worker_threads_per_core:
            worker_demand[w_iter] += service.blocking_perc
            worker_service_assignments[w_iter].append(service)
            service_map[service.name] = w_iter
        elif w_iter + 1 < WORKER_CORES:
            w_iter += 1
            worker_demand[w_iter] += service.blocking_perc
            worker_service_assignments[w_iter].append(service)
            service_map[service.name] = w_iter
        else:
            raise Exception("Not enough capacity to schedule provided services")            
    
    log.info("Worker Cores")
    worker_job_queues = {}
    worker_capacity_queues = {}
    worker_processes = []
    for w in range(worker_cores):
        q = multiprocessing.Queue()
        o = multiprocessing.Queue(maxsize=worker_threads_per_core)
        for i in range(worker_demand[w]):
            o.put(True)
        worker_job_queues[w] = q
        worker_capacity_queues[w] = o
        p = multiprocessing.Process(target=core_worker, args=(w, q, o, worker_service_assignments[w], extra_args))
        p.daemon = True
        p.start()
        worker_processes.append(p)

    log.info("Polling Dispatch Queue")
    while True:
        try:
            job = jobs.get()
            if isinstance(job, Task):
                delegated = False
                if job.blocking_perc < min([worker_threads_per_core - wd for wd in worker_demand.values()], default=worker_threads_per_core):
                    for worker_id, worker_capacity in dict(sorted(list(worker_capacity_queues.items()), reverse=True, key=lambda x: x[1].qsize())).items():
                        if worker_threads_per_core - worker_capacity.qsize() > job.blocking_perc:
                            for i in range(job.blocking_perc): # Note there is potential here for a deadlock if multiple workers fill up the queue before it can execute, possible reason just make the Queue's infinte
                                worker_capacity_queues[worker_id].put(True)
                            worker_job_queues[w].put(job)
                            delegated = True
                            break
                if not delegated:
                    log.warning("Submitted job requested more capacity than possible to schedule")
                    time.sleep(1)
                    jobs.put(job)
            elif isinstance(job, ServiceMessage):
                worker_job_queues[service_map[job.service_name]].put(job)
            else:
                log.warning(f"Received unknown message of type {type(job)}: {job}")
        except (KeyboardInterrupt, SystemExit, GracefulExit):
            log.warning("Supervisor Exiting...")
            for p in worker_processes:
                p.terminate()
            break


def core_worker(worker_id, job_queue, termination_queue, service_lst, extra_args):
    signal.signal(signal.SIGTERM, signal_handler)
    log = get_new_logger().bind(worker_id=f"cw_{worker_id}", type="worker", service="otpcoreworker")
    log.info("Setting up Services")
    service_queues = {}
    services = {}
    service_params = {}
    for service in service_lst:
        log.info(f"Scheduled service {service.name} onto worker {worker_id}")
        service_job_queue = queue.Queue()
        t = threading.Thread(target=service_worker, args=(service, service_job_queue)) 
        t.daemon = True
        t.start()
        service_queues[service.name] = service_job_queue
        services[service.name] = t
        service_params[service.name] = service
    log.info("Computing Extra Args")
    extra_args = {k: f() for k, f in extra_args.items()}
    log.info("Polling for Jobs")
    while True:
        try:
            job = job_queue.get()
            for service_name, service_thread in services.items():
                if not service_thread.is_alive():
                    t = threading.Thread(target=service_worker, args=(service_params[service_name], service_queues[service_name])) 
                    t.daemon = True
                    t.start()
                    services[service_name] = t       
            if isinstance(job, Task):
                t = threading.Thread(target=thread_worker, args=(log, job, termination_queue, extra_args)) 
                t.daemon = True
                t.start()
            elif isinstance(job, ServiceMessage):
                if service_queues.get(job.service_name):
                    service_queues[job.service_name].put(job)
                else:
                    log.warning(f"No service running for {job.service_name}")
        except GracefulExit:
            log.warning("Core Worker Exiting...")
            break


def get_function_kwargs(func):
    args, varargs, varkw, defaults = inspect.getargspec(func)
    return args[-len(defaults):] if defaults else []

def thread_worker(l, task, termination_queue, extra_args):
    log = l.bind(task=copy.deepcopy(task.func.__name__), task_type="blocking" if task.sink else "async")
    if not "log" in extra_args:
        extra_args["log"] = log
    kwargs = {}
    for kwarg in get_function_kwargs(task.func):
        if kwarg in extra_args:
            kwargs[kwarg] = extra_args[kwarg] 
    kwargs.update(task.kwargs or {})
    if task.sink:
        try:     
            task.sink.put(task.func(*task.args, **kwargs))
        except Exception as e:
            log_error(e, log)
            task.sink.put(e)                     
    else:
        try:    
            task.func(*task.args, **task.kwargs)
        except Exception as e:
            log_error(e, log)        
    for i in range(task.blocking_perc):
        termination_queue.get()

def service_worker(service, job_queue):
    log = get_new_logger().bind(type="service", service=service.name)
    service._set_logger(log)
    with service:
        log.info("Polling for Service Requests")
        while True:
            job = job_queue.get()
            service.run(*job.args, **job.kwargs)
