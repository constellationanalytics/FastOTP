import os
import structlog
import random
from collections import namedtuple

WORKER_THREADS_PER_CORE = os.environ.get('WORKER_THREADS_PER_CORE', 128)

Task = namedtuple('Task', ['priority', 'blocking_perc', 'random', 'func', 'args', 'kwargs', 'log_bindings', 'sink'])

def task_wrapper(func, args=None, kwargs=None, log_bindings=None, blocking_perc=16, sink=None, priority=1):
    args = args or ()
    kwargs = kwargs or {}
    log_bindings = log_bindings or {}
    return Task(
        func=func,
        args=args,
        kwargs=kwargs,
        random=random.random(),
        log_bindings=log_bindings,
        blocking_perc=blocking_perc,
        priority=priority,
        sink=sink
    )

def iotask_wrapper(func, args=None, kwargs=None, log_bindings=None, priority=1):
    return task_wrapper(func, args, kwargs, log_bindings, 1, priority=priority)

def cputask_wrapper(func, args=None, kwargs=None, log_bindings=None, priority=1):
    return task_wrapper(func, args, kwargs, log_bindings, WORKER_THREADS_PER_CORE // 2, priority=priority) 

