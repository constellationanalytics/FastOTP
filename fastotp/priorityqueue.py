import base64

from time import sleep
from datetime import datetime
from queue import Empty
from faster_fifo import Queue as ProcessQueue
#from multiprocessing import Queue as ProcessQueue

class MultiPocessingPriorityQueue(object):
    '''
    Simple priority queue that works with multiprocessing. Only a finite number 
    of priorities are allowed. Adding many priorities slow things down. 

    Also: no guarantee that this will pull the highest priority item 
    out of the queue if many items are being added and removed. Race conditions
    exist where you may not get the highest priority queue item.  However, if 
    you tend to keep your queues not empty, this will be relatively rare.
    '''
    def __init__(self, num_priorities=1, default_sleep=.2):
        self.queues = []
        self.default_sleep = default_sleep
        for i in range(0, num_priorities):
            self.queues.append(ProcessQueue())

    def __repr__(self):
        priors = ", ".join(map(lambda i, q: "%d:%d"%(i, q.qsize()), enumerate(self.queues)))
        return f"<Queue with {len(self.queues)} priorities, sizes: {priors}> "


    qsize = lambda self: sum(map(lambda q: q.qsize(), self.queues))

    def get(self, block=True, timeout=None):
        start = datetime.utcnow()
        while True:
            for q in self.queues:
                try:
                    return q.get(block=False)
                except Empty:
                    pass
            if not block:
                raise Empty
            if timeout and (datetime.utcnow()-start).total_seconds > timeout:
                raise Empty

            if timeout:
                time_left = (datetime.utcnow()-start).total_seconds - timeout
                sleep(time_left/4)
            else:
                sleep(self.default_sleep)

    get_nowait = lambda self: self.get(block=False)

    def put(self, priority, obj, block=False, timeout=None):
        if priority < 0 or priority >= len(self.queues):
            raise Exception("Priority %d out of range."%priority)
        # Block and timeout don't mean much here because we never set maxsize
        return self.queues[priority].put(obj, block=block, timeout=timeout)

