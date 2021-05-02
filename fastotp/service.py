import structlog

from collections import namedtuple

class Service(object):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a __init__ method")

    def _set_logger(self, log):
        self.log = log

    @property
    def blocking_perc():
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a blocking_perc property")

    @staticmethod
    def name():
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a name staticmethod")

    def bootstrap():
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a bootstrap method")

    def run(self, *args, **kwargs):
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a run method")

    def teardown(self, *args, **kwargs):
        raise NotImplementedError("Service Interface has not been fully implemented, please implement a teardown method")

    def __enter__(self):
        self.bootstrap()

    def __exit__(self, type, value, traceback):
        self.teardown()


ServiceMessage = namedtuple('ServiceMessage', ['priority', 'args', 'kwargs', 'service_name'])


class HelloWorldService(Service):
    def __init__(self, *args, **kwargs):
        pass

    @property
    def blocking_perc(self):
        return 2

    @property
    def name(self):
        return "HelloWorld"

    def bootstrap(self):
        self.log.info("Service is running")

    def run(self, text, **kwargs):
        self.log.info(text, **kwargs)

    def teardown(self, *args, **kwargs):
        pass

    def __enter__(self):
        self.bootstrap()

    def __exit__(self, type, value, traceback):
        self.teardown()
