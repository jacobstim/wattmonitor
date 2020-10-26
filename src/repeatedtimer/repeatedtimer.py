# Original: https://stackoverflow.com/a/52255230
# Usage: https://stackoverflow.com/a/40965385
import logging
import threading
import time
import traceback


class RepeatedTimer(object):

    def __init__(self, first_interval, interval, func, *args, **kwargs):
        self.timer      = None
        self.first_interval = first_interval
        self.interval   = interval
        self.func   = func
        self.args       = args
        self.kwargs     = kwargs
        self.running = False
        self.is_started = False
        self.latency_seconds = 0


    def first_start(self):
        try:
            # no race-condition here because only control thread will call this method
            # if already started will not start again
            if not self.is_started:
                self.is_started = True
                self.timer = threading.Timer(self.first_interval, self.run)
                self.running = True
                self.timer.start()
        except Exception as e:
            logging.error("Timer first_start failed: " + str(e))
            raise

    def run(self):
        # if not stopped start again
        now = time.time()
        if self.running:
            self.timer = threading.Timer(self.interval - self.latency_seconds , self.run)
            self.timer.start()
            self.latency_seconds = time.time() - now            # All the restarting takes a few milliseconds, this compensates for it
        self.func(*self.args, **self.kwargs)

    def stop(self):
        # cancel current timer in case failed it's still OK
        # if already stopped doesn't matter to stop again
        if self.timer:
            self.timer.cancel()
        self.running = False