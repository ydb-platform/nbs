import threading


class Scheduler(object):

    def __init__(self):
        self.__next_timer_id = 0
        self.__timers = {}

    def schedule(self, delay, callback):
        timer_id = self.__next_timer_id
        self.__next_timer_id += 1

        def callback_wrapper():
            callback()
            self.__timers.pop(timer_id, None)

        timer = threading.Timer(delay, callback_wrapper)
        self.__timers[timer_id] = timer
        timer.start()
        return timer_id

    def cancel(self, timer_id):
        timer = self.__timers.get(timer_id)
        if timer is None:
            return
        timer.cancel()
        self.__timers.pop(timer_id)

    def cancel_all(self):
        for timer in self.__timers.values():
            timer.cancel()
        self.__timers = {}
        self.__next_timer_id = 0
