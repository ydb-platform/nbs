import time

from functools import wraps


def retry(tries=1, delay=1, multiplier=1, exception=Exception):
    def deco_retry(func):
        @wraps(func)
        def func_retry(*args, **kwargs):
            my_tries, my_delay = tries, delay
            while my_tries > -1:
                try:
                    return func(*args, **kwargs)
                except exception as err:
                    my_tries -= 1
                    if my_tries == -1:
                        raise err
                    time.sleep(my_delay)
                    my_delay *= multiplier
        return func_retry
    return deco_retry


def timed_retry(timeout=120, delay=1, backoff=1, exception=Exception):
    def deco_retry(func):
        @wraps(func)
        def func_retry(*args, **kwargs):
            nonlocal delay
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    return func(*args, **kwargs)
                except exception:
                    if time.time() + delay < deadline:
                        time.sleep(delay)
                    else:
                        raise
                delay *= backoff
        return func_retry
    return deco_retry
