from concurrent.futures import Future


def unit(x):
    f = Future()
    f.set_result(x)
    return f


def bind(future, callback):
    """
    Applies function to the future result when it's ready, or propagates an exception
    """
    result = Future()

    def set_result(f):
        try:
            result.set_result(f.result())
        except Exception:
            result.set_exception(f.exception())

    def apply_callback(f):
        try:
            future = callback(f.result())
            future.add_done_callback(set_result)
        except Exception:
            result.set_exception(f.exception())

    future.add_done_callback(apply_callback)

    return result
