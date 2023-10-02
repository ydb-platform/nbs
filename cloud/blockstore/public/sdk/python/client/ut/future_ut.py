from concurrent.futures import Future
from cloud.blockstore.public.sdk.python.client.future import unit, bind


def test_associativity():
    foo = unit(97)
    bar = lambda x: unit(x + x)
    qux = lambda x: unit(x / 3)

    assert bind(bind(foo, bar), qux).result() == \
            bind(foo, lambda x: bind(bar(x), qux)).result()


def test_exception():
    ok = unit("ok")

    error = Future()
    error.set_exception(Exception("error"))

    assert bind(error, lambda x: ok).exception()
    assert bind(ok, lambda x: error).exception()
    assert bind(ok, lambda x: ok).exception() is None
