from typing import Callable

from pydbsp.zset import ZSet


def select[T](z: ZSet[T], pred: Callable[[T], bool]) -> ZSet[T]:
    return ZSet({k: v for k, v in z.inner.items() if pred(k)})


def project[A, B](z: ZSet[A], f: Callable[[A], B]) -> ZSet[B]:
    out: dict[B, int] = {}
    for k, v in z.inner.items():
        key = f(k)
        out[key] = out.get(key, 0) + v
    return ZSet({k: v for k, v in out.items() if v != 0})


def flatmap[A, B](z: ZSet[A], f: Callable[[A], ZSet[B]]) -> ZSet[B]:
    """Apply ``f`` to every element of ``z``, producing a ``ZSet[B]`` per
    element, then merge all results by summing weights.

    The weight of each output element is the product of the input element's
    weight and its weight in ``f(element)``. This makes ``flatmap`` linear in
    the Z-set group structure (scalar-multiplication distributes), so the
    operator does not require any Integrate/Differentiate wiring.

    Example::

        z = ZSet({1: 2, 3: 1})
        f = lambda x: ZSet({x: 1, x * 10: 1})
        flatmap(z, f)  # ZSet({1: 2, 10: 2, 3: 1, 30: 1})
    """
    out: dict[B, int] = {}
    for elem, weight in z.inner.items():
        for b, b_weight in f(elem).inner.items():
            combined = weight * b_weight
            out[b] = out.get(b, 0) + combined
    return ZSet({k: v for k, v in out.items() if v != 0})
