from typing import TypeVar


T = TypeVar('T')


def identity_fn(x: T) -> T:
    return x
