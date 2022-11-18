from itertools import islice
from typing import Iterable, List, TypeVar

T = TypeVar('T')


def identity_fn(x: T) -> T:
    return x


def iter_batches_list(long_list: List[T], batch_size: int) -> Iterable[List[T]]:
    offset = 0
    while offset < len(long_list):
        yield long_list[offset:min(offset + batch_size, len(long_list))]
        offset += batch_size


def iter_batches_iterable(
        long_list: Iterable[T],
        batch_size: int) -> Iterable[Iterable[T]]:
    it = iter(long_list)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            return
        yield batch


def iter_batches(long_list: Iterable[T], batch_size: int) -> Iterable[Iterable[T]]:
    if isinstance(long_list, list):
        return iter_batches_list(long_list, batch_size)
    return iter_batches_iterable(long_list, batch_size)
