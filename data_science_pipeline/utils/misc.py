from typing import List, T


def identity_fn(x: T) -> T:
    return x


def iter_batches(long_list: List[T], batch_size: int) -> List[List[T]]:
    offset = 0
    while offset < len(long_list):
        yield long_list[offset:min(offset + batch_size, len(long_list))]
        offset += batch_size
