import functools
import gzip
from io import StringIO
from typing import Callable, Optional, Any

import numpy as np
import pandas as pd

from data_science_pipeline.utils.io import (
    get_path,
    read_bytes,
    write_bytes
)


def isnull(value: Any) -> bool:
    # Note: this handles the following exception just using pd.isnull:
    #  ValueError: The truth value of an array with more than one element
    #     is ambiguous.
    if not isinstance(value, (list, set, np.ndarray)) and pd.isnull(value):
        return True
    return False


def wrap_fn_or_none(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapper(*args):
        for arg in args:
            if isnull(arg):
                return None
        return fn(*args)
    return wrapper


def apply_skip_null(
        ser: pd.Series,
        func: Callable,
        *args,
        **kwargs) -> pd.Series:
    return ser.apply(wrap_fn_or_none(func), *args, **kwargs)


def get_filepath_csv_separator(filepath: str):
    filepath = str(filepath)
    if filepath.endswith('.tsv') or filepath.endswith('.tsv.gz'):
        return '\t'
    return ','


def read_csv(
        filepath: str,
        sep: Optional[str] = None,
        compression: str = 'infer',
        encoding: str = 'utf-8',
        **kwargs) -> pd.DataFrame:
    if sep is None:
        sep = get_filepath_csv_separator(filepath)
    if compression == 'infer' and filepath.endswith('.gz'):
        compression = 'gzip'
    data = read_bytes(filepath)
    if compression == 'gzip':
        data = gzip.decompress(data)
    return pd.read_csv(StringIO(data.decode(encoding=encoding)), sep=sep, **kwargs)


def to_csv(
        df: pd.DataFrame,
        filepath: str,
        sep: Optional[str] = None,
        index: bool = False,
        compression: str = 'infer',
        encoding: str = 'utf-8',
        **kwargs):
    if sep is None:
        sep = get_filepath_csv_separator(filepath)

    get_path(filepath).parent.mkdir(parents=True, exist_ok=True)
    buffer = StringIO()
    df.to_csv(buffer, sep=sep, index=index, **kwargs)
    if compression == 'infer' and filepath.endswith('.gz'):
        compression = 'gzip'
    data = buffer.getvalue().encode(encoding)
    if compression == 'gzip':
        data = gzip.compress(data)
    write_bytes(filepath, data)


def dataframe_chunk(seq, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos:pos + size]
