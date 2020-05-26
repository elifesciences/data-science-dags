import numpy as np
import pandas as pd


def isnull(value: any) -> bool:
    # Note: this handles the following exception just using pd.isnull:
    #  ValueError: The truth value of an array with more than one element
    #     is ambiguous.
    if not isinstance(value, (list, set, np.ndarray)) and pd.isnull(value):
        return True
    return False
