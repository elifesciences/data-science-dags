import os
from pathlib import Path


SQL_DIR = os.path.dirname(__file__)
SQL_PATH = Path(SQL_DIR)


def get_sql(filename: str) -> str:
    return SQL_PATH.joinpath(filename).read_text()
