from typing import Optional
import pandas as pd

from IPython.display import display, Markdown


def to_markdown_sql(sql: str):
    return '> ```sql\n> %s\n> ```' % '\n> '.join(sql.splitlines())


def printmd(s: str):
    try:
        s = s.encode().decode('unicode_escape')
    except AttributeError:
        pass
    display(Markdown(s))


def read_big_query(
        query: str,
        project_id: Optional[str] = None,
        show_query: bool = True,
        **kwargs) -> pd.DataFrame:
    if show_query:
        printmd(to_markdown_sql(query))
    return pd.io.gbq.read_gbq(
        query,
        project_id=project_id,
        dialect='standard',
        **kwargs
    )
