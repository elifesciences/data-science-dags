from typing import Iterable
import pandas as pd


def get_author_ids_of_given_version_of_paper(
    manuscript_df: pd.DataFrame,
    version_id: str
) -> Iterable[str]:
    prediction_manuscript_version_df = manuscript_df.loc[
        manuscript_df['version_id'] == version_id
    ]
    return prediction_manuscript_version_df['author_person_ids'].values[0]
