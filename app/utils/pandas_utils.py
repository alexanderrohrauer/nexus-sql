import pandas as pd


def read_from_model_list(res: list) -> pd.DataFrame:
    return pd.DataFrame([x.model_dump() for x in res])
