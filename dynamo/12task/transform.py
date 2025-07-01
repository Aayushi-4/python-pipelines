from extract1 import extract_item
import pandas as pd

def transform_data(table):
    df = extract_item(table)
    # sorting
    df_sorted = df.sort_values(by="project_id")
    #exploading
    df_sorted["technologies"] = df_sorted["technologies"].apply(
    lambda x: x.split(", ") if isinstance(x, str) else x
)
    df_explode = df_sorted.explode("technologies")
    df_explode["technologies"] = df_explode["technologies"].apply(lambda x: str(x).strip("[]'\" "))

    return df_explode