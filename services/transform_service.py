def normalize_column_names(df):
    df.columns = [col.lower() for col in df.columns]
    return df
