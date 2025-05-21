import pandas as pd
from config.db_config import get_sqlserver_engine

def get_data_with_query(query):
    engine = get_sqlserver_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df
