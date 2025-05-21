import io
from config.db_config import get_postgres_engine
from sqlalchemy import text

def insert_into_postgres(df, table_name,chunksize=1000):
    engine = get_postgres_engine()
    df.to_sql(
        table_name, 
        engine, 
        schema="staging", 
        if_exists="append", 
        index=False, 
        chunksize=chunksize,
        method="multi"
        )

def fast_copy_insert(df, table_name):
    engine = get_postgres_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Exportar el DataFrame a CSV en memoria
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Ejecutar COPY
    cursor.copy_expert(f"""
        COPY staging.{table_name}
        FROM STDIN WITH CSV
    """, buffer)

    conn.commit()
    cursor.close()
    conn.close()

def execute_postgres_query(sql):
    engine = get_postgres_engine()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()