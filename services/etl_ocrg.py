
from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import execute_postgres_query, insert_into_postgres
from services.transform_service import normalize_column_names

def run_ocrg_etl():
    """Ejecuta el ETL para OCRG."""
    query = """
        SELECT GroupCode, GroupName, GroupType, Locked  
        FROM OCRG
    """
    
    print("Extrayendo datos de OCRG...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)
    
    # Paso 1: Truncar staging
    print("Truncando staging.dim_ocrg...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_ocrg;")

    # Paso 2: Insertar en staging
    print("Insertando en staging.dim_ocrg...")
    insert_into_postgres(df, "dim_ocrg") 

    # Paso 3: Truncar warehouse
    print("Truncando warehouse.dim_ocrg...")
    execute_postgres_query("TRUNCATE TABLE warehouse.dim_ocrg;")

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.dim_ocrg...")
    insert_query = """
    INSERT INTO warehouse.dim_ocrg
    SELECT * FROM staging.dim_ocrg;
    """
    execute_postgres_query(insert_query)

    # Paso 5: Limpiar staging
    print("Limpiando staging.dim_ocrg...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_ocrg;")

    print("ETL de OCRG completado con limpieza y actualización de warehouse.")
