from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import insert_into_postgres, execute_postgres_query
from services.transform_service import normalize_column_names

def run_ocrd_etl():
    """Ejecuta el ETL para OCRD con una consulta específica."""
    query = """
    SELECT CardCode, CardName, CardType, GroupCode, CreateDate, UpdateDate  
    FROM OCRD
    WHERE CAST(CreateDate AS DATE) >= CAST(DATEADD(DAY, -120, GETDATE()) AS DATE)
    OR CAST(UpdateDate AS DATE) >= CAST(DATEADD(DAY, -120, GETDATE()) AS DATE)
    """
    
    print("Extrayendo datos de OCRD...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)

    # Paso 1: Truncar staging
    print("Truncando staging.dim_ocrd...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_ocrd;")
    
    # Paso 2: Insertar en staging
    print("Insertando en staging.dim_ocrd...")
    insert_into_postgres(df, "dim_ocrd")

    # Paso 3: Eliminar de warehouse por CardCode
    print("Eliminando datos existentes en warehouse.dim_ocrd...")
    delete_query = """
    DELETE FROM warehouse.dim_ocrd
    WHERE cardcode IN (SELECT cardcode FROM staging.dim_ocrd);
    """
    execute_postgres_query(delete_query)

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.dim_ocrd...")
    insert_query = """
    INSERT INTO warehouse.dim_ocrd
    SELECT * FROM staging.dim_ocrd;
    """
    execute_postgres_query(insert_query)

    # Paso 5: Limpiar staging
    print("Limpiando staging.dim_ocrd...")
    execute_postgres_query("TRUNCATE TABLE staging.dim_ocrd;")

    print("ETL de OCRD completado con limpieza y actualización de warehouse.")
