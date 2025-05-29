from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import execute_postgres_query, insert_into_postgres
from services.transform_service import normalize_column_names

def run_all_kardex_etl():
    from datetime import datetime
    """Ejecuta el ETL de ventas para múltiples bases de datos y múltiples años."""
    bases_datos = [
        #'HU_GOLIVE', 
        #'HU_MEXICO', 
        #'HU_COSTARICA',
        #'UJUETA_TRADING',
        'HU_PANAMA',
        #'HU_ECUADOR'
    ]

    anio_actual = datetime.today().year
    anio_inicio = 2020

    for db in bases_datos:
        for anio in range(anio_inicio, anio_actual + 1):
            fecha_inicio_str = f"{anio}0101"
            fecha_fin_str = f"{anio}1231"
            print(f"\n=== Ejecutando ETL para base: {db}, año: {anio} ===")
            run_kardex_etl(db, fecha_inicio_str, fecha_fin_str)

def run_kardex_etl(database: str, fecha_inicio: str, fecha_fin: str):
    """Ejecuta el ETL para KARDEX con una consulta específica."""

    query = f""" 
    SELECT  TransNum, TransType, BASE_REF, DocLineNum, DocDate, CardCode, Ref1, Ref2, Comments, JrnlMemo, 
    ItemCode, InQty, OutQty, Price, CalcPrice, TransValue, Currency, Warehouse, '{database}' AS database_name
    FROM {database}..OINM
    WHERE DocDate BETWEEN '{fecha_inicio}' AND '{fecha_fin}' 
    """
    print(query)
    print("Extrayendo datos de KARDEX...")
    df = get_data_with_query(query)
    df = normalize_column_names(df)

    if df.empty:
        print(f"[{database}] - Sin datos entre {fecha_inicio} y {fecha_fin}. Proceso omitido.")
        return

    # Paso 1: Truncar staging
    print("Truncando staging.fact_kardex...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_kardex;")
    
    # Paso 2: Insertar en staging
    print("Insertando en staging.fact_kardex...")
    insert_into_postgres(df, "fact_kardex")

    # Paso 3: Eliminar de warehouse
    print("Eliminando datos existentes en warehouse.fact_kardex...")
    delete_query = """
    DELETE FROM warehouse.fact_kardex AS whs
    USING staging.fact_kardex AS stg
    WHERE stg.database_name = whs.database_name
    AND stg.transnum = whs.transnum
    AND stg.transtype = whs.transtype
    AND stg.docdate = whs.docdate
    AND stg.itemcode = whs.itemcode;
    """
    execute_postgres_query(delete_query)

    # Paso 4: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.fact_kardex...")
    insert_query = """
    INSERT INTO warehouse.fact_kardex
    SELECT * FROM staging.fact_kardex;
    """
    execute_postgres_query(insert_query)

    # Paso 6: Limpiar staging
    print("Limpiando staging.fact_kardex...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_kardex;")

    print("ETL de KARDEX completado con limpieza y actualización de warehouse.")
