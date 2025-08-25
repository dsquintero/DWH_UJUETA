from repository.sqlserver_repository import get_data_with_query
from repository.postgres_repository import execute_postgres_query, insert_into_postgres

def run_all_sales_etl_borrador():
    # from datetime import datetime, timedelta

    # Bases de datos a recorrer
    bases_datos = [
        'HU_GOLIVE', 
        'HU_MEXICO', 
        'HU_COSTARICA',
        'UJUETA_TRADING',
        'HU_PANAMA',
        'HU_ECUADOR'
        ]

    # Fechas automáticas: hoy y 7 días antes
    # fecha_fin = datetime.today()
    # fecha_inicio = fecha_fin - timedelta(days=7)

    # Convertir fechas al formato requerido: 'YYYYMMDD'
    # fecha_inicio_str = fecha_inicio.strftime('%Y%m%d')
    # fecha_fin_str = fecha_fin.strftime('%Y%m%d')

    fecha_inicio_str = "20200101"
    fecha_fin_str = "20201231"

    for db in bases_datos:
        print(f"\nEjecutando ETL para base de datos: {db}")
        run_sales_etl(db, fecha_inicio_str, fecha_fin_str)

def run_all_sales_etl():
    from datetime import datetime
    """Ejecuta el ETL de ventas para múltiples bases de datos y múltiples años."""
    bases_datos = [
        'HU_GOLIVE', 
        'HU_MEXICO', 
        'HU_COSTARICA',
        'UJUETA_TRADING',
        'HU_PANAMA',
        'HU_ECUADOR'
    ]

    anio_actual = datetime.today().year
    anio_inicio = 2025

    for db in bases_datos:
        for anio in range(anio_inicio, anio_actual + 1):
            fecha_inicio_str = f"{anio}0101"
            fecha_fin_str = f"{anio}1231"
            print(f"\n=== Ejecutando ETL para base: {db}, año: {anio} ===")
            run_sales_etl(db, fecha_inicio_str, fecha_fin_str)

def run_sales_etl(database: str, fecha_inicio: str, fecha_fin: str):
    """Ejecuta el ETL para VENTAS con una consulta específica."""
    # query = """ EXEC [UJUETA-APPS].[dbo].[ETL_CARGARINFORMEVENTAS_MULTIEMP] 'HU_MEXICO','20200101', '20201231' """
    query = f"""EXEC [UJUETA-APPS].[dbo].[ETL_CARGARINFORMEVENTAS_MULTIEMP] '{database}', '{fecha_inicio}', '{fecha_fin}'"""
    print("Extrayendo datos de VENTAS...")
    df = get_data_with_query(query)

    if df.empty:
        print(f"[{database}] - Sin datos entre {fecha_inicio} y {fecha_fin}. Proceso omitido.")
        return

    # Seleccionar solo las columnas necesarias
    columnas_necesarias = [
    'Db', 'NUM_INTERNO', 'TIPO', 'DocNum', 'DocDate', 'CANCELED', 'TransId',
    'CodigoCliente', 'CodigoArticulo', 'Quantity', 'Currency',
    'TotalMonedaLocal', 'TotalDolar', 'CODIGOVENDEDOR',
    'CodigoBodega', 'dbpais', 'CostoRecalculado', 'CostoRecalculadoDolar'
    ]

    df_filtrado = df[columnas_necesarias]

    nuevos_nombres = [
    'db', 'docentry', 'doctype', 'docnum', 'docdate', 'canceled', 'transid',
    'cardcode', 'itemcode', 'quantity', 'currency',
    'linetotallocal', 'linetotadolar', 'slpcode',
    'whscode', 'pais', 'costrecallocal', 'costrecaldolar'
    ]

    # Asignar nuevos nombres
    df_filtrado.columns = nuevos_nombres

    # Resultado final
    # print(df_filtrado.head())
    
    # Paso 1: Truncar staging
    print("Truncando staging.fact_sales...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_sales;")
    
    # Paso 2: Insertar en staging
    print("Insertando en staging.fact_sales...")
    # fast_copy_insert(df_filtrado, "fact_sales")
    insert_into_postgres(df_filtrado, "fact_sales")

    # Paso 3: Update en staging
    print("Actualizando staging.fact_sales...")
    update_query = """
    UPDATE staging.fact_sales
    SET margenlocal = 
        CASE 
            WHEN linetotallocal IS NOT NULL AND linetotallocal != 0 
            THEN (1-(costrecallocal / linetotallocal)) * 100
            ELSE 0
        END
    """
    execute_postgres_query(update_query)


    # Paso 4: Eliminar de warehouse
    print("Eliminando datos existentes en warehouse.fact_sales...")
    delete_query = """
    DELETE FROM warehouse.fact_sales AS whs
    USING staging.fact_sales AS stg
    WHERE stg.db = whs.db
    AND stg.doctype = whs.doctype
    AND stg.docentry = whs.docentry
    AND stg.docnum = whs.docnum
    AND stg.itemcode = whs.itemcode;
    """
    execute_postgres_query(delete_query)

    # Paso 5: Insertar en warehouse desde staging
    print("Insertando nuevos datos en warehouse.fact_sales...")
    insert_query = """
    INSERT INTO warehouse.fact_sales
    SELECT * FROM staging.fact_sales;
    """
    execute_postgres_query(insert_query)

    # Paso 6: Limpiar staging
    print("Limpiando staging.fact_sales...")
    execute_postgres_query("TRUNCATE TABLE staging.fact_sales;")

    print("ETL de VENTAS completado con limpieza y actualización de warehouse.")
