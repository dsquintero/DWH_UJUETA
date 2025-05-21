
# DWH_UJUETA

Este proyecto implementa un conjunto de procesos ETL modulares para la extracción de datos desde SQL Server, su transformación y carga en PostgreSQL. Se ha estructurado de forma limpia usando buenas prácticas de separación por capas.

## 🏗️ Estructura del Proyecto

```
dwh_etl/
├── config/            # Configuración de conexiones y cron
├── repository/        # Acceso a fuentes de datos
├── services/          # Transformaciones y lógica de negocio
├── scripts/           # Scripts ejecutables para cada ETL
├── .env               # Variables de entorno por defecto
├── requirements.txt   # Dependencias
```

## ⚙️ Requisitos

- Python 3.8+
- PostgreSQL
- SQL Server (con ODBC Driver instalado)

Instalar dependencias:

```bash
pip install -r requirements.txt
```

## 🌍 Variables de entorno

Utiliza múltiples archivos `.env` para manejar entornos (dev, prod, test). Ejemplo de `.env`:

```env
# SQL Server
MSSQL_DRIVER=ODBC Driver 17 for SQL Server
MSSQL_USER=your_mssql_username
MSSQL_PASSWORD=your_mssql_password
MSSQL_HOST=your_mssql_host
MSSQL_PORT=1433
MSSQL_DATABASE=your_mssql_database

# PostgreSQL
POSTGRES_USER=your_pg_user
POSTGRES_PASSWORD=your_pg_password
POSTGRES_HOST=your_pg_host
POSTGRES_PORT=5432
POSTGRES_DB=your_pg_database
```

Puedes usar múltiples entornos:

```bash
python scripts/run_ocrd.py --env .env.dev
```

## 🧪 ETLs Disponibles

- OCRD (Clientes)
- OCRG (Grupos de Clientes)
- OACT (Cuentas contables)
- OOCR (Centros de costos)
- OJDT (Asientos Contables)

Cada uno tiene su script ejecutable en la carpeta `scripts/`.

## 🛠️ Estandarización

- Todos los nombres de columnas se transforman a minúsculas antes de insertarse en PostgreSQL.
- Los procesos están desacoplados para facilitar su mantenimiento y ejecución independiente.
