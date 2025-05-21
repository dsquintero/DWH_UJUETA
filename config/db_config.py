import os
from sqlalchemy import create_engine
import urllib
#from dotenv import load_dotenv

#load_dotenv()
#def load_environment(env_file=".env"):
#    load_dotenv(dotenv_path=env_file)

def get_sqlserver_engine():
    encoded_password = urllib.parse.quote_plus(os.getenv('MSSQL_PASSWORD'))
    encoded_driver = urllib.parse.quote_plus(os.getenv('MSSQL_DRIVER'))
    conn_str = (
        f"mssql+pyodbc://{os.getenv('MSSQL_USER')}:"
        f"{encoded_password}@"
        f"{os.getenv('MSSQL_HOST')}:"
        f"{os.getenv('MSSQL_PORT')}/"
        f"{os.getenv('MSSQL_DATABASE')}"
        f"?driver={encoded_driver}"
    )
    return create_engine(conn_str)

def get_postgres_engine():
    encoded_password = urllib.parse.quote_plus(os.getenv('POSTGRES_PASSWORD'))
    postgres_conn_str = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
        f"{encoded_password}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(postgres_conn_str)
