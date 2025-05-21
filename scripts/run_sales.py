import sys
import os
from dotenv import load_dotenv

# Agregar la ruta raíz del proyecto a sys.path para evitar errores de importación
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.etl_sales import run_all_sales_etl

if __name__ == "__main__":

    env_file = ".env"
    if "--env" in sys.argv:
        env_file = sys.argv[sys.argv.index("--env") + 1]
    
    load_dotenv(dotenv_path=env_file) # Aquí cargas el .env según argumento
    run_all_sales_etl()
