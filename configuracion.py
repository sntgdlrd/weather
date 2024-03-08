from dotenv import load_dotenv
import os

load_dotenv()

# Datos de conexión a Redshift
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
