import os
import psycopg2
import pandas as pd
import configuracion  # Importa el archivo de configuración

def cargar_datos_redshift(ruta_completa, nombre_tabla):
    # Conexión a Redshift
    conn = psycopg2.connect(
        dbname=configuracion.DATABASE,
        user=configuracion.USER,
        password=configuracion.PASSWORD,
        host=configuracion.HOST,
        port=configuracion.PORT
    )
    cur = conn.cursor()
    
    # Leer el archivo Excel y obtener los nombres de las columnas
    df = pd.read_excel(ruta_completa, sheet_name="forecastday_arg")
    columnas = df.columns.tolist()
    
    # Verificar si la tabla existe, si no, crearla
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {nombre_tabla} (id INT PRIMARY KEY, {columnas[0]} INT, {columnas[1]} DATE, {columnas[2]} FLOAT, {columnas[3]} FLOAT, {columnas[4]} FLOAT, {columnas[5]} FLOAT, {columnas[6]} FLOAT, {columnas[7]} FLOAT, {columnas[8]} FLOAT, {columnas[9]} BOOLEAN, {columnas[10]} INT, {columnas[11]} BOOLEAN, {columnas[12]} INT, {columnas[13]} VARCHAR(40), {columnas[14]} INT, {columnas[15]} VARCHAR(12), {columnas[16]} VARCHAR(12), {columnas[17]} VARCHAR(12), {columnas[18]} BOOLEAN, {columnas[19]} VARCHAR(12), {columnas[20]} BOOLEAN, {columnas[21]} VARCHAR(20), {columnas[22]} INT, {columnas[23]} TIMESTAMP);
    '''

    cur.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla
    for index, row in df.iterrows():
        cur.execute(
            f"INSERT INTO {nombre_tabla} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (index, row[columnas[0]], row[columnas[1]], row[columnas[2]],row[columnas[3]], row[columnas[4]], row[columnas[5]],row[columnas[6]], row[columnas[7]], row[columnas[8]],row[columnas[9]], row[columnas[10]], row[columnas[11]],row[columnas[12]], row[columnas[13]], row[columnas[14]],row[columnas[15]], row[columnas[16]], row[columnas[17]],row[columnas[18]], row[columnas[19]], row[columnas[20]],row[columnas[21]], row[columnas[22]], row[columnas[23]])
        )
    
    # Commit de los cambios
    conn.commit()
    
    # Cerrar conexión
    cur.close()
    conn.close()


# Nombre del archivo Excel
nombre_archivo = "weather.xlsx"

# Obtiene el directorio del script en ejecución
directorio_script = os.path.dirname(os.path.abspath(__file__))
ruta_completa = os.path.join(directorio_script, nombre_archivo)

nombre_tabla = 'weather'

# Función para cargar los datos
cargar_datos_redshift(ruta_completa, nombre_tabla)
