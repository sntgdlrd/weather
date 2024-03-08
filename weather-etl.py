import requests
import pandas as pd
from datetime import datetime, timedelta

from configuracion import DATABASE, USER, PASSWORD, HOST, PORT
import psycopg2

def data_search(df, list, date):
    today = datetime.today()

    for item in list:
        url = f'http://api.weatherapi.com/v1/history.json?key=d8cb833dde7a48daa8b131011242802&q={item["name"]}&q={item["region"]}&q={item["country"]}&dt={date}'
        try:
            response = requests.get(url)
            # Esto generará una excepción si hay un error HTTP
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"No se pudo obtener la respuesta para {url}: {e}")
            continue

        data = response.json()

        #Se toman los datos que necesitamos de data
        new_data = {
        'name': data['location']['name'],
        'region': data['location']['region'],
        'country': data['location']['country'],
        'date': data['forecast']['forecastday'][0]['date'],
        'maxtemp_c': data['forecast']['forecastday'][0]['day']['maxtemp_c'],
        'mintemp_c': data['forecast']['forecastday'][0]['day']['mintemp_c'],
        'avgtemp_c': data['forecast']['forecastday'][0]['day']['avgtemp_c'],
        'maxwind_kph': data['forecast']['forecastday'][0]['day']['maxwind_kph'],
        'totalprecip_mm': data['forecast']['forecastday'][0]['day']['totalprecip_mm'],
        'totalsnow_cm': data['forecast']['forecastday'][0]['day']['totalsnow_cm'],
        'avgvis_km': data['forecast']['forecastday'][0]['day']['avgvis_km'],
        'avghumidity': data['forecast']['forecastday'][0]['day']['avghumidity'],
        'daily_will_it_rain': data['forecast']['forecastday'][0]['day']['daily_will_it_rain'],
        'daily_chance_of_rain': data['forecast']['forecastday'][0]['day']['daily_chance_of_rain'],
        'daily_will_it_snow': data['forecast']['forecastday'][0]['day']['daily_will_it_snow'],
        'daily_chance_of_snow': data['forecast']['forecastday'][0]['day']['daily_chance_of_snow'],
        'condition': data['forecast']['forecastday'][0]['day']['condition']['text'],
        'uv': data['forecast']['forecastday'][0]['day']['uv'],
        'sunrise': data['forecast']['forecastday'][0]['astro']['sunrise'],
        'sunset': data['forecast']['forecastday'][0]['astro']['sunset'],
        'moonrise': data['forecast']['forecastday'][0]['astro']['moonrise'],
        'had_moonrise': 'No' in data['forecast']['forecastday'][0]['astro']['moonrise'],
        'moonset': data['forecast']['forecastday'][0]['astro']['moonset'],
        'had_moonset': 'No' in data['forecast']['forecastday'][0]['astro']['moonset'],
        'moon_phase': data['forecast']['forecastday'][0]['astro']['moon_phase'],
        'moon_illumination': data['forecast']['forecastday'][0]['astro']['moon_illumination'],
        'entry_date': today}

        #Se agregan al dataframe
        df = df.append(pd.DataFrame(new_data), ignore_index=True)
    return(df)

def data_add(df, regions):
    
    # Obtener la fecha de hoy
    today = datetime.today()
    amount_days = 365

    # Iterar desde hoy hasta 365 días atrás
    for i in range(amount_days):
        # Calcular la fecha restando i días a la fecha actual
        date = today - timedelta(days=i)
        # Formatear la fecha en el formato YYYY-mm-dd
        formatted_date = date.strftime('%Y-%m-%d')

        #print(formatted_date)
        df = data_search(df, regions, formatted_date)
    return(df)

def data_load_redshift(df, table_name, column_names):
    # Conexión a Redshift
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    cur = conn.cursor()
    print("Datos conexion", cur)

    # Verificar si la tabla existe, si no, crearla
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (id INT PRIMARY KEY, {column_names[0]} VARCHAR(45), {column_names[1]} VARCHAR(30), {column_names[2]} VARCHAR(30), {column_names[3]} DATE, {column_names[4]} FLOAT, {column_names[5]} FLOAT, {column_names[6]} FLOAT, {column_names[7]} FLOAT, {column_names[8]} FLOAT, {column_names[9]} FLOAT, {column_names[10]} FLOAT, {column_names[11]} FLOAT, {column_names[12]} BOOLEAN, {column_names[13]} INT, {column_names[14]} BOOLEAN, {column_names[15]} INT, {column_names[16]} VARCHAR(40), {column_names[17]} INT, {column_names[18]} VARCHAR(12), {column_names[19]} VARCHAR(12), {column_names[20]} VARCHAR(12), {column_names[21]} BOOLEAN, {column_names[22]} VARCHAR(12), {column_names[23]} BOOLEAN, {column_names[24]} VARCHAR(20), {column_names[25]} INT, {column_names[26]} TIMESTAMP);
    '''

    cur.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla
    for index, row in df.iterrows():
        cur.execute(
            f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (index, row[column_names[0]], row[column_names[1]], row[column_names[2]],row[column_names[3]], row[column_names[4]], row[column_names[5]],row[column_names[6]], row[column_names[7]], row[column_names[8]],row[column_names[9]], row[column_names[10]], row[column_names[11]],row[column_names[12]], row[column_names[13]], row[column_names[14]],row[column_names[15]], row[column_names[16]], row[column_names[17]],row[column_names[18]], row[column_names[19]], row[column_names[20]],row[column_names[21]], row[column_names[22]], row[column_names[23]], row[column_names[24]], row[column_names[25]], row[column_names[26]])
        )
    
    # Commit de los cambios
    conn.commit()
    
    # Cerrar conexión
    cur.close()
    conn.close()

    print(f"Datos cargados exitosamente en la tabla {table_name}")

column_names = ['name', 'region', 'country', 'date', 'maxtemp_c', 'mintemp_c', 'avgtemp_c', 'maxwind_kph', 'totalprecip_mm', 'totalsnow_cm', 'avgvis_km', 'avghumidity', 'daily_will_it_rain', 'daily_chance_of_rain', 'daily_will_it_snow', 'daily_chance_of_snow', 'condition', 'uv', 'sunrise', 'sunset', 'moonrise', 'had_moonrise', 'moonset', 'had_moonset', 'moon_phase', 'moon_illumination', 'entry_date']

# Datos que quieres agregar
prov = [{'id': 1 , 'name':'La Plata', 'region': 'Buenos Aires', 'country': 'Argentina'},
            {'id': 2 , 'name':'Districto Federal', 'region': 'Buenos Aires', 'country': 'Argentina'},
            {'id': 3 , 'name':'San Fernando del Valle de Catamarca', 'region': 'Catamarca', 'country': 'Argentina'},
            {'id': 4 , 'name':'Resistencia', 'region': 'Chaco', 'country': 'Argentina'},
            {'id': 5 , 'name':'Rawson','region': 'Chubut','country': 'Argentina'},
            {'id': 6 , 'name':'Córdoba','region': 'Córdoba','country': 'Argentina'},
            {'id': 7 , 'name':'Corrientes','region': 'Corrientes','country': 'Argentina'},
            {'id': 8 , 'name':'Paraná','region': 'Entre Ríos','country': 'Argentina'},
            {'id': 9 , 'name':'Formosa','region': 'Formosa','country': 'Argentina'},
            {'id': 10 , 'name':'San Salvador de Jujuy','region': 'Jujuy','country': 'Argentina'},
            {'id': 11 , 'name':'Santa Rosa','region': 'La Pampa','country': 'Argentina'},
            {'id': 12 , 'name':'La Rioja','region': 'La Rioja','country': 'Argentina'},
            {'id': 13 , 'name':'Mendoza','region': 'Mendoza','country': 'Argentina'},
            {'id': 14 , 'name':'Posadas','region': 'Misiones','country': 'Argentina'},
            {'id': 15 , 'name':'Neuquén','region': 'Neuquén','country': 'Argentina'},
            {'id': 16 , 'name':'Viedma','region': 'Río Negro','country': 'Argentina'},
            {'id': 17 , 'name':'Salta','region': 'Salta','country': 'Argentina'},
            {'id': 18 , 'name':'San Juan','region': 'San Juan','country': 'Argentina'},
            {'id': 19 , 'name':'San Luis','region': 'San Luis','country': 'Argentina'},
            {'id': 20 , 'name':'Río Gallegos','region': 'Santa Cruz','country': 'Argentina'},
            {'id': 21 , 'name':'Santa Fe','region': 'Santa Fe','country': 'Argentina'},
            {'id': 22 , 'name':'Santiago del Estero','region': 'Santiago del Estero','country': 'Argentina'},
            {'id': 23 , 'name':'Ushuaia','region': 'Tierra del Fuego','country': 'Argentina'},
            {'id': 24 , 'name':'San Miguel de Tucumán','region': 'Tucumán','country': 'Argentina'}]

city = [{'id': 25 , 'name':'Washington', 'region': '', 'country': 'United States of America'},
            {'id': 26 , 'name':'Beijing', 'region': '', 'country': 'China'},
            {'id': 27 , 'name':'Tokyo', 'region': '', 'country': 'Japan'},
            {'id': 28 , 'name':'Berlin', 'region': '', 'country': 'Germany'},
            {'id': 29 , 'name':'New Delhi', 'region': '', 'country': 'India'},
            {'id': 30 , 'name':'London', 'region': '', 'country': 'United Kingdom'},
            {'id': 31 , 'name':'Paris', 'region': '', 'country': 'France'},
            {'id': 32 , 'name':'Rome', 'region': '', 'country': 'Italy'},
            {'id': 33 , 'name':'Brasilia', 'region': 'Distrito Federal', 'country': 'Brasil'},
            {'id': 34 , 'name':'Ottawa', 'region': '', 'country': 'Canada'},
            {'id': 35 , 'name':'Seoul', 'region': '', 'country': 'South Korea'},
            {'id': 36 , 'name':'Moscow', 'region': '', 'country': 'Russia'},
            {'id': 37 , 'name':'Canberra', 'region': '', 'country': 'Australia'},
            {'id': 38 , 'name':'Madrid', 'region': '', 'country': 'Spain'},
            {'id': 39 , 'name':'Mexico City', 'region': '', 'country': 'Mexico'}]

df = pd.DataFrame(columns=column_names)

# Agregar datos a la hoja
df = data_add(df, prov)
df = data_add(df, city)

#Finalizacion del proceso
print("Datos agregados exitosamente al Dataframe.")

table_name = "weatherapi"
data_load_redshift(df, table_name, column_names)

print(df)