# Weather API

En una descarga inicial se toman los datos metereologicos de los distintos dias de un año desde la API de mismo nombre, de una lista de ciudades para asi guardarlos en una tabla que se aloja en Redshift.

## Requisitos

- Python 3.x
- pandas
- psycopg2

## Descripcion

weather-etl.py es el script encargado de realizar la extraccion de los datos, guardardos en un Dataframe para luego usar el mismo para cargarlo en una tabla de Redshift segun las credenciales que configracion.py toma de un archivo .env / las variables de entorno.

