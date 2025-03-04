# Última versión

import pandas as pd
import boto3
import io
import logging
import traceback

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Nombre del bucket y carpetas en S3
BUCKET_NAME = 'pf-datos-sin-procesar'
OUTPUT_PATH = 'output/'

# Conexión con S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio de la función Lambda para procesamiento de datos.")
    
    # Imprimir el evento completo en los logs de CloudWatch
    logger.info("Evento recibido: %s", json.dumps(event, indent=2))
    
    try:
        # Extraer el nombre del archivo desde el evento de S3
        record = event['Records'][0]
        file_key = record['s3']['object']['key']

        # Detectar si el archivo pertenece a la carpeta 'sitios/' o 'reviews/'
        if file_key.startswith("sitios/"):
            dataset_type = "sitios"
        elif file_key.startswith("reviews/"):
            dataset_type = "reviews"
        else:
            logger.warning("Archivo %s ignorado (no está en 'sitios/' ni 'reviews/')", file_key)
            return {"statusCode": 200, "body": f"Archivo {file_key} ignorado."}

        logger.info("Procesando archivo de %s: s3://%s/%s", dataset_type, BUCKET_NAME, file_key)
        logger.info("Archivo recibido desde S3: %s", file_key)

        # Descargar archivo desde S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(file_content), engine='fastparquet')

        logger.info("Archivo %s leído correctamente. Registros: %d", dataset_type, len(df))

        # Aplicar transformaciones según el tipo de dataset
        if dataset_type == "sitios":
            df = procesar_sitios(df)
            output_file_key = file_key.replace("sitios/", OUTPUT_PATH)
        elif dataset_type == "reviews":
            df = procesar_reviews(df)
            output_file_key = file_key.replace("reviews/", OUTPUT_PATH)

        # Guardar archivo procesado en S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='fastparquet', index=False)
        buffer.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=output_file_key, Body=buffer.getvalue())

        logger.info("Archivo procesado y guardado en: s3://%s/%s", BUCKET_NAME, output_file_key)

        return {"statusCode": 200, "body": f"Archivo {file_key} procesado y guardado como {output_file_key}"}

    except Exception as e:
        logger.error("Error en la función Lambda: %s", str(e))
        logger.error(traceback.format_exc())
        return {"statusCode": 500, "body": f"Error en la transformación: {str(e)}"}

# -------------------- FUNCIONES DE PROCESAMIENTO --------------------

def procesar_sitios(df):
    """Aplica las transformaciones correspondientes al dataset de sitios."""
    logger.info("Iniciando transformación de datos para sitios.")

    # Eliminación de columnas innecesarias
    df.drop(columns=['description', 'state', 'relative_results', 'url'], inplace=True, errors='ignore')

    # Filtrar pizzerías
    total_sitios = len(df)
    df = df[df['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]
    logger.info("Filtrado de pizzerías completado: %d de %d registros son pizzerías.", len(df), total_sitios)

    # Extraer estado y limpiar direcciones
    df['state'] = df['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
    df = df[df['state'].isin(['NJ', 'NY'])]
    df['cleaned_address'] = df['address'].str.replace(", United States", "", regex=False)

    # Extraer calle, ciudad y código postal
    pattern_1 = r'(?P<street_address_temp>.+),\s*(?P<city>[^,]+),\s*[A-Z]{2}\s*(?P<zip_code>\d{5})$'
    df_extracted = df['cleaned_address'].str.extract(pattern_1)
    df = df.join(df_extracted)
    df["street_address"] = df["street_address_temp"].str.split(",", n=1).str[1].str.strip()
    df.drop(['address', 'cleaned_address', 'street_address_temp'], axis=1, inplace=True)

    # Expandir horarios en columnas separadas
    df['hours_dict'] = df['hours'].apply(lambda x: dict(x) if isinstance(x, list) else {})
    hours_expanded = df['hours_dict'].apply(pd.Series)
    df = pd.concat([df, hours_expanded], axis=1)
    df.drop(['hours', 'hours_dict'], axis=1, inplace=True)

    # Reemplazar símbolos en `price`
    df['price'] = df['price'].replace({'₩': '$', '₩₩': '$$'})

    # Expandir `MISC`
    df['MISC'] = df['MISC'].apply(lambda x: {} if pd.isnull(x) else x)
    misc_expanded = df['MISC'].apply(pd.Series)
    df = pd.concat([df, misc_expanded], axis=1)
    df.drop(['MISC'], axis=1, inplace=True)

    # Convertir `Service options`, `Amenities`, `Atmosphere` y `Popular for` en variables dummies
    df_dummies_serv = df['Service options'].str.get_dummies(sep=',')
    df_dummies_am = df['Amenities'].str.get_dummies(sep=',')
    df_dummies_at = df['Atmosphere'].str.get_dummies(sep=',')
    df_dummies_pop = df['Popular for'].str.get_dummies(sep=',')
    df = pd.concat([df, df_dummies_serv, df_dummies_am, df_dummies_at, df_dummies_pop], axis=1)
    df.drop(['Service options', 'Amenities', 'Atmosphere', 'Popular for'], axis=1, inplace=True)

    logger.info("Transformación de sitios completada. Registros finales: %d", len(df))

    return df

def procesar_reviews(df):
    """Aplica las transformaciones correspondientes al dataset de reviews."""
    logger.info("Iniciando transformación de datos para reviews.")

    total_reviews = len(df)
    df.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
    df.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
    df['date'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
    df.drop(columns=['time'], inplace=True)

    logger.info("Transformación de reviews completada. Registros iniciales: %d, después de limpieza: %d", total_reviews, len(df))

    return df

# Versión: V9
# - Se agregaron todas las transformaciones faltantes para igualar la versión local.
# - Se extrajeron `street_address`, `city` y `zip_code`.
# - Se expandieron `hours` y `MISC` en múltiples columnas.
# - Se reemplazaron símbolos en `price`.
# - Se convirtieron `Service options`, `Amenities`, `Atmosphere`, `Popular for` en dummies.
# - Se optimizaron logs para mejor monitoreo.

# v9 (No sé donde quedó V8 jiji)

import pandas as pd
import boto3
import io
import logging
import traceback

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Nombre del bucket y carpetas en S3
BUCKET_NAME = 'pf-datos-sin-procesar'
OUTPUT_PATH = 'output/'

# Conexión con S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio de la función Lambda para procesamiento de datos.")

    try:
        # Extraer el nombre del archivo desde el evento de S3
        record = event['Records'][0]
        file_key = record['s3']['object']['key']

        # Detectar si el archivo pertenece a la carpeta 'sitios/' o 'reviews/'
        if file_key.startswith("sitios/"):
            dataset_type = "sitios"
        elif file_key.startswith("reviews/"):
            dataset_type = "reviews"
        else:
            logger.warning("Archivo %s ignorado (no está en 'sitios/' ni 'reviews/')", file_key)
            return {"statusCode": 200, "body": f"Archivo {file_key} ignorado."}

        logger.info("Procesando archivo de %s: s3://%s/%s", dataset_type, BUCKET_NAME, file_key)

        # Descargar archivo desde S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(file_content), engine='fastparquet')

        logger.info("Archivo %s leído correctamente. Registros: %d", dataset_type, len(df))

        # Aplicar transformaciones según el tipo de dataset
        if dataset_type == "sitios":
            df = procesar_sitios(df)
            output_file_key = file_key.replace("sitios/", OUTPUT_PATH)
        elif dataset_type == "reviews":
            df = procesar_reviews(df)
            output_file_key = file_key.replace("reviews/", OUTPUT_PATH)

        # Guardar archivo procesado en S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='fastparquet', index=False)
        buffer.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=output_file_key, Body=buffer.getvalue())

        logger.info("Archivo procesado y guardado en: s3://%s/%s", BUCKET_NAME, output_file_key)

        return {"statusCode": 200, "body": f"Archivo {file_key} procesado y guardado como {output_file_key}"}

    except Exception as e:
        logger.error("Error en la función Lambda: %s", str(e))
        logger.error(traceback.format_exc())
        return {"statusCode": 500, "body": f"Error en la transformación: {str(e)}"}

# -------------------- FUNCIONES DE PROCESAMIENTO --------------------

def procesar_sitios(df):
    """Aplica las transformaciones correspondientes al dataset de sitios."""
    logger.info("Iniciando transformación de datos para sitios.")

    # Eliminación de columnas innecesarias
    df.drop(columns=['description', 'state', 'relative_results', 'url'], inplace=True, errors='ignore')

    # Filtrar pizzerías
    total_sitios = len(df)
    df = df[df['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]
    logger.info("Filtrado de pizzerías completado: %d de %d registros son pizzerías.", len(df), total_sitios)

    # Extraer estado y limpiar direcciones
    df['state'] = df['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
    df = df[df['state'].isin(['NJ', 'NY'])]
    df['cleaned_address'] = df['address'].str.replace(", United States", "", regex=False)

    # Extraer calle, ciudad y código postal
    pattern_1 = r'(?P<street_address_temp>.+),\s*(?P<city>[^,]+),\s*[A-Z]{2}\s*(?P<zip_code>\d{5})$'
    df_extracted = df['cleaned_address'].str.extract(pattern_1)
    df = df.join(df_extracted)
    df["street_address"] = df["street_address_temp"].str.split(",", n=1).str[1].str.strip()
    df.drop(['address', 'cleaned_address', 'street_address_temp'], axis=1, inplace=True)

    # Expandir horarios en columnas separadas
    df['hours_dict'] = df['hours'].apply(lambda x: dict(x) if isinstance(x, list) else {})
    hours_expanded = df['hours_dict'].apply(pd.Series)
    df = pd.concat([df, hours_expanded], axis=1)
    df.drop(['hours', 'hours_dict'], axis=1, inplace=True)

    # Reemplazar símbolos en `price`
    df['price'] = df['price'].replace({'₩': '$', '₩₩': '$$'})

    # Expandir `MISC`
    df['MISC'] = df['MISC'].apply(lambda x: {} if pd.isnull(x) else x)
    misc_expanded = df['MISC'].apply(pd.Series)
    df = pd.concat([df, misc_expanded], axis=1)
    df.drop(['MISC'], axis=1, inplace=True)

    # Convertir `Service options`, `Amenities`, `Atmosphere` y `Popular for` en variables dummies
    df_dummies_serv = df['Service options'].str.get_dummies(sep=',')
    df_dummies_am = df['Amenities'].str.get_dummies(sep=',')
    df_dummies_at = df['Atmosphere'].str.get_dummies(sep=',')
    df_dummies_pop = df['Popular for'].str.get_dummies(sep=',')
    df = pd.concat([df, df_dummies_serv, df_dummies_am, df_dummies_at, df_dummies_pop], axis=1)
    df.drop(['Service options', 'Amenities', 'Atmosphere', 'Popular for'], axis=1, inplace=True)

    logger.info("Transformación de sitios completada. Registros finales: %d", len(df))

    return df

def procesar_reviews(df):
    """Aplica las transformaciones correspondientes al dataset de reviews."""
    logger.info("Iniciando transformación de datos para reviews.")

    total_reviews = len(df)
    df.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
    df.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
    df['date'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
    df.drop(columns=['time'], inplace=True)

    logger.info("Transformación de reviews completada. Registros iniciales: %d, después de limpieza: %d", total_reviews, len(df))

    return df

# Versión: V9
# - Se agregaron todas las transformaciones faltantes para igualar la versión local.
# - Se extrajeron `street_address`, `city` y `zip_code`.
# - Se expandieron `hours` y `MISC` en múltiples columnas.
# - Se reemplazaron símbolos en `price`.
# - Se convirtieron `Service options`, `Amenities`, `Atmosphere`, `Popular for` en dummies.
# - Se optimizaron logs para mejor monitoreo.


# V7
# Esta version está mala, no hace las transformaciones que si hace el código de Victoria

import pandas as pd
import boto3
import io
import logging
import traceback

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Nombre del bucket y carpetas en S3
BUCKET_NAME = 'pf-datos-sin-procesar'
OUTPUT_PATH = 'output/'

# Conexión con S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio de la función Lambda para procesamiento de datos.")

    try:
        # Extraer el nombre del archivo desde el evento de S3
        record = event['Records'][0]
        file_key = record['s3']['object']['key']

        # Detectar si el archivo pertenece a la carpeta 'sitios/' o 'reviews/'
        if file_key.startswith("sitios/"):
            dataset_type = "sitios"
        elif file_key.startswith("reviews/"):
            dataset_type = "reviews"
        else:
            logger.warning("Archivo %s ignorado (no está en 'sitios/' ni 'reviews/')", file_key)
            return {"statusCode": 200, "body": f"Archivo {file_key} ignorado."}

        logger.info("Procesando archivo de %s: s3://%s/%s", dataset_type, BUCKET_NAME, file_key)

        # Descargar archivo desde S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(file_content), engine='fastparquet')

        logger.info("Archivo %s leído correctamente. Registros: %d", dataset_type, len(df))

        # Aplicar transformaciones según el tipo de dataset
        if dataset_type == "sitios":
            df = procesar_sitios(df)
            output_file_key = file_key.replace("sitios/", OUTPUT_PATH)
        elif dataset_type == "reviews":
            df = procesar_reviews(df)
            output_file_key = file_key.replace("reviews/", OUTPUT_PATH)

        # Guardar archivo procesado en S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='fastparquet', index=False)
        buffer.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=output_file_key, Body=buffer.getvalue())

        logger.info("Archivo procesado y guardado en: s3://%s/%s", BUCKET_NAME, output_file_key)

        return {"statusCode": 200, "body": f"Archivo {file_key} procesado y guardado como {output_file_key}"}

    except Exception as e:
        logger.error("Error en la función Lambda: %s", str(e))
        logger.error(traceback.format_exc())
        return {"statusCode": 500, "body": f"Error en la transformación: {str(e)}"}

# -------------------- FUNCIONES DE PROCESAMIENTO --------------------

def procesar_sitios(df):
    """Aplica las transformaciones correspondientes al dataset de sitios."""
    logger.info("Iniciando transformación de datos para sitios.")

    # Eliminación de columnas innecesarias
    df.drop(columns=['description', 'state', 'relative_results', 'url'], inplace=True, errors='ignore')

    # Filtrar pizzerías
    total_sitios = len(df)
    df = df[df['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]
    logger.info("Filtrado de pizzerías completado: %d de %d registros son pizzerías.", len(df), total_sitios)

    # Extraer estado y limpiar direcciones
    df['state'] = df['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
    df = df[df['state'].isin(['NJ', 'NY'])]
    df['cleaned_address'] = df['address'].str.replace(", United States", "", regex=False)
    
    logger.info("Filtrado por estado completado. Registros finales: %d", len(df))

    return df

def procesar_reviews(df):
    """Aplica las transformaciones correspondientes al dataset de reviews."""
    logger.info("Iniciando transformación de datos para reviews.")

    total_reviews = len(df)

    # Eliminación de columnas innecesarias
    df.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')

    # Eliminar duplicados
    df.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)

    # Convertir columna time a formato datetime
    df['date'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
    df.drop(columns=['time'], inplace=True)

    logger.info("Transformación de reviews completada. Registros iniciales: %d, después de limpieza: %d", total_reviews, len(df))

    return df

# Versión: V7
# - Se modificó la función para que se active automáticamente cuando un archivo se sube a S3 en 'sitios/' o 'reviews/'.
# - Se eliminó la referencia a archivos fijos, ahora se procesa el archivo subido.
# - Se separaron las transformaciones en dos funciones (`procesar_sitios()` y `procesar_reviews()`).
# - Se mantiene la estructura de salida en `output/` con el mismo nombre del archivo original.
# - Se agregaron logs detallados en cada paso.

# V6
import pandas as pd
import boto3
import io
import logging
import traceback

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Nombre del bucket y carpetas en S3
BUCKET_NAME = 'pf-datos-sin-procesar'
INPUT_PATH = 'input/'
OUTPUT_PATH = 'output/'

# Archivos en S3
S3_FILE_SITIOS = f'{INPUT_PATH}gm_sitios_raw_10PC.parquet'
S3_FILE_REVIEWS = f'{INPUT_PATH}gm_rev_raw_10PC.parquet'

# Archivos de salida
OUTPUT_FILE_SITIOS = f'{OUTPUT_PATH}gm_sitios_NJNY_10PC.parquet'
OUTPUT_FILE_REVIEWS = f'{OUTPUT_PATH}gm_rev_NJNY_10PC.parquet'

# Conexión con S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio de la función Lambda para procesamiento de datos.")

    try:
        # Descargar archivo gm_sitios desde S3
        logger.info("Descargando archivo: %s", S3_FILE_SITIOS)
        response_sitios = s3.get_object(Bucket=BUCKET_NAME, Key=S3_FILE_SITIOS)
        gm_sitios_10PC = pd.read_parquet(io.BytesIO(response_sitios['Body'].read()), engine='fastparquet')

        # Descargar archivo gm_reviews desde S3
        logger.info("Descargando archivo: %s", S3_FILE_REVIEWS)
        response_reviews = s3.get_object(Bucket=BUCKET_NAME, Key=S3_FILE_REVIEWS)
        gm_reviews_10PC = pd.read_parquet(io.BytesIO(response_reviews['Body'].read()), engine='fastparquet')

        logger.info("Archivos descargados y leídos correctamente.")

        # Eliminación de columnas innecesarias
        gm_sitios_10PC.drop(columns=['description', 'state', 'relative_results', 'url'], inplace=True)

        # Filtrar pizzerías
        total_sitios = len(gm_sitios_10PC)
        gm_sitios_10PC_pizza = gm_sitios_10PC[gm_sitios_10PC['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]
        logger.info("Filtrado de pizzerías completado: %d de %d registros son pizzerías.", len(gm_sitios_10PC_pizza), total_sitios)

        # Extraer estado y limpiar direcciones
        gm_sitios_10PC_pizza['state'] = gm_sitios_10PC_pizza['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
        gm_sitios_10PC_pizza_NJNY = gm_sitios_10PC_pizza[gm_sitios_10PC_pizza['state'].isin(['NJ', 'NY'])].copy()
        logger.info("Filtrado por estado completado: %d pizzerías en NJ y NY.", len(gm_sitios_10PC_pizza_NJNY))

        gm_sitios_10PC_pizza_NJNY['cleaned_address'] = gm_sitios_10PC_pizza_NJNY['address'].str.replace(", United States", "", regex=False)

        # Procesar reviews
        total_reviews = len(gm_reviews_10PC)
        gm_reviews_10PC.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
        gm_reviews_10PC.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
        gm_reviews_10PC['date'] = pd.to_datetime(gm_reviews_10PC['time'], unit='ms')
        gm_reviews_10PC.drop(columns=['time'], inplace=True)
        logger.info("Procesamiento de reviews completado: %d de %d registros son únicos.", len(gm_reviews_10PC), total_reviews)

        # Realizar `merge` entre datasets
        gm_10PC = pd.merge(gm_sitios_10PC_pizza_NJNY, gm_reviews_10PC, how='inner', on='gmap_id')

        # Separar datasets procesados
        gm_sitios_NJNY_10PC = gm_10PC[['gmap_id', 'name', 'street_address', 'city', 'state', 'zip_code', 'latitude', 'longitude', 'avg_rating', 'num_of_reviews', 'price']].drop_duplicates()
        gm_rev_NJNY_10PC = gm_10PC[['gmap_id', 'user_id', 'date', 'rating', 'text']]

        # Guardar archivos en memoria
        output_buffer_sitios = io.BytesIO()
        output_buffer_reviews = io.BytesIO()

        gm_sitios_NJNY_10PC.to_parquet(output_buffer_sitios, engine='fastparquet', index=False)
        gm_rev_NJNY_10PC.to_parquet(output_buffer_reviews, engine='fastparquet', index=False)

        logger.info("Archivos transformados. Preparando carga a S3...")

        # Subir archivos procesados a S3
        output_buffer_sitios.seek(0)
        output_buffer_reviews.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_FILE_SITIOS, Body=output_buffer_sitios.getvalue())
        s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_FILE_REVIEWS, Body=output_buffer_reviews.getvalue())

        logger.info("Archivos procesados guardados en S3: %s y %s", OUTPUT_FILE_SITIOS, OUTPUT_FILE_REVIEWS)

        return {"statusCode": 200, "body": "Transformación completada y archivos almacenados en S3"}

    except Exception as e:
        logger.error("Error en la función Lambda: %s", str(e))
        logger.error(traceback.format_exc())  # Se mantiene traceback para mejor depuración
        return {"statusCode": 500, "body": f"Error en la transformación: {str(e)}"}

# Versión: V6
# Se eliminaron importaciones innecesarias (`numpy` y `warnings`).
# Se incorporó `traceback.format_exc()` para mejor depuración de errores.
# Se agregaron logs detallados en cada paso del procesamiento.
# Se registran la cantidad de registros antes y después del filtrado.


# V5 
# Nueva función ETL de Victoria

import numpy as np
import pandas as pd
import warnings
import boto3
import io
import logging

warnings.filterwarnings("ignore")

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Nombre del bucket y carpetas en S3
BUCKET_NAME = 'pf-datos-sin-procesar'
INPUT_PATH = 'input/'
OUTPUT_PATH = 'output/'

# Archivos en S3
S3_FILE_SITIOS = f'{INPUT_PATH}gm_sitios_raw_10PC.parquet'
S3_FILE_REVIEWS = f'{INPUT_PATH}gm_rev_raw_10PC.parquet'

# Archivos de salida
OUTPUT_FILE_SITIOS = f'{OUTPUT_PATH}gm_sitios_NJNY_10PC.parquet'
OUTPUT_FILE_REVIEWS = f'{OUTPUT_PATH}gm_rev_NJNY_10PC.parquet'

# Conexión con S3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio de la función Lambda para procesamiento de datos.")

    try:
        # Descargar archivo gm_sitios desde S3
        logger.info("Descargando archivo: %s", S3_FILE_SITIOS)
        response_sitios = s3.get_object(Bucket=BUCKET_NAME, Key=S3_FILE_SITIOS)
        gm_sitios_10PC = pd.read_parquet(io.BytesIO(response_sitios['Body'].read()), engine='fastparquet')

        # Descargar archivo gm_reviews desde S3
        logger.info("Descargando archivo: %s", S3_FILE_REVIEWS)
        response_reviews = s3.get_object(Bucket=BUCKET_NAME, Key=S3_FILE_REVIEWS)
        gm_reviews_10PC = pd.read_parquet(io.BytesIO(response_reviews['Body'].read()), engine='fastparquet')

        logger.info("Archivos descargados y leídos correctamente.")

        # Eliminación de columnas innecesarias
        gm_sitios_10PC.drop(columns=['description', 'state', 'relative_results', 'url'], inplace=True)

        # Filtrar pizzerías
        gm_sitios_10PC_pizza = gm_sitios_10PC[gm_sitios_10PC['category'].apply(lambda x: isinstance(x, list) and 'Pizza restaurant' in x)]

        # Extraer estado y limpiar direcciones
        gm_sitios_10PC_pizza['state'] = gm_sitios_10PC_pizza['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
        gm_sitios_10PC_pizza_NJNY = gm_sitios_10PC_pizza[gm_sitios_10PC_pizza['state'].isin(['NJ', 'NY'])].copy()
        gm_sitios_10PC_pizza_NJNY['cleaned_address'] = gm_sitios_10PC_pizza_NJNY['address'].str.replace(", United States", "", regex=False)

        # Extraer información de la dirección
        pattern_1 = r'(?P<street_address_temp>.+),\s*(?P<city>[^,]+),\s*[A-Z]{2}\s*(?P<zip_code>\d{5})$'
        df_extracted10 = gm_sitios_10PC_pizza_NJNY['cleaned_address'].str.extract(pattern_1)
        gm_sitios_10PC_pizza_NJNY = gm_sitios_10PC_pizza_NJNY.join(df_extracted10)

        # Ajustes finales en las direcciones
        gm_sitios_10PC_pizza_NJNY["street_address"] = gm_sitios_10PC_pizza_NJNY["street_address_temp"].str.split(",", n=1).str[1].str.strip()
        gm_sitios_10PC_pizza_NJNY.drop(columns=['address', 'cleaned_address', 'street_address_temp'], inplace=True)

        # Eliminar duplicados
        gm_sitios_10PC_pizza_NJNY.drop_duplicates(subset=['gmap_id'], keep='first', inplace=True)

        # Procesar la columna `hours`
        gm_sitios_10PC_pizza_NJNY['hours_dict'] = gm_sitios_10PC_pizza_NJNY['hours'].apply(lambda x: dict(x) if isinstance(x, list) else {})
        hours_expanded10 = gm_sitios_10PC_pizza_NJNY['hours_dict'].apply(pd.Series)
        gm_sitios_10PC_pizza_NJNY = pd.concat([gm_sitios_10PC_pizza_NJNY, hours_expanded10], axis=1)

        # Procesamiento de reviews
        gm_reviews_10PC.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
        gm_reviews_10PC.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
        gm_reviews_10PC['date'] = pd.to_datetime(gm_reviews_10PC['time'], unit='ms')
        gm_reviews_10PC.drop(columns=['time'], inplace=True)

        # Realizar `merge` entre datasets
        gm_10PC = pd.merge(gm_sitios_10PC_pizza_NJNY, gm_reviews_10PC, how='inner', on='gmap_id')

        # Separar datasets procesados
        gm_sitios_NJNY_10PC = gm_10PC[['gmap_id', 'name', 'street_address', 'city', 'state', 'zip_code', 'latitude', 'longitude', 'avg_rating', 'num_of_reviews', 'price']].drop_duplicates()
        gm_rev_NJNY_10PC = gm_10PC[['gmap_id', 'user_id', 'date', 'rating', 'text']]

        # Guardar archivos en memoria
        output_buffer_sitios = io.BytesIO()
        output_buffer_reviews = io.BytesIO()

        gm_sitios_NJNY_10PC.to_parquet(output_buffer_sitios, engine='fastparquet', index=False)
        gm_rev_NJNY_10PC.to_parquet(output_buffer_reviews, engine='fastparquet', index=False)

        logger.info("Archivos transformados. Preparando carga a S3...")

        # Subir archivos procesados a S3
        output_buffer_sitios.seek(0)
        output_buffer_reviews.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_FILE_SITIOS, Body=output_buffer_sitios.getvalue())
        s3.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_FILE_REVIEWS, Body=output_buffer_reviews.getvalue())

        logger.info("Archivos procesados guardados en S3: %s y %s", OUTPUT_FILE_SITIOS, OUTPUT_FILE_REVIEWS)

        return {"statusCode": 200, "body": "Transformación completada y archivos almacenados en S3"}

    except Exception as e:
        logger.error("Error en la función Lambda: %s", str(e))
        return {"statusCode": 500, "body": f"Error en la transformación: {str(e)}"}

# Versión: V5
# Se eliminan rutas locales y se usa S3 para leer/escribir los archivos.
# Se aseguran los archivos de entrada en `input/` y los de salida en `output/`.
# Se agregan logs para monitoreo en AWS CloudWatch.
# Este es la última versión del código ETL que Victoria envió el 25-02-2025.


# V4
# nota: Funcion Lambda configurada a 3008mb de memoria

import boto3
import io
import json
import pandas as pd
import pyarrow.parquet as pq
import logging
import traceback

from boto3.s3.transfer import S3Transfer

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info("Inicio de ejecución de la función Lambda.")

    try:
        # Conexión con AWS S3
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)

        # Especificación del archivo en S3
        bucket_name = 'pf-datos-sin-procesar'
        gm_sitios_pizza_key = 'Input/gm_sitios_pizza.parquet'
        gm_review_NJ_key = 'Input/gm_review_NJ_raw.parquet'
        gm_review_NY_key = 'Input/gm_review_NY_raw.parquet'

        # Output S3 paths
        output_key_sitios = 'output/gm_sitios_NJNY.parquet'
        output_key_rev = 'output/gm_rev_NJNY.parquet'

        # Descarga `gm_sitios_pizza.parquet` desde S3
        logger.info("Descargando archivo desde S3: %s/%s", bucket_name, gm_sitios_pizza_key)
        response_sitios_pizza = s3.get_object(Bucket=bucket_name, Key=gm_sitios_pizza_key)
        
        # Leer archivo en memoria usando chunks
        parquet_file = pq.ParquetFile(io.BytesIO(response_sitios_pizza['Body'].read()))
        df_chunks = [batch.to_pandas() for batch in parquet_file.iter_batches(batch_size=5000)]
        gm_sitios_pizza = pd.concat(df_chunks, ignore_index=True)
        
        logger.info("Archivo gm_sitios_pizza cargado en DataFrame. Shape: %s", gm_sitios_pizza.shape)

        # Extraer estado y limpiar dirección
        gm_sitios_pizza['state'] = gm_sitios_pizza['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
        gm_sitios_pizza_NJNY = gm_sitios_pizza[gm_sitios_pizza['state'].isin(['NJ', 'NY'])].copy()
        gm_sitios_pizza_NJNY['cleaned_address'] = gm_sitios_pizza_NJNY['address'].str.replace(", United States", "", regex=False)

        logger.info("Filtrado de direcciones completado. Shape final: %s", gm_sitios_pizza_NJNY.shape)

        # Descargar archivos de reviews desde S3
        logger.info("Descargando reviews desde S3: %s", gm_review_NJ_key)
        response_review_NJ = s3.get_object(Bucket=bucket_name, Key=gm_review_NJ_key)
        df_rev_NJ = pd.read_parquet(io.BytesIO(response_review_NJ['Body'].read()), engine='pyarrow')

        logger.info("Descargando reviews desde S3: %s", gm_review_NY_key)
        response_review_NY = s3.get_object(Bucket=bucket_name, Key=gm_review_NY_key)
        df_rev_NY = pd.read_parquet(io.BytesIO(response_review_NY['Body'].read()), engine='pyarrow')

        # Procesar reviews
        for df, state in [(df_rev_NJ, 'NJ'), (df_rev_NY, 'NY')]:
            df.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
            df.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
            df['date'] = pd.to_datetime(df['time'], unit='ms')
            logger.info("Procesamiento de reviews %s completado. Shape: %s", state, df.shape)

        # Guardar archivos en memoria
        output_buffer_sitios = io.BytesIO()
        output_buffer_review = io.BytesIO()

        gm_sitios_pizza_NJNY.to_parquet(output_buffer_sitios, engine='pyarrow')
        df_rev_NJ.to_parquet(output_buffer_review, engine='pyarrow')

        logger.info("Conversión a Parquet completada. Preparando carga a S3...")

        # Subir directamente desde memoria a S3
        output_buffer_sitios.seek(0)
        output_buffer_review.seek(0)

        s3.put_object(Bucket=bucket_name, Key=output_key_sitios, Body=output_buffer_sitios.getvalue())
        s3.put_object(Bucket=bucket_name, Key=output_key_rev, Body=output_buffer_review.getvalue())

        logger.info("Archivo %s procesado y guardado en S3.", output_key_sitios)
        logger.info("Archivo %s procesado y guardado en S3.", output_key_rev)

        return {'statusCode': 200, 'body': json.dumps({'message': "Success"})}

    except Exception as e:
        logger.error("Error en la ejecución de Lambda: %s", str(e))
        logger.error(traceback.format_exc())
        return {'statusCode': 500, 'body': json.dumps({'message': "Error", 'error': str(e)})}

# Versión: V4
# En esta versión del código se agregaron logs con `logging` para monitorear la ejecución de la función Lambda.
# Se registran eventos clave antes y después de descargar archivos de S3, transformar datos y subir resultados a S3.
# También se manejan errores con `traceback` para mejorar la depuración en AWS CloudWatch.


# V3
# nota: Funcion Lambda configurada a 3008mb de memoria

import boto3
import io
import json
import pandas as pd
import pyarrow.parquet as pq
from boto3.s3.transfer import S3Transfer
import traceback

def lambda_handler(event, context):
    print("Lambda function started")

    try:
        # Conexión con AWS S3
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)

        # Especificación del archivo en S3
        bucket_name = 'pf-datos-sin-procesar'
        gm_sitios_pizza_key = 'Input/gm_sitios_pizza.parquet'
        gm_review_NJ_key = 'Input/gm_review_NJ_raw.parquet'
        gm_review_NY_key = 'Input/gm_review_NY_raw.parquet'

        # Output S3 paths
        output_key_sitios = 'output/gm_sitios_NJNY.parquet'
        output_key_rev = 'output/gm_rev_NJNY.parquet'

        # Descarga `gm_sitios_pizza.parquet` desde S3
        print(f"Attempting to get file: {gm_sitios_pizza_key} from bucket: {bucket_name}")
        response_sitios_pizza = s3.get_object(Bucket=bucket_name, Key=gm_sitios_pizza_key)
        print("File retrieved successfully from S3")

        # Leer `gm_sitios_pizza.parquet` en memoria usando chunks
        parquet_file = pq.ParquetFile(io.BytesIO(response_sitios_pizza['Body'].read()))
        df_chunks = [batch.to_pandas() for batch in parquet_file.iter_batches(batch_size=5000)]
        gm_sitios_pizza = pd.concat(df_chunks, ignore_index=True)

        print(f"DataFrame gm_sitios_pizza loaded. Shape: {gm_sitios_pizza.shape}")

        # Extraer estado y limpiar dirección
        gm_sitios_pizza = gm_sitios_pizza.copy()
        gm_sitios_pizza['state'] = gm_sitios_pizza['address'].str.extract(r',\s*([A-Z]{2})\s*\d{5}')
        gm_sitios_pizza_NJNY = gm_sitios_pizza[gm_sitios_pizza['state'].isin(['NJ', 'NY'])].copy()
        gm_sitios_pizza_NJNY['cleaned_address'] = gm_sitios_pizza_NJNY['address'].str.replace(", United States", "", regex=False)

        # 📌 **Descargar archivos de reviews desde S3 sin escribir en `/tmp/`**
        print(f"Downloading {gm_review_NJ_key} from S3")
        response_review_NJ = s3.get_object(Bucket=bucket_name, Key=gm_review_NJ_key)
        df_rev_NJ = pd.read_parquet(io.BytesIO(response_review_NJ['Body'].read()), engine='pyarrow')

        print(f"Downloading {gm_review_NY_key} from S3")
        response_review_NY = s3.get_object(Bucket=bucket_name, Key=gm_review_NY_key)
        df_rev_NY = pd.read_parquet(io.BytesIO(response_review_NY['Body'].read()), engine='pyarrow')

        # Procesar reviews
        df_rev_NJ.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
        df_rev_NJ.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
        df_rev_NJ['date'] = pd.to_datetime(df_rev_NJ['time'], unit='ms')

        df_rev_NY.drop(columns=['name', 'pics', 'resp'], inplace=True, errors='ignore')
        df_rev_NY.drop_duplicates(subset=['user_id', 'gmap_id', 'time'], keep='first', inplace=True)
        df_rev_NY['date'] = pd.to_datetime(df_rev_NY['time'], unit='ms')

        # 📌 **Guardar archivos en memoria (`io.BytesIO()`) en lugar de `/tmp/`**
        output_buffer_sitios = io.BytesIO()
        output_buffer_review = io.BytesIO()

        gm_sitios_pizza_NJNY.to_parquet(output_buffer_sitios, engine='pyarrow')
        df_rev_NJ.to_parquet(output_buffer_review, engine='pyarrow')

        # 📌 **Subir directamente desde memoria a S3**
        output_buffer_sitios.seek(0)
        output_buffer_review.seek(0)

        s3.put_object(Bucket=bucket_name, Key=output_key_sitios, Body=output_buffer_sitios.getvalue())
        s3.put_object(Bucket=bucket_name, Key=output_key_rev, Body=output_buffer_review.getvalue())

        print(f"Archivo {output_key_sitios} procesado y guardado en S3")
        print(f"Archivo {output_key_rev} procesado y guardado en S3")

        return {'statusCode': 200, 'body': json.dumps({'message': "Success"})}

    except Exception as e:
        print(traceback.format_exc())
        return {'statusCode': 500, 'body': json.dumps({'message': "Error", 'error': str(e)})}
