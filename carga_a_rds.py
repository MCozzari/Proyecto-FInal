# última versión

import json
import pymysql
import boto3
import os
import pandas as pd
import io
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Configuración de la BD y S3 desde variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PROCESADO_PREFIX = os.getenv("S3_PROCESADO_PREFIX")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    logger.info("Inicio de ejecución de la función Lambda.")

    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        logger.info("Procesando archivo recibido en S3: %s", s3_key)

        if not s3_key.startswith(S3_PROCESADO_PREFIX):
            logger.warning("El archivo %s no está en la carpeta %s. Se ignora.", s3_key, S3_PROCESADO_PREFIX)
            continue

        try:
            # Descargar archivo desde S3
            logger.info("Descargando archivo desde S3: %s/%s", S3_BUCKET, s3_key)
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            file_content = response['Body'].read()

            # Determinar tipo de archivo
            if s3_key.endswith(".json"):
                data = json.loads(file_content.decode("utf-8"))
                logger.info("Archivo JSON leído correctamente. Registros: %d", len(data))
            elif s3_key.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(file_content))
                data = df.to_dict(orient="records")
                logger.info("Archivo Parquet leído correctamente. Registros: %d", len(data))
            else:
                logger.warning("Formato de archivo no soportado: %s", s3_key)
                continue

            # Cargar datos en MySQL
            logger.info("Iniciando inserción en la base de datos.")
            insertar_en_mysql(data)

        except json.JSONDecodeError as e:
            logger.error("Error al decodificar JSON en %s: %s", s3_key, str(e))
        except pd.errors.EmptyDataError as e:
            logger.error("Error al leer Parquet en %s: %s", s3_key, str(e))
        except Exception as e:
            logger.error("Error inesperado al procesar el archivo %s: %s", s3_key, str(e))

    logger.info("Ejecución de Lambda finalizada.")
    return {"statusCode": 200, "body": json.dumps("Carga completa")}


def insertar_en_mysql(data):
    logger.info("Conectando a la base de datos MySQL.")

    try:
        conexion = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conexion.cursor()

        # Insertar datos
        for i, item in enumerate(data):
            try:
                sql = "INSERT INTO datos_procesados (nombre, edad, ciudad) VALUES (%s, %s, %s)"
                cursor.execute(sql, (item["nombre"], item["edad"], item["ciudad"]))

                if i % 100 == 0:  # Log cada 100 registros insertados
                    logger.info("Insertados %d registros en MySQL...", i + 1)

            except KeyError as e:
                logger.warning("Registro inválido, falta clave %s: %s", str(e), item)
            except Exception as e:
                logger.error("Error al insertar en MySQL: %s", str(e))

        conexion.commit()
        logger.info("Datos insertados correctamente en MySQL.")

    except pymysql.MySQLError as e:
        logger.error("Error en la conexión o inserción en MySQL: %s", str(e))
    except Exception as e:
        logger.error("Error inesperado en la inserción MySQL: %s", str(e))
    finally:
        cursor.close()
        conexion.close()
        logger.info("Conexión MySQL cerrada.")

# Versión: V2
# En esta versión del código se incorporan logs con `logging` para monitorear cada paso de la ejecución,
# incluyendo descargas desde S3, detección de formatos, lectura de datos y carga en MySQL.
# También se agregaron logs de advertencia y error para manejar archivos inválidos o fallas en MySQL.

# V2 chatgpt

import json
import pymysql
import boto3
import os
import pandas as pd
import io

# Configuración de la BD desde variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PROCESADO_PREFIX = os.getenv("S3_PROCESADO_PREFIX")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        if not s3_key.startswith(S3_PROCESADO_PREFIX):
            print(f"El archivo {s3_key} no está en la carpeta correcta.")
            continue
        
        # Descargar archivo desde S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()

        # Detectar el tipo de archivo por su extensión
        if s3_key.endswith(".json"):
            data = json.loads(file_content.decode("utf-8"))  # Leer JSON normalmente
        elif s3_key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(file_content))  # Leer Parquet en un DataFrame
            data = df.to_dict(orient="records")  # Convertir DataFrame a lista de diccionarios
        else:
            print(f"Formato de archivo no soportado: {s3_key}")
            continue

        # Conectar a la BD
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Insertar datos
        for item in data:
            sql = "INSERT INTO datos_procesados (nombre, edad, ciudad) VALUES (%s, %s, %s)"
            cursor.execute(sql, (item["nombre"], item["edad"], item["ciudad"]))
        
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Datos de {s3_key} insertados en la BD.")

    return {"statusCode": 200, "body": json.dumps("Carga completa")}

# V1.1

import json
import pymysql
import boto3
import os
import pandas as pd
import io

# Configuración de la BD desde variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PROCESADO_PREFIX = os.getenv("S3_PROCESADO_PREFIX")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        if not s3_key.startswith(S3_PROCESADO_PREFIX):
            print(f"El archivo {s3_key} no está en la carpeta correcta.")
            continue
        
        # Descargar archivo desde S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()

        # Detectar el tipo de archivo por su extensión
        if s3_key.endswith(".json"):
            data = json.loads(file_content.decode("utf-8"))  # Leer JSON normalmente
        elif s3_key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(file_content))  # Leer Parquet en un DataFrame
            data = df.to_dict(orient="records")  # Convertir DataFrame a lista de diccionarios
        else:
            print(f"Formato de archivo no soportado: {s3_key}")
            continue

        # Conectar a la BD
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Insertar datos
        for item in data:
            sql = "INSERT INTO datos_procesados (nombre, edad, ciudad) VALUES (%s, %s, %s)"
            cursor.execute(sql, (item["nombre"], item["edad"], item["ciudad"]))
        
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Datos de {s3_key} insertados en la BD.")

    return {"statusCode": 200, "body": json.dumps("Carga completa")}


# Original
# Este solo usa archivos .JSON que sean subidos a la carpeta
import json
import pymysql
import boto3
import os

# Configuración de la BD desde variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PROCESADO_PREFIX = os.getenv("S3_PROCESADO_PREFIX")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        if not s3_key.startswith(S3_PROCESADO_PREFIX):
            print(f"El archivo {s3_key} no está en la carpeta correcta.")
            continue
        
        # Descargar archivo desde S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Convertir contenido en formato JSON (ajustar según sea necesario)
        data = json.loads(file_content)

        # Conectar a la BD
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Insertar datos
        for item in data:
            sql = "INSERT INTO datos_procesados (nombre, edad, ciudad) VALUES (%s, %s, %s)"
            cursor.execute(sql, (item["nombre"], item["edad"], item["ciudad"]))
        
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Datos de {s3_key} insertados en la BD.")

    return {"statusCode": 200, "body": json.dumps("Carga completa")}

# V2

import json
import pymysql
import boto3
import os
import pandas as pd
import io

# Configuración de la BD desde variables de entorno
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PROCESADO_PREFIX = os.getenv("S3_PROCESADO_PREFIX")

s3 = boto3.client("s3")

def lambda_handler(event, context):
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        if not s3_key.startswith(S3_PROCESADO_PREFIX):
            print(f"El archivo {s3_key} no está en la carpeta correcta.")
            continue
        
        # Descargar archivo desde S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()

        # Detectar el tipo de archivo por su extensión
        if s3_key.endswith(".json"):
            data = json.loads(file_content.decode("utf-8"))  # Leer JSON normalmente
        elif s3_key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(file_content))  # Leer Parquet en un DataFrame
            data = df.to_dict(orient="records")  # Convertir DataFrame a lista de diccionarios
        else:
            print(f"Formato de archivo no soportado: {s3_key}")
            continue

        # Conectar a la BD
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Insertar datos
        for item in data:
            sql = "INSERT INTO datos_procesados (nombre, edad, ciudad) VALUES (%s, %s, %s)"
            cursor.execute(sql, (item["nombre"], item["edad"], item["ciudad"]))
        
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Datos de {s3_key} insertados en la BD.")

    return {"statusCode": 200, "body": json.dumps("Carga completa")}
