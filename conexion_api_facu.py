# Última versión

import urllib.request
import boto3
import json
import pymysql
import logging
from datetime import datetime

# Configuración de logging para mensajes de depuración del código
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info("Iniciando ejecución de la función Lambda.")

    # Cliente de S3
    s3 = boto3.client('s3')
    bucket_name = 'pf-datos-sin-procesar'  # Bucket actualizado

    # URL de la API del Censo
    url = "https://api.census.gov/data/2022/ecnbasic?get=YEAR,NAME,NAICS2022,RCPTOT,EMP,ESTAB&for=state:*&NAICS2022=722513"
    
    request = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0'},
        method='GET'
    )

    try:
        logger.info("Realizando solicitud a la API: %s", url)
        
        with urllib.request.urlopen(request, timeout=10) as response:
            logger.info("Respuesta recibida con código: %d", response.status)
            
            if response.status == 200:
                datos_json = json.loads(response.read().decode())

                # Reemplazo del código NAICS2022 por "Restaurantes"
                for registro in datos_json[1:]:
                    if len(registro) > 2:
                        registro[2] = "Restaurantes"

                logger.info("Datos obtenidos y transformados correctamente. Cantidad de registros: %d", len(datos_json) - 1)

                # Guardar en S3 en output/
                fecha_actual = datetime.now().strftime('%Y-%m-%d')
                file_key = f'output/census_todos_estados_{fecha_actual}.json'
                
                logger.info("Subiendo datos a S3 en: %s/%s", bucket_name, file_key)
                
                s3.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=json.dumps(datos_json),
                    ContentType='application/json'
                )

                logger.info("Archivo guardado correctamente en S3.")

                # Eliminar la primera fila (cabecera)
                datos_sin_cabecera = datos_json[1:]

                # Llamar a la función de carga en MySQL
                logger.info("Iniciando carga de datos en MySQL.")
                cargar_a_mysql(datos_sin_cabecera)

            else:
                logger.error("Error al obtener datos: código de respuesta %d", response.status)

    except urllib.error.URLError as e:
        logger.error("Error de red al obtener datos: %s", str(e))
    except json.JSONDecodeError as e:
        logger.error("Error al decodificar JSON: %s", str(e))
    except Exception as e:
        logger.error("Error inesperado: %s", str(e))

    logger.info("Ejecución de Lambda finalizada.")
    
    return {
        'statusCode': 200,
        'body': 'Datos subidos a S3 (output/) y MySQL'
    }


def cargar_a_mysql(datos):
    logger.info("Conectando a la base de datos MySQL.")

    try:
        conexion = pymysql.connect(
            host="proyecto-final-db.cz0m0gyimazl.sa-east-1.rds.amazonaws.com",
            user="nahuelfns",
            password="Gimnasia#22",
            database="proyectofinal",
            cursorclass=pymysql.cursors.DictCursor
        )

        with conexion.cursor() as cursor:
            for i, registro in enumerate(datos):
                if len(registro) < 7:
                    logger.warning("Registro inválido en posición %d: %s", i, registro)
                    continue

                year = int(registro[0])
                name = registro[1]
                sector = registro[2]
                cod_sector = int(registro[6]) if registro[6].isdigit() else None
                rcptot = int(registro[3]) if registro[3].isdigit() else None
                emp = int(registro[4]) if registro[4].isdigit() else None
                estab = int(registro[5]) if registro[5].isdigit() else None
                state = registro[7]

                sql = """
                INSERT INTO census_old (year, name, sector, rcptot, emp, estab, cod_sector, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    rcptot = VALUES(rcptot), 
                    emp = VALUES(emp), 
                    estab = VALUES(estab)
                """
                cursor.execute(sql, (year, name, sector, rcptot, emp, estab, cod_sector, state))

                if i % 100 == 0:  # Log cada 100 registros
                    logger.info("Insertados %d registros en MySQL...", i + 1)

        conexion.commit()
        logger.info("Datos insertados o actualizados correctamente en MySQL.")

    except pymysql.MySQLError as e:
        logger.error("Error en MySQL: %s", str(e))
    except Exception as e:
        logger.error("Error inesperado en MySQL: %s", str(e))
    finally:
        conexion.close()
        logger.info("Conexión MySQL cerrada.")

# Versión: V2
# En esta versión del código se incorpora logs para depurar el funcionamiento de la funcion 
# y se modifican carpetas para calzar con la estructura del cloud de Felipe

# V2 incorpora logs para depurar el funcionamiento de la funcion y se modifican carpetas para calzar con la estructura del cloud de Felipe

import urllib.request
import boto3
import json
import pymysql
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info("Iniciando ejecución de la función Lambda.")

    # Cliente de S3
    s3 = boto3.client('s3')
    bucket_name = 'pf-datos-sin-procesar'  # Bucket actualizado

    # URL de la API del Censo
    url = "https://api.census.gov/data/2022/ecnbasic?get=YEAR,NAME,NAICS2022,RCPTOT,EMP,ESTAB&for=state:*&NAICS2022=722513"
    
    request = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0'},
        method='GET'
    )

    try:
        logger.info("Realizando solicitud a la API: %s", url)
        
        with urllib.request.urlopen(request, timeout=10) as response:
            logger.info("Respuesta recibida con código: %d", response.status)
            
            if response.status == 200:
                datos_json = json.loads(response.read().decode())

                # Reemplazo del código NAICS2022 por "Restaurantes"
                for registro in datos_json[1:]:
                    if len(registro) > 2:
                        registro[2] = "Restaurantes"

                logger.info("Datos obtenidos y transformados correctamente. Cantidad de registros: %d", len(datos_json) - 1)

                # Guardar en S3 en output/
                fecha_actual = datetime.now().strftime('%Y-%m-%d')
                file_key = f'output/census_todos_estados_{fecha_actual}.json'
                
                logger.info("Subiendo datos a S3 en: %s/%s", bucket_name, file_key)
                
                s3.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=json.dumps(datos_json),
                    ContentType='application/json'
                )

                logger.info("Archivo guardado correctamente en S3.")

                # Eliminar la primera fila (cabecera)
                datos_sin_cabecera = datos_json[1:]

                # Llamar a la función de carga en MySQL
                logger.info("Iniciando carga de datos en MySQL.")
                cargar_a_mysql(datos_sin_cabecera)

            else:
                logger.error("Error al obtener datos: código de respuesta %d", response.status)

    except urllib.error.URLError as e:
        logger.error("Error de red al obtener datos: %s", str(e))
    except json.JSONDecodeError as e:
        logger.error("Error al decodificar JSON: %s", str(e))
    except Exception as e:
        logger.error("Error inesperado: %s", str(e))

    logger.info("Ejecución de Lambda finalizada.")
    
    return {
        'statusCode': 200,
        'body': 'Datos subidos a S3 (output/) y MySQL'
    }


def cargar_a_mysql(datos):
    logger.info("Conectando a la base de datos MySQL.")

    try:
        conexion = pymysql.connect(
            host="proyecto-final-db.cz0m0gyimazl.sa-east-1.rds.amazonaws.com",
            user="nahuelfns",
            password="Gimnasia#22",
            database="proyectofinal",
            cursorclass=pymysql.cursors.DictCursor
        )

        with conexion.cursor() as cursor:
            for i, registro in enumerate(datos):
                if len(registro) < 7:
                    logger.warning("Registro inválido en posición %d: %s", i, registro)
                    continue

                year = int(registro[0])
                name = registro[1]
                sector = registro[2]
                cod_sector = int(registro[6]) if registro[6].isdigit() else None
                rcptot = int(registro[3]) if registro[3].isdigit() else None
                emp = int(registro[4]) if registro[4].isdigit() else None
                estab = int(registro[5]) if registro[5].isdigit() else None
                state = registro[7]

                sql = """
                INSERT INTO census_old (year, name, sector, rcptot, emp, estab, cod_sector, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    rcptot = VALUES(rcptot), 
                    emp = VALUES(emp), 
                    estab = VALUES(estab)
                """
                cursor.execute(sql, (year, name, sector, rcptot, emp, estab, cod_sector, state))

                if i % 100 == 0:  # Log cada 100 registros
                    logger.info("Insertados %d registros en MySQL...", i + 1)

        conexion.commit()
        logger.info("Datos insertados o actualizados correctamente en MySQL.")

    except pymysql.MySQLError as e:
        logger.error("Error en MySQL: %s", str(e))
    except Exception as e:
        logger.error("Error inesperado en MySQL: %s", str(e))
    finally:
        conexion.close()
        logger.info("Conexión MySQL cerrada.")

# V1 (me la envío facu por wsp el 27-03-2025)

import urllib.request
import boto3
import json
import pymysql
from datetime import datetime

def lambda_handler(event, context):
    # Cliente de S3
    s3 = boto3.client('s3')
    bucket_name = 'proyecto-final-ingesta-manual'
    
    # URL con for=state:* para obtener datos de todos los estados a la vez
    url = "https://api.census.gov/data/2022/ecnbasic?get=YEAR,NAME,NAICS2022,RCPTOT,EMP,ESTAB&for=state:*&NAICS2022=722513"
    
    request = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0'},
        method='GET'
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status == 200:
                datos_json = json.loads(response.read().decode())

                # Cambiar 'NAICS2022' a 'sector'
                for registro in datos_json[1:]:
                    if len(registro) > 2:
                        registro[2] = "Restaurantes"  # Cambio de código NAICS a nombre

                # Guardar en S3
                fecha_actual = datetime.now().strftime('%Y-%m-%d')
                file_key = f'censo/census_todos_estados_{fecha_actual}.json'
                s3.put_object(
                    Bucket=bucket_name,
                    Key=file_key,
                    Body=json.dumps(datos_json),
                    ContentType='application/json'
                )

                print(f"Datos de todos los estados subidos correctamente a {file_key}")

                # Eliminar la primera fila (nombres de las columnas)
                datos_sin_cabecera = datos_json[1:]

                # Llamar a la función para cargar en MySQL
                cargar_a_mysql(datos_sin_cabecera)

            else:
                print(f"Error al obtener datos: {response.status}")

    except Exception as e:
        print(f"Error al obtener datos: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Datos de todos los estados subidos a S3 y MySQL'
    }

def cargar_a_mysql(datos):
    conexion = pymysql.connect(
        host="proyecto-final-db.cz0m0gyimazl.sa-east-1.rds.amazonaws.com",
        user="nahuelfns",
        password="Gimnasia#22",
        database="proyectofinal",
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with conexion.cursor() as cursor:
            for registro in datos:
                print(f"Registro recibido: {registro}")  # Depuración

                # Verificar que la fila tenga al menos 7 elementos
                if len(registro) < 7:
                    print(f"Registro inválido (muy corto): {registro}")
                    continue  # Saltar filas incorrectas

                year = int(registro[0])  # "2022"
                name = registro[1]  # "Alabama"
                sector = registro[2]
                cod_sector = int(registro[6]) if registro[6].isdigit() else None
                rcptot = int(registro[3]) if registro[3].isdigit() else None
                emp = int(registro[4]) if registro[4].isdigit() else None
                estab = int(registro[5]) if registro[5].isdigit() else None
                state = registro[7]  # Código del estado

                sql = """
                INSERT INTO census_old (year, name, sector, rcptot, emp, estab, cod_sector, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    rcptot = VALUES(rcptot), 
                    emp = VALUES(emp), 
                    estab = VALUES(estab)
                """
                cursor.execute(sql, (year, name, sector, rcptot, emp, estab, cod_sector, state))

        conexion.commit()
        print("Datos insertados o actualizados correctamente")

    except Exception as e:
        print(f"Error MySQL: {str(e)}")

    finally:
        conexion.close()

# Original

import urllib.request
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    estados = {
        "ny": "36",
        "nj": "34"
    }

    s3 = boto3.client('s3')
    bucket_name = 'pf-datos-sin-procesar/input'  # Acá va tu bucket real

    for nombre, codigo in estados.items():
        url = f"https://api.census.gov/data/2022/ecnbasic?get=NAICS2022_LABEL,EMP,PAYANN,ESTAB&for=state:{codigo}"
        request = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'},
            method='GET'
        )

        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status == 200:
                    datos_json = json.loads(response.read().decode())

                    # Guardar con fecha en el nombre del archivo
                    fecha_actual = datetime.now().strftime('%Y-%m-%d')
                    file_key = f'censo/census_{nombre}_{fecha_actual}.json'

                    # Convertir el JSON a string antes de subirlo a S3
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=file_key,
                        Body=json.dumps(datos_json),
                        ContentType='application/json'
                    )

                    print(f"Datos de {nombre.upper()} subidos correctamente a {file_key}")
                else:
                    print(f"Error al obtener datos de {nombre.upper()}: {response.status}")
        except urllib.error.URLError as e:
            print(f"Error al obtener datos de {nombre.upper()}: {e.reason}")
        except urllib.error.HTTPError as e:
            print(f"Error HTTP al obtener datos de {nombre.upper()}: {e.code}")
        except Exception as e:
            print(f"Error inesperado al obtener datos de {nombre.upper()}: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Datos de NY y NJ subidos a S3'
    }