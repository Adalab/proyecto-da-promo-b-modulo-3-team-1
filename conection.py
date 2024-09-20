# !pip install mysql-connector
# !pip install mysql-connector-python

#%%

# IMPORTS
# Libraries for data processing
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

pd.set_option('display.max_columns', None)


# Función para manejo de errores y conexión a la base de datos
def create_connection():
    try:
        cnx = mysql.connector.connect(user='root', password='AlumnaAdalab',
                                      host='127.0.0.1',
                                      database='abc_corp_employees')
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Something is wrong with your user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else:
            print(err)
        return None


# Intentar crear la conexión
cnx = create_connection()
if cnx:
    print("Conexión exitosa:", cnx)
    cnx.close()

# DATA INSERTION
# Procedemos a la inserción de datos

try:
    # Cargar datos desde el archivo CSV
    df = pd.read_csv('Files/HR RAW DATA CLEAN.csv')

    # Filtrar filas donde `EmployeeNumber` no es 'Unknown'
    df_filtered = df[df["EmployeeNumber"] != 'Unknown']

    # Configuración de la conexión
    cnx = create_connection()
    if cnx is None:
        raise Exception("No se pudo establecer la conexión a la base de datos")

    cursor = cnx.cursor()

    # Consulta SQL de inserción (comillas invertidas en los nombres de tablas y columnas)
    query_personaldetails = """
        INSERT INTO `personaldetails`
        (`EmployeeNumber`, `Age`, `Education`, `EducationField`, `Gender`, `MaritalStatus`, `DateBirth`)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # Preparar los datos para la inserción sin preocuparse por valores nulos
    data_table_personaldetails = list(zip(
        df_filtered["EmployeeNumber"].values,
        df_filtered["Age"].values,
        df_filtered["Education"].values,
        df_filtered["EducationField"].values,
        df_filtered["Gender"].values,
        df_filtered["MaritalStatus"].values,
        df_filtered["DateBirth"].values
    ))

    # Convertir columnas que deban ser enteros
    def convert_int(lista_tuplas):
        data_table_def = []
        for tupla in lista_tuplas:
            lista_intermedia = []
            for elemento in tupla:
                try:
                    lista_intermedia.append(int(elemento))
                except ValueError:
                    lista_intermedia.append(elemento)
            data_table_def.append(tuple(lista_intermedia))
        return data_table_def

    # Convertir los datos a enteros donde sea necesario
    personal_details = convert_int(data_table_personaldetails)

    # Ejecutar la inserción masiva
    cursor.executemany(query_personaldetails, personal_details)
    cnx.commit()

    print("Datos insertados correctamente")

    # Id con autoincremento (no es necesario después de `executemany`)
    last_employee_number = cursor.lastrowid
    print(f"Último ID insertado: {last_employee_number}")

except mysql.connector.Error as err:
    print(f"Error al ejecutar la consulta: {err}")
except Exception as e:
    print(f"Error general: {e}")
finally:
    if cursor:
        cursor.close()
    if cnx and cnx.is_connected():
        cnx.close()


# %%
