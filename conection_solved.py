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

try:
    # Cargar datos desde el archivo CSV
    df = pd.read_csv('Files/HR RAW DATA CLEAN.csv')

    # Reemplazar 'Unknown' por NaN para la manipulación
    df['EmployeeNumber'] = df['EmployeeNumber'].replace('Unknown', pd.NA)

    # Asignar números ascendentes empezando desde 1
    df['EmployeeNumber'] = range(1, len(df) + 1)

    # Convertir 'EmployeeNumber' a tipo int nativo de Python
    df['EmployeeNumber'] = df['EmployeeNumber'].apply(lambda x: int(x))
    df['EmployeeNumber'] = df['EmployeeNumber'].astype(object)


    # Verificar los primeros números asignados
    print(df[['EmployeeNumber']].head())

    # Preparar los datos para la inserción en la base de datos
    query_personaldetails = """
        INSERT INTO `PersonalDetails`
        (`EmployeeNumber`, `Age`, `Education`, `EducationField`, `Gender`, `MaritalStatus`, `DateBirth`)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        `Age`=VALUES(`Age`),
        `Education`=VALUES(`Education`),
        `EducationField`=VALUES(`EducationField`),
        `Gender`=VALUES(`Gender`),
        `MaritalStatus`=VALUES(`MaritalStatus`),
        `DateBirth`=VALUES(`DateBirth`)
    """

    # Preparar los datos para la inserción
    data_table_personaldetails = list(zip(
        df["EmployeeNumber"].values,
        df["Age"].values,
        df["Education"].values,
        df["EducationField"].values,
        df["Gender"].values,
        df["MaritalStatus"].values,
        df["DateBirth"].values
    ))

    # Insertar los datos en la base de datos
    cursor = cnx.cursor()
    cursor.executemany(query_personaldetails, data_table_personaldetails)
    cnx.commit()

    print("Datos insertados correctamente")

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
