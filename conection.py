# !pip install mysql-connector
# !pip install mysql-connector-python

#%%

# IMPORTS
# Libraries for data processing
import pandas as pd
import numpy as np

# Libaries for Mysql conection
import mysql.connector
from mysql.connector import errorcode

pd.set_option('display.max_columns', None) 


# We create the connection with the arguments
cnx = mysql.connector.connect(user='root', password='AlumnaAdalab',
                              host='127.0.0.1',
                              database='mydb')

print(cnx)
cnx.close()

# We do a try except, this allowed us to do error handling, to prevent our code from stalling. So what we're doing is
## try to make the connection to the BBDD 
try:
  cnx = mysql.connector.connect(user='root', password='AlumnaAdalab',
                              host= '127.0.0.1',
                              database='mydb')
# If there is some error do:
except mysql.connector.Error as err:

  # If it's an error with the password return me an access denied message as we have problems with the password.
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print('Something is wrong with your user name or password')
  
  # If the error is the database does not exist, return a message that the database does not exist
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print('Database does not exist')
  
  # If it's not for any of the above errors, print me what error I'm getting on my connection
  else:
    print(err)
else:
  cnx.close()

# DATA INSERTION
# Previous to this step, we would create our database with its corresponding tables and restrictions. 
# but, in our case, we have already created it with MySQL Workbench, so let's proceed to the insertion of the data.

try:
    # Cargar datos desde el archivo CSV
    df = pd.read_csv('Files/HR RAW DATA CLEAN.csv')

    # Configuración de la conexión
    cnx = mysql.connector.connect(
        user='root',
        password='AlumnaAdalab',
        host='127.0.0.1',
        database='mydb'
    )

    cursor = cnx.cursor()

    # Consulta SQL de inserción
    query_personaldetails = """
        INSERT INTO 'personaldetails'
        ('EmployeeNumber', 'Age', 'Education', 'EducationField', 'Gender', 'MaritalStatus', 'DateBirth')
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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

    # Ejecutar la inserción masiva
    cursor.executemany(query_personaldetails, data_table_personaldetails)
    cnx.commit()

    print("Datos insertados correctamente")

except mysql.connector.Error as err:
        print(f"Error al ejecutar la consulta: {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'cnx' in locals() and cnx.is_connected():
        cnx.close()
        print("Conexión cerrada")

def convertir_int(lista_tuplas):
    datos_tabla_caract_def = []
    for tupla in lista_tuplas:
        lista_intermedia = []
        for elemento in tupla:
            try:
                lista_intermedia.append(int(elemento))
            except:
                lista_intermedia.append(elemento)
            
        datos_tabla_caract_def.append(tuple(lista_intermedia))
    
    return datos_tabla_caract_def

# Creas una función que te pase las columnas que haya que pasar a int. Crea una lista con los que ha podido convertir en int y los que no, sería algo así:



# %%
