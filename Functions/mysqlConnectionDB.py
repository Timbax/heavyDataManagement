import mysql.connector
from mysql.connector import Error

def mysql_db_connection():
    host = "localhost"
    port = 3306
    user = "root"  
    password = ""
    database = "add_logs_data"

    try:
        conectionMySQL =  mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database = database
        ) 

        if conectionMySQL.is_connected():
            print("Conexión exitosa a la base de datos")

    except Error as e:
            print(f"Error al conectar con MySQL: {e}")
            return None
            
    return (conectionMySQL)