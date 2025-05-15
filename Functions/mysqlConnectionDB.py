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
            print("Proceso: Conexión exitosa a la base de datos!")

    except Error as e:
            print(f"Proceso: Error al conectar con MySQL: {e}")
            return None
            
    return conectionMySQL


def create_table_mysql_db(connection_Mysql_param, table_name_param, table_columns_param):
    mysql_cursor = connection_Mysql_param.cursor()

    mysql_cursor.execute(f"SHOW TABLES LIKE'{table_name_param}'")
    result = mysql_cursor.fetchone()


    data_columns = table_columns_param

    columns_definition = ",".join([f"{value} VARCHAR(255)" for value in data_columns]) 

    if result is None:
        insert_query =f'''
            CREATE TABLE {table_name_param} (
                {columns_definition}
            )
        '''
        mysql_cursor.execute(insert_query)
        print(f"Proceso: Tabla {table_name_param} creada exitosamente.")
    else:
        print(f"Proceso: La tabla {table_name_param} ya existe.")

    return