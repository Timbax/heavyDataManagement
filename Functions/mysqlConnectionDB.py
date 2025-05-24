import mysql.connector
from mysql.connector import Error
import pandas as pd

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
            print("Proceso: Conexión exitosa a la base de datos.")

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
        create_mysql_table =f'''
            CREATE TABLE {table_name_param} (
                {columns_definition}
            )
        '''
        mysql_cursor.execute(create_mysql_table)
        print(f"Proceso: Tabla {table_name_param} creada exitosamente.")
    else:
        print(f"Proceso: La tabla {table_name_param} ya existe.")

    return

def insert_data_mysql_db(connection_Mysql_param, table_name_param, df_columns_param, df_for_insert_mysql_param):
    mysql_cursor = connection_Mysql_param.cursor()
    
    try:

        data_columns_into_str = df_columns_param
        # Obtiene los nombres de todas las columnas del DataFrame
        data_columns_into_str = ", ".join(df_columns_param) # ", ".join(...) - Une todos los elementos de la lista con una coma y un espacio entre ellos.

        
        for _, row in df_for_insert_mysql_param.iterrows():
            # RECUERDA: iterrows() se utiliza para iterar o recorrer sobre las filas de un DataFrame. (FILAS - ) y (COLUMNAS | )
            # El for_, hace referencia a que el for debe de ignorar el inide de la de la fila. 
           
            df_values = []
            for column in df_for_insert_mysql_param.columns:
                # row[column]: obtiene el valor específico de esa columna en la fila actual
                # f'"{row[column]}"': Encierra el valor entre comillas dobles
                # df_values.append(): Agrega cada valor con comillas a la lista llamada "df_values"

                if pd.isna(row[column]) or str(row[column]).strip() == "":
                    df_values.append("NULL")
                else:
                    df_values.append(f'"{row[column]}"')         
            
            row_data_str = ", ".join(df_values)
            # Toma la lista df_values con todos los valores entre comillas
            # Los une con comas y espacios

            insert_mysql_data = f'''
                INSERT INTO {table_name_param}({data_columns_into_str}) VALUES ({row_data_str})
            '''

            #print("Proceso: insert script: ",insert_mysql_data)
       
            mysql_cursor.execute(insert_mysql_data)
            connection_Mysql_param.commit()
        
        
    except Error as e:
        print(f"Proceso: Error al conectar con MySQL: {e}")
        return None
    
    return print(f'Proceso: Datos insertados correctamente en la tabla {table_name_param}.\nTotal de inserciones procesadas: {len(df_for_insert_mysql_param)} ')
    
def close_mysql_connection(connection_Mysql_param):
    
    try:
        if connection_Mysql_param.is_connected():
                connection_Mysql_param.close()

    except Error as e:

        print(f"Error al cerrar conexion de MySQL: {e}")
        return None         
    return(print("Proceso: Conexión a la base de datos cerrada."))  
     