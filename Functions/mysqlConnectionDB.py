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
    print("Proceso: datos del data frame: ",df_for_insert_mysql_param)
    print("Proceso: tipo de dato de df_for_insert_mysql_param : ",type(df_for_insert_mysql_param))
    try:

        data_columns_into_str = df_columns_param
        data_columns_into_str = ", ".join(df_columns_param) # ", ".join(...) - Une todos los elementos de la lista con una coma y un espacio entre ellos.

        #print("Proceso: contenido de data_transformed_str: ",data_transformed_str)
        #print("Proceso: Tipo de variable de data_transformed_str: ",type(data_transformed_str))
        
        for _, row_clean_data in df_for_insert_mysql_param.iterrows():
            clean_and_transform_data = row_clean_data
        
        clean_and_transform_data.to_list()
        
        print("Proceso: contenido de la varible clean_and_transform_data: ",clean_and_transform_data)
        print("Proceso: Tipo de variable de transform_data: ",type(clean_and_transform_data))
       

        
        insert_mysql_data = f'''
            INSERT INTO {table_name_param}({data_columns_into_str}) VALUES ({clean_and_transform_data})
        '''

        print("Proceso: insert script: ",insert_mysql_data)
        #print("Proceso: contenido de data_value : ",data_values)
        #mysql_cursor.execute(insert_mysql_data)
        #connection_Mysql_param.commit()
    except Error as e:
        print(f"Proceso: Error al conectar con MySQL: {e}")
        return None
    
    return (print("Proceso: Datos insertados correctamente en la tabla."))
    
         
     