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

def insert_data_mysql_db(connection_Mysql_param, table_name_param, table_columns_param, data_for_insert_mysql_param):
    mysql_cursor = connection_Mysql_param.cursor()
    #print(data_for_insert_mysql_param[:5])
    try:

        data_transformed_str = table_columns_param
        data_transformed_str = ", ".join(table_columns_param) #", ".join(...) - Une todos los elementos de la lista con una coma y un espacio entre ellos.

        #print("Proceso: contenido de data_transformed_str: ",data_transformed_str)
        #print("Proceso: Tipo de variable de data_transformed_str: ",type(data_transformed_str))

        for count in range(len(data_for_insert_mysql_param)):
            transform_data = data_for_insert_mysql_param.iloc[count]
            transform_data.to_list() # accede a la fila en la posición count del DataFrame usando el método iloc (indexación basada en posición).
            
            #print("Proceso: Esta es la data: ",transform_data)
            #print("Proceso: Tipo de variable de transform_data: ",type(transform_data))
            

        # Filtro 1: Proceso para eliminar cualquier campo que venga vacio y reemplazarlo por un NULL     
        data_without_empty_spaces = []
        for count_no_space in transform_data [:5]:
            print(count_no_space)
            print(type(count_no_space))
            #if isinstance(count_no_space, str) and count_no_space.strip() == "":
            #    data_without_empty_spaces.append('NULL')
            #else:
            #    data_without_empty_spaces.append(count_no_space)
        
        #print("Proceso: contenido de  data_without_empty_spaces: ",data_without_empty_spaces)
        #print("Proceso: tipo de variable data_without_empty_spaces: ",type(data_without_empty_spaces))



        #Filtro 2: Recibe la data del Filtro 1 y elimina los caracteres "[ ]" que tiene la vaiarble transform_data que ahora data_without_empty_spaces. Es una tipo Lista.
        """ clean_transform_data = []
        for count_no_character in data_without_empty_spaces:
            data_without_characters = count_no_character.replace("[", "").replace("]", "")
            clean_transform_data.append(data_without_characters) """
        
        insert_mysql_data = f'''
            INSERT INTO {table_name_param}({data_transformed_str}) VALUES ({data_without_empty_spaces})
        '''

        print("Proceso: insert script: ",insert_mysql_data)
        #mysql_cursor.execute(insert_mysql_data)
        #connection_Mysql_param.commit()
    except Error as e:
        print(f"Proceso: Error al conectar con MySQL: {e}")
        return None
    
    return (print("Proceso: Datos insertados correctamente en la tabla."))
    
         
     