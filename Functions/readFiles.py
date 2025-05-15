
import pandas as pd
 
def read_heavy_file():
    file_path = './Files/2024-09-14_2024-09-15_Consolidation_recov.txt'
    try:    
        # Leer el archivo con pandas
        heavy_data = pd.read_csv(
            file_path,
            sep=",",
            low_memory=False
        )

        print("Proceso: Archivo leido correctamente!")
        heavy_data_50 = heavy_data.head(50)
        print("Proceso: primeros 50 registros obtenidos")

        #heavy_data_columns = heavy_data.columns
        #print("Proceso Columnas: ",heavy_data_columns)

        return heavy_data_50
    
    except Exception as e:
        print(f"Proceso: Error al leer el archivo: {e}")
        return None