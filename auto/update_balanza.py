import pyodbc
import pandas as pd
import os
from datetime import datetime, timedelta
from tokens import username, password, url_supabase, key_supabase
from supabase_connection import delete_table_data, insert_table_data, update_log
from decimal import Decimal
import time


if os.path.exists('//dc01/Usuarios/PowerBI/flastra/Documents/dassa_operativo_stream/auto'):
    os.chdir('//dc01/Usuarios/PowerBI/flastra/Documents/dassa_operativo_stream/auto')
elif os.path.exists('C:/Users/facun/OneDrive/Documentos/GitHub/dassa_operativo_stream/auto'):
    os.chdir('C:/Users/facun/OneDrive/Documentos/GitHub/dassa_operativo_stream/auto')
else:
    print("Se usa working directory por defecto")

path = '//dc01/Usuarios/PowerBI/flastra/Documents/dassa_operativo_stream/auto'

print('Descargando datos de SQL')
server = '101.44.8.58\\SQLEXPRESS_X86,1436'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';UID='+username+';PWD='+ password)
cursor = conn.cursor()

fecha = datetime.now().strftime('%Y-%m-%d')

cursor.execute(f"""
SELECT idpesada, cl_nombre, cl_cuit, ata_nombre, ata_cuit, contenedor, entrada, salida, peso_bruto, peso_tara, peso_neto, peso_merca, tara_cnt, desc_merc, patente_ch, patente_se, chofer, tipodocum, numdoc, observa, tipo_oper, booking, permisoemb, precinto
FROM DEPOFIS.DASSA.BALANZA_PESADA
WHERE entrada >= '{fecha}'
""")

rows = cursor.fetchall()
columns = [column[0] for column in cursor.description]
balanza = pd.DataFrame.from_records(rows, columns=columns)

columns_to_strip = ['cl_nombre', 'ata_nombre', 'chofer', 'tipo_oper', 'desc_merc', 'observa', 'patente_ch', 'patente_se', 'contenedor', 'booking']
for col in columns_to_strip:
    balanza[col] = balanza[col].str.strip()

columns_to_title = ['cl_nombre', 'ata_nombre', 'chofer', 'tipo_oper', 'observa']
for col in columns_to_title:
    balanza[col] = balanza[col].str.title()
balanza['entrada'] = pd.to_datetime(balanza['entrada']).dt.strftime('%H:%M')
balanza['salida'] = pd.to_datetime(balanza['salida']).dt.strftime('%H:%M')
balanza['salida'] = balanza['salida'].apply(lambda x: '-' if x == '00:00' else x)
balanza['contenedor'] = balanza['contenedor'].replace('', '-')
balanza['contenedor'] = balanza['contenedor'].replace('-      -', '-')
balanza['permisoemb'] = balanza['permisoemb'].str.strip()
balanza['precinto'] = balanza['precinto'].str.strip()
balanza['precinto'] = balanza['precinto'].replace('', '-')
balanza['numdoc'] = balanza['numdoc'].str.strip()
balanza['tipodocum'] = balanza['tipodocum'].str.strip()
balanza['Estado'] = balanza['salida'].apply(lambda x: 'En curso' if x == '-' else 'Pesado')
peso_columns = ['peso_bruto', 'peso_tara', 'peso_neto', 'peso_merca']
for col in peso_columns:
    balanza[col] = balanza[col].astype(int).astype(str)
    balanza[col] = balanza[col].replace('0', '-')
balanza.columns = ['ID Pesada', 'Cliente', 'CUIT Cliente', 'ATA', 'CUIT ATA', 'Contenedor', 'Entrada', 'Salida', 'Peso Bruto', 'Peso Tara', 'Peso Neto', 'Peso Mercadería','Tara CNT', 'Descripción', 'Patente Chasis', 'Patente Semi', 'Chofer', 'Tipo Doc', 'DNI', 'Observaciones', 'tipo_oper', 'Booking', 'Permiso Emb.', 'Precinto', 'Estado']
balanza = balanza.sort_values(by='Salida', ascending=False).drop_duplicates(subset=['Cliente', 'ATA', 'Peso Bruto'], keep='first')
balanza = balanza.sort_values(by='ID Pesada', ascending=False)
balanza['Patente Semi'] = balanza['Patente Semi'].str.strip().str.replace(' ', '')

# Convert Decimal objects to float
balanza = balanza.apply(lambda col: col.map(lambda x: float(x) if isinstance(x, Decimal) else x))

print('Subiendo datos a Supabase')
delete_table_data("balanza_data")
time.sleep(3)  # Wait for the deletion to complete
insert_table_data("balanza_data", balanza.to_dict(orient="records"))

update_log('Balanza')

