# -*- coding: utf-8 -*-
"""Rendimiento Observadores.ipynb"""
import datetime as dt
import pandas as pd
import string as str
import numpy as np

#docuemntos de Kobotoolbox
df_auto = pd.read_csv('/Formulario_estaciones_automaticas.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'],on_bad_lines='skip')
df_conve = pd.read_csv('/Formulario_estaciones_climaticas_convencionales.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'])
df_sinop = pd.read_csv('/Formulario_estaciones_sinopticas.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'],on_bad_lines='skip')

df_auto_drop = df_auto
df_conve_drop = df_conve
df_sinop_drop = df_sinop

# Convierte las columnas a formato de fecha
df_conve_drop['start'] = pd.to_datetime(df_conve_drop['start'], utc=True)
df_conve_drop['end'] = pd.to_datetime(df_conve_drop['end'], utc=True)
df_conve_drop['_submission_time'] = pd.to_datetime(df_conve_drop['_submission_time'], utc=True)


# Convierte las columnas a formato de fecha
df_auto_drop['start'] = pd.to_datetime(df_auto_drop['start'], utc=True)
df_auto_drop['end'] = pd.to_datetime(df_auto_drop['end'], utc=True)
df_auto_drop['_submission_time'] = pd.to_datetime(df_auto_drop['_submission_time'], utc=True)

# Convierte las columnas a formato de fecha y hora
df_sinop_drop['start'] = pd.to_datetime(df_sinop_drop['start'], utc=True)
df_sinop_drop['end'] = pd.to_datetime(df_sinop_drop['end'], utc=True)
df_sinop_drop['_submission_time'] = pd.to_datetime(df_sinop_drop['_submission_time'], utc=True)
# Eliminar la zona horaria
df_conve_drop['start2'] = df_conve_drop['start'].dt.tz_localize(None)
df_conve_drop['end2'] = df_conve_drop['end'].dt.tz_localize(None)
df_conve_drop['_submission_time2'] = df_conve_drop['_submission_time'].dt.tz_localize(None)

# Eliminar la zona horaria
df_auto_drop['start2'] = df_auto_drop['start'].dt.tz_localize(None)
df_auto_drop['end2'] = df_auto_drop['end'].dt.tz_localize(None)
df_auto_drop['_submission_time2'] = df_auto_drop['_submission_time'].dt.tz_localize(None)
# Eliminar la zona horaria
df_sinop_drop['start2'] = df_sinop_drop['start'].dt.tz_localize(None)
df_sinop_drop['end2'] = df_sinop_drop['end'].dt.tz_localize(None)
df_sinop_drop['_submission_time2'] = df_sinop_drop['_submission_time'].dt.tz_localize(None)


df_auto_drop['Tiempo_llenado_segundos'] = df_auto_drop['end2'] - df_auto_drop['start2']
df_conve_drop['Tiempo_llenado_segundos'] = df_conve_drop['end2'] - df_conve_drop['start2']
df_sinop_drop['Tiempo_llenado_segundos'] = df_sinop_drop['end2'] - df_sinop_drop['start2']
#pd.Timedelta(x) para convertir el valor de la columna 'diferencia_tiempo' en un objeto de tiempo, y luego utilizamos .total_seconds() para obtener la cantidad total de segundos.
df_auto_drop['Tiempo_llenado_segundos'] = df_auto_drop['Tiempo_llenado_segundos'].apply(lambda x: pd.Timedelta(x).total_seconds())
df_conve_drop['Tiempo_llenado_segundos'] = df_conve_drop['Tiempo_llenado_segundos'].apply(lambda x: pd.Timedelta(x).total_seconds())
df_sinop_drop['Tiempo_llenado_segundos'] = df_sinop_drop['Tiempo_llenado_segundos'].apply(lambda x: pd.Timedelta(x).total_seconds())



#Tiempo de llegada a la base de Kobotoolbox
#df_conve_drop['end'] = df_conve_drop['end'].dt.tz_localize(None)
df_auto_drop['Tiempo_llegada_DB_seg'] = df_auto_drop['_submission_time2'] - df_auto_drop['end2']
df_conve_drop['Tiempo_llegada_DB_seg'] = df_conve_drop['_submission_time2'] - df_conve_drop['end2']
df_sinop_drop['Tiempo_llegada_DB_seg'] = df_sinop_drop['_submission_time2'] - df_sinop_drop['end2']



#Tiempo total
df_auto_drop['Tiempo_total'] = df_auto_drop['_submission_time2'] - df_auto_drop['start2']
df_conve_drop['Tiempo_total'] = df_conve_drop['_submission_time2'] - df_conve_drop['start2']
df_sinop_drop['Tiempo_total'] = df_sinop_drop['_submission_time2'] - df_sinop_drop['start2']
#concatena las 3 tablas
info_total = pd.concat([df_conve_drop, df_auto_drop, df_sinop_drop], ignore_index=True)

#pasa a mayuscula
info_total['estacion'] = info_total['estacion'].str.upper()


#pasar nombre de estación al ID
#Diccionario que remplaza todos los nombres de las estaciones por los codigos de las mismas
ID={}

info_total['estacion'] = info_total['estacion'].replace(ID)

info_total_id = info_total
info_total_id2 = info_total_id.drop(columns = ["end","start","_submission_time"])


info_total_id3 = info_total_id2.rename(columns = {'estacion': 'CODIGO','end2': 'FINAL_FORMULARIO','start2':'INICIO_FORMULARIO','_submission_time2':'FORMULARIO_ENVIADO', '_submitted_by':'ENVIADO_POR'})


info_total_id3.to_csv('rendimiento_general.csv',index=False)
