# -*- coding: utf-8 -*-
"""Rendimiento Observadores.ipynb"""
import datetime as dt
import pandas as pd
import string as str
import numpy as np

#docuemntos de Kobotoolbox
df_auto = pd.read_csv('/home/clima/salida_auto_kobo/Formulario_estaciones_automaticas.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'],on_bad_lines='skip')
df_conve = pd.read_csv('/home/clima/salida_auto_kobo/Formulario_estaciones_climaticas_convencionales.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'])
df_sinop = pd.read_csv('/home/clima/salida_auto_kobo/Formulario_estaciones_sinopticas.csv', header = 0,sep=";",usecols=['start','end','estacion','Hora','_id','_submission_time','_submitted_by'],on_bad_lines='skip')

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
ID={"RETALHULEU_AEROPUERTO":"INS110101CV","CHAMPERICO_FEGUA":"INS110701CV","COBAN":"INS160101CV","ESQUIPULAS":"INS200701CV","FLORES_AEROPUERTO":"INS170101CV",\
 "LA_AURORA":"INS010102CV","LA_FRAGUA":"INS190201CV","LOS_ALTOS":"INS090101CV","MONTUFAR":"INS221401CV",\
 "PUERTO_BARRIOS_PHC":"INS180101CV","RETALHULEU_AEROPUERTO":"INS110101CV","HUEHUETENANGO":"INS130101CV",\
 "POPTUN":"INS171201CV","SAN_JOSE_AEROPUERTO":"INS050901CV","TECUN_UMAN":"INS121701CV",\
 "RETALHULEU_AREOPUERTO":"INS110101CV","SAN_JOSE_AREOPUERTO":"INS050901CV",
 "CAMOTAN":"INS200501CV","CATARINA":"INS121601CV","EL_CAPITAN":"INS071301CV","LA_CEIBITA":"INS210601CV",\
 "LABOR_OVALLE":"INS090301CV","MAZATENANGO":"INS100101CV","PANZÓS_PHC_ALTAVERAPAZ":"INS160701CV",\
 "SACAPULAS":"INS141601CV","SAN_MARCOS_PHC":"INS120101CV","SANTA_CRUZ_BALANYA":"INS041001CV",\
 "TODOS_SANTOS":"INS131501CV","INSIVUMEH":"INS010101CV","ALAMEDA_ICTA":"INS040101CV",\
 "ASUNCION_MITA":"INS220501CV","CHINIQUE":"INS140301CV","EL_TABLON":"INS070101CV","LA_UNION":"INS190901CV",\
 "LAS_VEGAS_PHC":"INS180201CV","NEBAJ":"INS141301CV","QUEZADA":"INS221701CV","SABANA_GRANDE":"INS050101CV",\
 "SAN_AGUSTIN_ACASAGUASTLAN":"INS020302CV","SAN_JERONIMO":"INS150701CV","SAN_PEDRO_NECTA":"INS130601CV",\
 "SANTA_MARIA_CAHABON":"INS161201CV","SANTIAGO_ATITLAN":"INS071901CV","SUIZA_CONTENTA":"INS030801CV",\
 "CHIXOY_PCH":"INS141901CV","CUBULCO":"INS150401CV","LOS_ALBORES":"INS020301CV","LOS_ESCLAVOS":"INS060101CV",\
 "PASABIEN":"INS190301CV","POTRERO_CARRILLO":"INS210101CV","SAN_MARTIN_JILOTEPEQUE":"INS040301CV","PANZOS_PHC_ALTA_VERAPAZ":"INS160701CV","SAN_JERONIMO_R_H":"INS150701CV",\
 "CAMOTAN":"INS200501CV", "CAMOTAN":"INS200501CV","CATARINA":"INS121601CV","EL_CAPITAN":"INS071301CV","LA_CEIBITA":"INS210601CV",\
 "LABOR_OVALLE":"INS090301CV","MAZATENANGO":"INS100101CV","PANZÓS_PHC_ALTAVERAPAZ":"INS160701CV",\
 "SACAPULAS":"INS141601CV","SAN_MARCOS_PHC":"INS120101CV","SANTA_CRUZ_BALANYA":"INS041001CV",\
 "TODOS_SANTOS":"INS131501CV","INSIVUMEH":"INS010101CV","ALAMEDA_ICTA":"INS040101CV",\
 "ASUNCION_MITA":"INS220501CV","CHINIQUE":"INS140301CV","EL_TABLON":"INS070101CV","LA_UNION":"INS190901CV",\
 "LAS_VEGAS_PHC":"INS180201CV","NEBAJ":"INS141301CV","QUEZADA":"INS221701CV","SABANA_GRANDE":"INS050101CV",\
 "SAN_AGUSTIN_ACASAGUASTLAN":"INS020302CV","SAN_JERONIMO":"INS150701CV","SAN_PEDRO_NECTA":"INS130601CV",\
 "SANTA_MARIA_CAHABON":"INS161201CV","SANTIAGO_ATITLAN":"INS071901CV","SUIZA_CONTENTA":"INS030801CV",\
 "CHIXOY_PCH":"INS141901CV","CUBULCO":"INS150401CV","LOS_ALBORES":"INS020301CV","LOS_ESCLAVOS":"INS060101CV",\
 "PASABIEN":"INS190301CV","POTRERO_CARRILLO":"INS210101CV","SAN_MARTIN_JILOTEPEQUE":"INS040301CV","PANZOS_PHC_ALTA_VERAPAZ":"INS160701CV","SAN_JERONIMO_R_H":"INS150701CV",\
 "AMATITLAN":"INS011401AT","ANTIGUA_GUATEMALA":"INS030101AT","CONCEPCION":"INS050101AT","IXCHIGUAN":"INS122301AT","JALAPA":"INS210101AT","LA_REFORMA":"INS122101AT",\
 "LAS_NUBES":"INS010331CV","LO_DE_COY":"INS010801AT","MARISCOS":"INS180501AT","MORALES_MET":"INS180401AT","NENTON":"INS130501AT","NUEVA_CONCEPCION":"INS090901AT",\
 "PACHUTE":"INS090401AT","PLAYA_GRANDE_IXCAN":"INS142201AT","SAN_JOSE_PINULA":"INS010301AT","SAN_PEDRO_AYAMPUC":"INS010701AT","SANTA_CRUZ_DEL_QUICHE":"INS140101AT",\
 "SANTA_MARGARITA":"INS040801AT","TOTONICAPAN":"INS080101AT","CHINIQUE":"INS140301CV","SANTA_CRUZ_DEL_QUICHE":"INS140101AT","RETALHULEU_AEROPUERTO":"INS110101CV" ,\
 }

info_total['estacion'] = info_total['estacion'].replace(ID)

info_total_id = info_total
info_total_id2 = info_total_id.drop(columns = ["end","start","_submission_time"])


info_total_id3 = info_total_id2.rename(columns = {'estacion': 'CODIGO','end2': 'FINAL_FORMULARIO','start2':'INICIO_FORMULARIO','_submission_time2':'FORMULARIO_ENVIADO', '_submitted_by':'ENVIADO_POR'})


info_total_id3.to_csv('/home/clima/Desktop/NO_BORRAR/rendimiento_general.csv',index=False)