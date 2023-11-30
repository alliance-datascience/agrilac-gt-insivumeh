
"""
Creado por: Jose Angel Valenzuela Gomez, Sofia Tzorin Herrera y Peter Darwin Argueta Ordoñez


Descripcion:
Programa que controla los diferentes procesos creados para la automatizacion del ingreso de los datos cliamticos
a travez de la plataforma de Kobotoolbox.


Ultima midificacion 11/08/2023

"""

import json, os
import datetime as dt
import pandas as pd 
import string as str
import numpy as np
import viento as viento
import codigo_kobo_c2 as coko2
import ordenar_columnas as orden
import control_reporte as reporte
import routine as rou


#Rutas de las carpetas necesarias para este programa
#Ruta contenedora de este programa y archivos necesarios para las comparaciones y analisis 
ruta_contenedora=''

#Ruta de salida para todos los archivos creados durante los procesos.
ruta_salida=""

#Nombre de los archivos crudos que se descargan de KOBO
name_sinop= ruta_salida +'Formulario_estaciones_sinopticas.csv'
name_con=ruta_salida+'Formulario_estaciones_climaticas_convencionales.csv'
name_auto=ruta_salida+'Formulario_estaciones_automaticas.csv'
name_anulacion=ruta_salida+"Formulario_anulacion_datos_climaticos.csv"

#Nombre de los archivos limpios descargados de KOBO 
des_conven=ruta_salida+'data_con.csv'
des_sinop=ruta_salida +'data_sinop.csv'
des_auto=ruta_salida+'data_auto.csv'
des_total=ruta_salida+'data_total.csv'
des_control_hora=ruta_salida+'data_control_hora.csv' 
des_datadiaria_formato=ruta_salida+'df_datadiaria_formato.csv'
des_EstacionesConvencionales=ruta_contenedora+'EstacionesConvencionales.xlsx'
des_info_feno=ruta_salida+'info_feno.csv'

#Token de Autorizacion generado en KOBOTOOLBOX y es unico para cada una de las cuantes creadas en KOBOTOOLBOX


#Descarga de los datos desde Kobo y los renombra con los nombres de los formularios
coko2.descarga_datos(ruta_salida,ruta_salida)

#Limpian los datos dejando solamente las variables que necesitamos devuelve un dataframe y crea archivos limpios con cada uno de ellos.
df_sinop=coko2.data_sinop(name_sinop,ruta_salida) #archivo de salida "data_sinop.csv" datos de las estaciones sinopticas limpios
df_conve=coko2.data_conven(name_con,ruta_salida) #archivo de salida "data_con.csv" datos de las estaciones convencionales limpios
df_auto=coko2.data_auto(name_auto,ruta_salida) #archivo de salida "data_auto.csv" datos de las estaciones automaticas limpios
coko2.data_anulacion(name_anulacion,ruta_salida)

#Une todos los dataframe anteriores y genera un archivo con las salidas finales sin pasar por la validacion de rangos
coko2.salida_diaria(df_sinop,df_conve,df_auto,ruta_salida) #archivo de salida "datatotal.csv"

#Anulacion de Data Climatica
coko2.anulacion_data(des_sinop,des_conven,des_auto,des_total,name_anulacion)

#Calcula la predominancia de viento para todos los archivos de salida anterioes y devuelve nuevamente "data_total.csv"
viento.extraccion_viento(des_auto,des_sinop,des_conven,des_total,ruta_salida)  #archivo de salida "data_total.csv"

#Correccion de fecha de lluvia restando un dia a la fecha de la lluvia.
coko2.correccion_fecha_lluvia(des_total) #archivo de salida "data_total.csv"

#se cargan los dataframe de:
#df_datatotal contiene los datos sin pivot y sin validacion de rangos
#df_historico contiene los rangos historicos por cada estacion y por cada variables que estas contengan
df_datatotal = pd.read_csv(des_total, header = 0)
df_historico = pd.read_csv(ruta_contenedora+'MasterData Estaciones - MasterData.csv', header = 0)

#Realiza la validacion de los diferentes rangos por cada variables de cada estacion y devuelves dos archivos
#archivo de salida "Fueraderango.xlsx" contiene todos los valores fuera de rango que se hayan encontrado en la validacion de rangos
#archivo de salida "docuemnto_drive.csv" contiene todos los datos ya con la validacion de rangos (los datos fuera de rango no se borran al momento de generara este documento)
coko2.validacion_rangos_data_KOBO(df_datatotal,df_historico,ruta_salida) #archivo de salida "documento_drive.csv", "documento_drive.xlsx" y "Fueraderango.xlsx"

#Reordena las columnas los mas parecido posible al documento de "INGRESO DATOS DIARIO -PRELIMINAR-", tambien genera una version XLSX de "docuemnto_drive" 
#Ademas filtra el docuemnto de  "Fueraderango.xlsx", para que muestre solamente los fuera de rango del dia en curso (dia actual)
orden.orden_columnas(ruta_salida,ruta_contenedora) #archivo de salida "documento_drive.csv", "documento_drive.xlsx" y "Fueraderango.xlsx"

#Control de reportes y de rengos generales de variables

reporte.control_entrega_reporte(des_control_hora,des_EstacionesConvencionales)
reporte.control_variables(des_control_hora)


#Sube los los diferentes documentos a DRIVE de manera automatica
rou.carga_automatica()

coko2.fenomeno_tratamiento(des_total,des_datadiaria_formato,des_EstacionesConvencionales,ruta_salida)
coko2.modificacion_datos()
coko2.salida_base_datos(des_total,des_info_feno,ruta_salida)


