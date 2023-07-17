
import json, os
import datetime as dt
import pandas as pd 
import string as str
import numpy as np
import viento as viento
import codigo_kobo_c2 as coko2
import ordenar_columnas as orden
import routine as rou
import control_hora as control
import lluvias_parciales as parcial
#Rutas de las carpetas necesarias para este programa
#Ruta contenedora de este programa y archivos necesarios para las comparaciones y analisis 
ruta_contenedora='/home/joshc/automatizacion_kobo/'

#Ruta de salida para todos los archivos creados durante los procesos.
ruta_salida="/home/joshc/prueba_salida/"

#Nombre de los archivos crudos que se descargan de KOBO
name_sinop= ruta_salida +'Formulario_estaciones_sinopticas.csv'
name_con=ruta_salida+'Formulario_estaciones_climaticas_convencionales.csv'
name_auto=ruta_salida+'Formulario_estaciones_automaticas.csv'
name_anulacion=ruta_salida+"Formulario_anulacion_datos_climaticos.csv"
name_lluvia_parcial=ruta_salida+"Formulario_lluvias_parciales.csv"

#Nombre de los archivos limpios descargados de KOBO 
des_conven=ruta_salida+'data_con.csv'
des_sinop=ruta_salida +'data_sinop.csv'
des_auto=ruta_salida+'data_auto.csv'
des_total=ruta_salida+'data_total.csv'
documento_coordenadas=ruta_contenedora+"Instrumentación_Climatología - Intrumentación.csv" 
#des_total=ruta_contenedora+'data_total.csv'

def correccion_fecha_lluvia(df_total):

    df_data=pd.read_csv(df_total, header = 0)

    df_lluvia = df_data[['estacion', 'Unnamed: 0','lluvia','fecha','Nombre']]
    df_lluvia['fecha'] = pd.to_datetime(df_lluvia['fecha'],format='%d/%m/%Y')
    df_lluvia["fecha"] = df_lluvia["fecha"] - dt.timedelta(days=1)

    df_lluvia_melt = pd.melt(df_lluvia, id_vars = ["fecha", "estacion",'Unnamed: 0','Nombre'], value_vars = ['lluvia'])

    df_data = df_data.drop(columns = ["lluvia"])
    df_data['fecha'] = pd.to_datetime(df_data['fecha'],format='%d/%m/%Y')

    df_data_melt = pd.melt(df_data, id_vars = [ "fecha", "estacion",'Unnamed: 0','Nombre'], value_vars = ['tmin',
               'tseca',
               'tmax',
               'eva_tan',
               'eva_piche',
               'hum_rel',
               'bri_solar',
               'nub',
               'vel_viento',
               'dir_viento',
               'pre_atmos',
               #'tsuelo_5',
               'tsuelo_100',
               'rad_solar'])
    
    df_total_nueva = df_data_melt.append(df_lluvia_melt)

    df_valida_var_pivot = df_total_nueva
    df_valida_var_pivot = df_valida_var_pivot.pivot_table(index = ["fecha", "estacion",'Unnamed: 0','Nombre'], columns = 'variable', aggfunc = 'first')['value']

    df_valida_var_pivot = df_valida_var_pivot.reset_index()
    df_valida_var_pivot['fecha']=pd.to_datetime(df_valida_var_pivot['fecha']).dt.strftime('%d/%m/%Y')
    df_valida_var_pivot.head()
    df_valida_var_pivot.to_csv(df_total)

correccion_fecha_lluvia(des_total)
