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




def salida_data_general(documento_coordenadas,docuemnto_salida,ruta_cont):
    df_data=pd.read_csv(documento_coordenadas)
    df_data_nueva=pd.read_csv(docuemnto_salida,index_col=False)


    #carga_nombre_estacion=orden_id_des(df_data_nueva)
    #df_data_nueva.insert(3,'Nombre',carga_nombre_estacion)


    df_data_nueva["Longitud"]=np.nan
    df_data_nueva["Latitud"]=np.nan
    df_data_nueva["Altitud"]=np.nan



    for i in df_data['Código']:
        filtro1=df_data["Código"]==i
        coordenadas=df_data[filtro1]
        df_data_nueva.loc[df_data_nueva['estacion']==i,'Longitud']=coordenadas['Longitud'].values[0]
        df_data_nueva.loc[df_data_nueva['estacion']==i,'Latitud']=coordenadas['Latitud'].values[0]
        df_data_nueva.loc[df_data_nueva['estacion']==i,'Altitud']=coordenadas['Altitud'].values[0]

    df_data_nueva.drop(columns=['id'],inplace=True)
    df_data_nueva.to_csv(ruta_cont+"data_procesada.csv",index=False)


salida_data_general(documento_coordenadas,des_total,ruta_contenedora)#archivo de salida "data_procesada.csv"