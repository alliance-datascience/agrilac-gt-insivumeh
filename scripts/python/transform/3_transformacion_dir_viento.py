
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
import rosa



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



def extraccion_viento(df_nombre1,df_nombre2,df_nombre3,df_final_nombre,ruta_salida):
    df=pd.read_csv(df_nombre1)
    df_final=pd.read_csv(df_final_nombre)
    df['fecha']=pd.to_datetime(df['fecha']).dt.strftime('%d/%m/%Y')
    uniestacion=df['estacion'].unique()
    #print(uniestacion)
    unifecha=df['fecha'].unique()
    for i in uniestacion:
        filtro1=df['estacion']==i
        df_fil1=df[filtro1]
        unifecha=df_fil1['fecha'].unique()
        for j in unifecha:
            filtro2=df_fil1['fecha']==j
            df_fil2=df_fil1[filtro2]
            if (len(df_fil2['dir_viento'])==3):
                var1=df_fil2['dir_viento'].values[0]
                var2=df_fil2['dir_viento'].values[1]
                var3=df_fil2['dir_viento'].values[2]

                ang,ang_dis =rosa.get_degree_value(var1,var2,var3)
                filtro3=df_final['estacion']==i
                df_final2=df_final[filtro3]

                filtro4=df_final2['fecha']==j
                df_final3=df_final2[filtro4]

                indice=(df_final3.index)
                df_final.loc[indice,'dir_viento']=ang_dis


    df1=pd.read_csv(df_nombre2)
    df1['fecha']=pd.to_datetime(df1['fecha']).dt.strftime('%d/%m/%Y')
    uniestacion=df1['estacion'].unique()
    #print(uniestacion)
    unifecha=df1['fecha'].unique()
    for i in uniestacion:
        filtro1=df1['estacion']==i
        df_fil1=df1[filtro1]
        unifecha=df_fil1['fecha'].unique()
        for j in unifecha:
            filtro2=df_fil1['fecha']==j
            df_fil2=df_fil1[filtro2]
            if (len(df_fil2['dir_viento'])==3):
                var1=df_fil2['dir_viento'].values[0]
                var2=df_fil2['dir_viento'].values[1]
                var3=df_fil2['dir_viento'].values[2]

                ang,ang_dis =rosa.get_degree_value(var1,var2,var3)
                filtro3=df_final['estacion']==i
                df_final2=df_final[filtro3]

                filtro4=df_final2['fecha']==j
                df_final3=df_final2[filtro4]

                indice=(df_final3.index)
                df_final.loc[indice,'dir_viento']=ang_dis



    df2=pd.read_csv(df_nombre3)
    df2['fecha']=pd.to_datetime(df2['fecha']).dt.strftime('%d/%m/%Y')
    uniestacion=df2['estacion'].unique()
    #print(uniestacion)
    unifecha=df2['fecha'].unique()
    for i in uniestacion:
        filtro1=df2['estacion']==i
        df_fil1=df2[filtro1]
        unifecha=df_fil1['fecha'].unique()
        for j in unifecha:
            filtro2=df_fil1['fecha']==j
            df_fil2=df_fil1[filtro2]
            if (len(df_fil2['dir_viento'])==3):
                var1=df_fil2['dir_viento'].values[0]
                var2=df_fil2['dir_viento'].values[1]
                var3=df_fil2['dir_viento'].values[2]

                ang,ang_dis =rosa.get_degree_value(var1,var2,var3)
                filtro3=df_final['estacion']==i
                df_final2=df_final[filtro3]

                filtro4=df_final2['fecha']==j
                df_final3=df_final2[filtro4]

                indice=(df_final3.index)
                df_final.loc[indice,'dir_viento']=ang_dis




       
    df_final.to_csv(ruta_salida+"data_total.csv",index=False)


extraccion_viento(des_auto,des_sinop,des_conven,des_total,ruta_salida)
