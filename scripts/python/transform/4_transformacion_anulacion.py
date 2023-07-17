
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



def anulacion_data(name_sinop,name_con,name_auto,name_total,name_anulacion):

     #print("hola_mundo")

    df_sinop=pd.read_csv(name_sinop,index_col=False)
    df_con=pd.read_csv(name_con,index_col=False)
    df_auto=pd.read_csv(name_auto,index_col=False)
    df_total=pd.read_csv(name_total,index_col=False)
    df_anulacion=pd.read_csv(name_anulacion,delimiter=';',index_col=False)
    #carga_nombre_estacion=orden_id_des(df_total)
    #df_total.insert(3,'Nombre',carga_nombre_estacion)
    fecha_anulacion=pd.to_datetime(df_anulacion['fecha']).dt.strftime('%d/%m/%Y')

    

    
    for i in range(len(df_anulacion["Tipo_de_Estacion"])):
        if df_anulacion['Tipo_de_Estacion'][i] == "esta_conven":
  
            con_esta_anulacion=df_anulacion['estacion_conven'].str.upper()

            filtro1 = df_con['Nombre'] == con_esta_anulacion[i]
            df1_con=df_con[filtro1]

        
            filtro2 = df1_con['fecha'] == df_anulacion['fecha'][i]
            df2_con = df1_con[filtro2]

   
            filtro3 = df2_con['hora'] == df_anulacion['Hora'][i]
            df3_con = df2_con[filtro3]

            #print(df3_con.head())
            filtro4=(df3_con.index)
            df_con.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan


            filtro1_total=df_total['Nombre'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]#.dt.strftime('%d/%m/%Y')
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan

            print("Estacion_convencionales_anulacion "+con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i])

        if df_anulacion['Tipo_de_Estacion'][i] == "esta_auto":
  
            con_esta_anulacion=df_anulacion['estacion_auto'].str.upper()

            filtro1 = df_auto['Nombre'] == con_esta_anulacion[i]
            df1_auto=df_auto[filtro1]

            filtro2 = df1_auto['fecha'] == df_anulacion['fecha'][i]
            df2_auto = df1_auto[filtro2]

            filtro3 = df2_auto['hora'] == df_anulacion['Hora'][i]
            df3_auto = df2_auto[filtro3]

            filtro4=(df3_auto.index)
            df_auto.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan



            filtro1_total=df_total['Nombre'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]#.dt.strftime('%d/%m/%Y')
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan

            print("Estacion_automaticas_anulacion " + con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i])

        if df_anulacion['Tipo_de_Estacion'][i] == "esta_sinop":
  
            con_esta_anulacion=df_anulacion['estacion_sinop'].str.upper()

            filtro1 = df_sinop['Nombre'] == con_esta_anulacion[i]
            df1_sinop=df_sinop[filtro1]

            filtro2 = df1_sinop['fecha'] == df_anulacion['fecha'][i]
            df2_sinop = df1_sinop[filtro2]

            filtro3 = df2_sinop['hora'] == df_anulacion['Hora'][i]
            df3_sinop = df2_sinop[filtro3]

            filtro4=(df3_sinop.index)
            df_sinop.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan



            filtro1_total=df_total['Nombre'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]#.dt.strftime('%d/%m/%Y')
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan
            print("Estacion_sinopticas_anulacion " + con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i] )
     

    df_con.to_csv(name_con,index=False)
    df_auto.to_csv(name_auto,index=False)
    df_sinop.to_csv(name_sinop,index=False)
    df_total.to_csv(name_total,index=False)

anulacion_data(des_sinop,des_conven,des_auto,des_total,name_anulacion)
