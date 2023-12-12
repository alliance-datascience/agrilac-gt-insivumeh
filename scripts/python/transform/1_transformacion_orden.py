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


ruta_contenedora=

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


ID={ }


# Funcion devuelve una columna con los nombres de las estaciones remplazando los ID, con el indice "Nombre"  para analisis posteriores y para el archivo fuera de rango
# Para que funcion "orden_id(df)" df debe de ser un dataframe y debe tener una columna con nombre de indice "Estacion"
def orden_id(df):
    #Se asigna y se le da vuelta al diccionario ID para poder guardarlo en d_swap
    d=ID
    d_swap = {v: k for k, v in d.items()}
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario
    con_esta=df['Estacion'].str.upper()
    #Este ciclo compara los valores de la columna "Estacion" con los valores del diccionario d_saw 
    # y si no encuentra algun valor en el diccionario mostrara en pantalla "Fallo para " y el valor que hace falta en el diccionario
    for i in range(len(con_esta)):
        try: con_esta[i]=d_swap[con_esta[i]]
        except: hola=''#print("Fallo para ", con_esta[i])
    #Renombra el indice de la columna del dataframe de "Estacion" a "Nombre"
    con_esta.rename({'Nombre':'Estacion'},inplace=True)
    return con_esta

def orden_id_des(df):
    #Se asigna y se le da vuelta al diccionario ID para poder guardarlo en d_swap
    d=ID
    d_swap = {v: k for k, v in d.items()}
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario
    con_esta=df['estacion'].str.upper()
    #Este ciclo compara los valores de la columna "Estacion" con los valores del diccionario d_saw 
    # y si no encuentra algun valor en el diccionario mostrara en pantalla "Fallo para " y el valor que hace falta en el diccionario
    for i in range(len(con_esta)):
        try: con_esta[i]=d_swap[con_esta[i]]
        except: print("Fallo para ", con_esta[i])
    #Renombra el indice de la columna del dataframe de "Estacion" a "Nombre"
    con_esta.rename({'Nombre':'estacion'},inplace=True)
    return con_esta


"""Las funciones 'data' son las que limpian los archivos crudos que se descargaron con la funcion 'descarga_datos()'""" 
#Funcion "data_sinop(name_sinop)" es la que limpia el archivos con el nombre "Formulario_estaciones_sinopticas.csv"

def data_sinop(name_sinop,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_sinopticas.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_sinop,delimiter=';',usecols=['start','end','Fecha' , 'estacion', 'Hora','lluvia','tmin'
    ,'tseca', 'tmax','eva_tan','hum_rel','rad_solar','bri_solar', 'nub', 'vel_viento','dir_viento', 'pre_atmos','tsuelo_100','Fenomenos',"_submission_time",'deviceid','_id','_submitted_by'])
    con_esta=df['estacion'].str.upper()

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: i=1
    df['estacion']=con_esta

    df['eva_tan'].replace(-1,np.nan,inplace=True)

    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"],'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"], 'hora':df["Hora"],'lluvia':df["lluvia"],'tmin':df["tmin"]\
    , 'tseca':df["tseca"], 'tmax':df["tmax"], 'eva_tan':df["eva_tan"],  'hum_rel':df["hum_rel"],  'rad_solar':df["rad_solar"] \
    ,'bri_solar':df["bri_solar"], 'nub':df["nub"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"], 'pre_atmos':df["pre_atmos"]\
    ,'tsuelo_100':df["tsuelo_100"],'fenomenos':df["Fenomenos"],'submission_time':df["_submission_time"],'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"]} 

    #se crea el dataframe y se crea el siguiente archivo con este "data_sinop.csv"
    da=pd.DataFrame(nom)
    name_fil = ruta_salida+'data_sinop.csv'
    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)
    da.to_csv(name_fil,index=False)
    return da
    
#Funcion "data_conven(name_con)" es la que limpia el archivos con el nombre "Formulario_estaciones_climaticas_convencionales.csv"
def data_conven(name_con,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_climaticas_convencionales.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_con,delimiter=';',usecols=['start', 'end','Fecha' ,'estacion','Hora','lluvia', 'tmin','tseca'
    ,'tmax', 'eva_tan','eva_piche', 'hum_rel','bri_solar', 'nub', 'vel_viento','dir_viento', 'pre_atmos','tsuelo_5', 'tsuelo_50','tsuelo_100','fenomenos',"_submission_time",'deviceid','_id','_submitted_by'])
    con_esta=df['estacion'].str.upper()

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])

    df['eva_tan'].replace(-1,np.nan,inplace=True)
    df['eva_piche'].replace(-1,np.nan,inplace=True)

    df['estacion']=con_esta

    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"], 'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"],'hora':df["Hora"],'lluvia':df["lluvia"], 'tmin':df["tmin"],'tseca':df["tseca"]\
    ,'tmax':df["tmax"], 'eva_tan':df["eva_tan"],'eva_piche':df["eva_piche"], 'hum_rel':df["hum_rel"],'bri_solar':df["bri_solar"]\
    , 'nub':df["nub"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"], 'pre_atmos':df["pre_atmos"]\
    ,'tsuelo_5':df["tsuelo_5"], 'tsuelo_50':df["tsuelo_50"],'tsuelo_100':df["tsuelo_100"],'fenomenos':df["fenomenos"],'submission_time':df["_submission_time"],'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"]} 

    #se crea el dataframe y se crea el siguiente archivo con este "data_con.csv"
    da=pd.DataFrame(nom)
    name_fil =ruta_salida + 'data_con.csv'
    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)
    da.to_csv(name_fil,index=False)
    return da
      
#Funcion "data_auto(name_auto)" es la que limpia el archivos con el nombre "Formulario_estaciones_automaticas.csv"
def data_auto(name_auto,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_automaticas.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_auto,delimiter=';',usecols=["nub",'start','end','Fecha','estacion',"Hora","lluvia","lluvia_total","vel_viento","dir_viento", "eva_tan","Fenomenos","_submission_time",'deviceid','_id','_submitted_by'],index_col=False)
    con_esta=df['estacion'].str.upper()
    
    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: pass#print("Fallo para ", con_esta[i])
    df['estacion']=con_esta


    #df['lluvia'].replace(np.nan,0)
    #df['lluvia_total'].replace(np.nan,0)

    df['Lluvia']=df['lluvia']+df['lluvia_total']


    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"], 'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"],'hora':df["Hora"],'lluvia':df["lluvia"],\
         'nub':df["nub"],'eva_tan':df["eva_tan"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"],'fenomenos':df["Fenomenos"],'submission_time':df["_submission_time"]
         ,'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"]} 

    
    #se crea el dataframe y se crea el siguiente archivo con este "data_auto.csv"
    da=pd.DataFrame(nom)


    name_fil =ruta_salida+ 'data_auto.csv'
    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)
    da.to_csv(name_fil,index=False)
    return da




data_auto(name_auto,ruta_salida)
data_conven(name_con,ruta_salida)
data_sinop(name_sinop,ruta_salida)
