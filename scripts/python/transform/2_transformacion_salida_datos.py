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



ID={"RETALHULEU_AEROPUERTO":"INS110101CV","CHAMPERICO_FEGUA":"INS110701CV","COBAN":"INS160101CV","ESQUIPULAS":"INS200701CV","FLORES_AEROPUERTO":"INS170101CV",\
    "LA_AURORA":"INS010102CV","LA_FRAGUA":"INS190201CV","LOS_ALTOS":"INS090101CV","MONTUFAR":"INS221401CV",\
    "PUERTO_BARRIOS_PHC":"INS180101CV","RETALHULEU_AEROPUERTO":"INS110101CV","HUEHUETENANGO":"INS130101CV",\
    "POPTUN":"INS171201CV","SAN_JOSE_AEROPUERTO":"INS050901CV","TECUN_UMAN":"INS121701CV",\
    "RETALHULEU_AREOPUERTO":"RETALHULEU_AEROPUERTO","SAN_JOSE_AREOPUERTO":"INS050901CV",
    "CAMOTAN":"INS200501CV","CATARINA":"INS121601CV","EL_CAPITAN":"INS071301CV","LA_CEIBITA":"INS210601CV",\
    "LABOR_OVALLE":"INS090301CV","MAZATENANGO":"INS100101CV","PANZÓS_PHC_ALTAVERAPAZ":"INS160701CV",\
    "SACAPULAS":"INS141601CV","SAN_MARCOS_PHC":"INS120101CV","SANTA_CRUZ_BALANYA":"INS041001CV",\
    "TODOS_SANTOS":"INS131501CV","INSIVUMEH":"INS010101CV","ALAMEDA_ICTA":"INS040101CV",\
    "ASUNCION_MITA":"INS220501CV","CHINIQUE":"INS-140301","EL_TABLON":"INS070101CV","LA_UNION":"INS190901CV",\
    "LAS_VEGAS_PHC":"INS180201CV","NEBAJ":"INS141301CV","QUEZADA":"INS221701CV","SABANA_GRANDE":"INS050101CV",\
    "SAN_AGUSTIN_ACASAGUASTLAN":"INS020302CV","SAN_JERONIMO":"INS150701CV","SAN_PEDRO_NECTA":"INS130601CV",\
    "SANTA_MARIA_CAHABON":"INS161201CV","SANTIAGO_ATITLAN":"INS071901CV","SUIZA_CONTENTA":"INS030801CV",\
    "CHIXOY_PCH":"INS141901CV","CUBULCO":"INS150401CV","LOS_ALBORES":"INS020301CV","LOS_ESCLAVOS":"INS060101CV",\
    "PASABIEN":"INS190301CV","POTRERO_CARRILLO":"INS210101CV","SAN_MARTIN_JILOTEPEQUE":"INS040301CV","PANZOS_PHC_ALTA_VERAPAZ":"INS160701CV","SAN_JERONIMO_R_H":"INS150701CV",\
    "CAMOTAN":"INS200501CV", "CAMOTAN":"INS200501CV","CATARINA":"INS121601CV","EL_CAPITAN":"INS071301CV","LA_CEIBITA":"INS210601CV",\
    "LABOR_OVALLE":"INS090301CV","MAZATENANGO":"INS100101CV","PANZÓS_PHC_ALTAVERAPAZ":"INS160701CV",\
    "SACAPULAS":"INS141601CV","SAN_MARCOS_PHC":"INS120101CV","SANTA_CRUZ_BALANYA":"INS041001CV",\
    "TODOS_SANTOS":"INS131501CV","INSIVUMEH":"INS010101CV","ALAMEDA_ICTA":"INS040101CV",\
    "ASUNCION_MITA":"INS220501CV","CHINIQUE":"INS-140301","EL_TABLON":"INS070101CV","LA_UNION":"INS190901CV",\
    "LAS_VEGAS_PHC":"INS180201CV","NEBAJ":"INS141301CV","QUEZADA":"INS221701CV","SABANA_GRANDE":"INS050101CV",\
    "SAN_AGUSTIN_ACASAGUASTLAN":"INS020302CV","SAN_JERONIMO":"INS150701CV","SAN_PEDRO_NECTA":"INS130601CV",\
    "SANTA_MARIA_CAHABON":"INS161201CV","SANTIAGO_ATITLAN":"INS071901CV","SUIZA_CONTENTA":"INS030801CV",\
    "CHIXOY_PCH":"INS141901CV","CUBULCO":"INS150401CV","LOS_ALBORES":"INS020301CV","LOS_ESCLAVOS":"INS060101CV",\
    "PASABIEN":"INS190301CV","POTRERO_CARRILLO":"INS210101CV","SAN_MARTIN_JILOTEPEQUE":"INS040301CV","PANZOS_PHC_ALTA_VERAPAZ":"INS160701CV","SAN_JERONIMO_R_H":"INS150701CV",\
    "AMATITLAN":"INS011401AT","ANTIGUA_GUATEMALA":"INS030101AT","CONCEPCION":"INS050101AT","IXCHIGUAN":"INS122301AT","JALAPA":"INS210101AT","LA_REFORMA":"INS122101AT",\
    "LAS_NUBES":"INS-010331","LO_DE_COY":"INS010801AT","MARISCOS":"INS180501AT","MORALES_MET":"INS180401AT","NENTON":"INS-130527","NUEVA_CONCEPCION":"PLUVIOMÉT",\
    "PACHUTE":"INS090401AT","PLAYA_GRANDE_IXCAN":"INS142201AT","SAN_JOSE_PINULA":"INS010301AT","SAN_PEDRO_AYAMPUC":"INS010701AT","SANTA_CRUZ_DEL_QUICHE":"INS140101AT",\
    "SANTA_MARGARITA":"INS040801AT","TOTONICAPAN":"INS080101AT","CHINIQUE":"INS-140301","SANTA_CRUZ_DEL_QUICHE":"INS140101AT","RETALHULEU_AEROPUERTO":"INS110101CV"
    }



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


"""La funcion salida diaria toma los datos enviados en los tres horarios por los observadores y realiza todos los promedios necesarios en las 
variables que sean necesarias y toma los valores y los ordena a demas de aproximar a la cantidad de decimales que se necesaria"""
def salida_diaria(data_sinop,data_conven,data_auto,ruta_salida):
    #se carga el dataframe "Formulario_estaciones_sinopticas.csv" en df1, "Formulario_estaciones_climaticas_convencionales.csv" en df2 y "Formulario_estaciones_automaticas.csv" en df3
    df1=data_sinop
    df2=data_conven
    df3=data_auto

    #Realiza un merge sobre todos los dataframe definidos anteriormente
    merged_left=pd.merge(df2,df1,how='outer')
    merged_left=pd.merge(merged_left,df3,how='outer' )
    
    #cambia de formato la columna 'start' a un nuevo formato de hora
    merged_left['fecha']=pd.to_datetime(merged_left['fecha']).dt.strftime('%d/%m/%Y')
    #se filtran los datos a traves de un groupby tanto por estacion como por hora de inicio de formulario

    merged_left.to_csv(ruta_salida+"data_prueba_salida.csv",index=True)

    df_prueba_prom=merged_left.groupby(['fecha','estacion']).mean()
    df_prueba_prom_lluvia=merged_left.groupby(['fecha','estacion']).sum()
    df_prueba_prom_lluvia.to_csv(ruta_salida+"Holamundo.csv")
    #Se se toman las columnas de los dataframe y se aproximan a la cantidad de desimales que sean necesarias
    df_prueba_prom["lluvia"]=round(df_prueba_prom_lluvia["lluvia"],1)
    df_prueba_prom["tmin"]=round(df_prueba_prom["tmin"],1)
    df_prueba_prom["tseca"]=round(df_prueba_prom["tseca"],1)
    df_prueba_prom["tmax"]=round(df_prueba_prom["tmax"],1)
    df_prueba_prom["eva_tan"]=round(df_prueba_prom["eva_tan"],1)
    df_prueba_prom["eva_piche"]=round(df_prueba_prom["eva_piche"],1)
    df_prueba_prom["hum_rel"]=round(df_prueba_prom["hum_rel"],0)
    df_prueba_prom["rad_solar"]=round(df_prueba_prom["rad_solar"],1)
    df_prueba_prom["bri_solar"]=round(df_prueba_prom["bri_solar"],1)
    df_prueba_prom["nub"]=round(df_prueba_prom["nub"],0)
    df_prueba_prom["vel_viento"]=round(df_prueba_prom["vel_viento"],1)
    df_prueba_prom["pre_atmos"]=round(df_prueba_prom["pre_atmos"],1)
    df_prueba_prom["tsuelo_100"]=round(df_prueba_prom["tsuelo_100"],1)
    df_prueba_prom["tsuelo_5"]=round(df_prueba_prom["tsuelo_5"],1)
    df_prueba_prom["tsuelo_50"]=round(df_prueba_prom["tsuelo_50"],1)

    
    #Se crea el archivo "data_total.csv" el cual contiene todos los datos diarios para verificar con los datos el la funcion validacion_rangos_data_KOBO(df_datatotal,df_historicos)
    df_prueba_prom=df_prueba_prom.reset_index()
    carga_nombre_estacion=orden_id_des(df_prueba_prom)
    df_prueba_prom.insert(3,'Nombre',carga_nombre_estacion)

    df_prueba_prom.to_csv(ruta_salida+"data_total.csv",index=True)



df_sinop=pd.read_csv(des_sinop)
df_conven=pd.read_csv(des_conven)
df_auto=pd.read_csv(des_auto)

salida_diaria(df_auto,df_conven,df_sinop,ruta_salida)