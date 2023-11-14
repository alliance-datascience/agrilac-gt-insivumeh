import json, os
import datetime as dt
import pandas as pd 
import string as str
import numpy as np
import viento as viento
import mysql.connector
import pymysql
from sqlalchemy import create_engine



"""
#Rutas de las carpetas necesarias para este programa
#Ruta contenedora de este programa y archivos necesarios para las comparaciones y analisis 
ruta_contenedora='/home/joshc/automatizacion_kobo_v3/'

#Ruta de salida para todos los archivos creados durante los procesos.
ruta_salida="/home/joshc/salida_auto_kobo_v3/"


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
des_datadiaria_formato=ruta_salida+'df_datadiaria_formato.csv'
des_EstacionesConvencionales=ruta_contenedora+'EstacionesConvencionales.xlsx'
des_info_feno=ruta_salida+'info_feno.csv'
"""

#Token de Autorizacion generado en KOBOTOOLBOX y es unico para cada una de las cuantes creadas en KOBOTOOLBOX
#token="a2d815357d35aa5773ba65f1c5537b267eb1c9eb"
token='e6f030307158e83c473c76789f11159a4c945e50'


#Diccionario que remplaza todos los nombres de las estaciones por los codigos de las mismas 
"""ID={"RETALHULEU_AEROPUERTO":"INS110101CV","CHAMPERICO_FEGUA":"INS110701CV","COBAN":"INS160101CV","ESQUIPULAS":"INS200701CV","FLORES_AEROPUERTO":"INS170101CV",\
    "LA_AURORA":"INS010102CV","LA_FRAGUA":"INS190201CV","LOS_ALTOS":"INS090101CV","MONTUFAR":"INS221401CV",\
    "PUERTO_BARRIOS_PHC":"INS180101CV","RETALHULEU_AEROPUERTO":"INS110101CV","HUEHUETENANGO":"INS130101CV",\
    "POPTUN":"INS171201CV","SAN_JOSE_AEROPUERTO":"INS050901CV","TECUN_UMAN":"INS121701CV",\
    "RETALHULEU_AREOPUERTO":"RETALHULEU_AEROPUERTO","SAN_JOSE_AREOPUERTO":"INS050901CV",
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
    "LAS_NUBES":"INS010331CV","LO_DE_COY":"INS010801AT","MARISCOS":"INS180501AT","MORALES_MET":"INS180401AT","NENTON":"INS-130527","NUEVA_CONCEPCION":"PLUVIOMÉT",\
    "PACHUTE":"INS090401AT","PLAYA_GRANDE_IXCAN":"INS142201AT","SAN_JOSE_PINULA":"INS010301AT","SAN_PEDRO_AYAMPUC":"INS010701AT","SANTA_CRUZ_DEL_QUICHE":"INS140101AT",\
    "SANTA_MARGARITA":"INS040801AT","TOTONICAPAN":"INS080101AT","CHINIQUE":"INS140301CV","SANTA_CRUZ_DEL_QUICHE":"INS140101AT","RETALHULEU_AEROPUERTO":"INS110101CV"
    }
"""
#Diccionario que remplaza todos los nombres de las estaciones por los codigos de las mismas 
ID={"RETALHULEU_AEROPUERTO":"INS110101CV","CHAMPERICO_FEGUA":"INS110701CV","COBAN":"INS160101CV","ESQUIPULAS":"INS200701CV","FLORES_AEROPUERTO":"INS170101CV",\
 "LA_AURORA":"INS010102CV","LA_FRAGUA":"INS190201CV","LOS_ALTOS":"INS090101CV","MONTUFAR":"INS221401CV",\
 "PUERTO_BARRIOS_PHC":"INS180101CV","RETALHULEU_AEROPUERTO":"INS110101CV","HUEHUETENANGO":"INS130101CV",\
 "POPTUN":"INS171201CV","SAN_JOSE_AEROPUERTO":"INS050901CV","TECUN_UMAN":"INS121701CV",\
 "RETALHULEU_AREOPUERTO":"RETALHULEU_AEROPUERTO","SAN_JOSE_AREOPUERTO":"INS050901CV",
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

nombre_variables={"lluvia":"PRECIPTACIÓN","hum_rel":"HUMEDAD_RELATIVA","tmin":"TEMPERATURA_MÍNIMA","tmax":"TEMPERATURA_MÁXIMA",
                  "tseca":"TEMPERATURA_MEDIA","rad_solar":"RADIACIÓN","bri_solar":"BRILLO_SOLAR","eva_tan":"EVAPORACIÓN_TANQUE",
                  "pre_atmos":"PRESIÓN_ATMOSFÉRICA","eva_piche":"EVAPORACIÓN_PICHE","vel_viento":"VELOCIDAD_VIENTO",
                  "dir_viento":"DIRECCIÓN_VIENTO","nub":"NUBOSIDAD","tsuelo_100":"TEMPERATURA_SUELO_100"}


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


#Funcion que descarga los archivos crudos desde KOBO y los renombra con el nombre de su respectivo formulario
#Es necesario tener en la misma carpera un archivo .json con el nombre de "apidic.json"(no importa si el archivo esta vacio o no)
def descarga_datos(ruta_salida,ruta):
    #Se le dan las credenciales y el token del propitario del formulario de KOBO
    
    token='e6f030307158e83c473c76789f11159a4c945e50'
    #os.system('curl -X GET https://kobo.humanitarianresponse.info/api/v2/assets.json -H "Authorization: Token e6f030307158e83c473c76789f11159a4c945e50" >'+ ruta+'apiDic.json')
    #os.system('curl -X GET https://kobo.humanitarianresponse.info/api/v2/assets.json -H "Authorization: Token '+token+'" >'+ ruta+'apiDic.json')
    os.system('curl -X GET https://eu.kobotoolbox.org/api/v2/assets.json -H "Authorization: Token e6f030307158e83c473c76789f11159a4c945e50" > '+ ruta+'apiDic.json')
    #Se almacena todo el contenido de la API de KOBO dentro del archivo "apiDic.json"
    dicjson = open(ruta+'apiDic.json')
    apiDic = json.load(dicjson)
    dicjson.close()

    #Ciclo que navega a traves de la API y se dirije a las rutas donde se almacenan los nombres de los formularios y los datos recolectados por los formularios
    # El rango del ciclo va a depender de la cantidad de formmulario que se tenga para descargar
    # El orden que se descargan los formularios es el orden en el que se les implemento por ultima vez dentro de kobo

    for i in range(0,5):
        #Ruta dentro de la API que contiene los nombres de los formularios 
        nom_formulario=apiDic['results'][i]['name']
        #Ruta dentro de la API que contiene los Links de descarga de los formularios
        link = apiDic['results'][i]['export_settings'][0]['data_url_csv']
       
        #Se les da permisos para podes descargar los formularios a traves de la ruta obtenida anteriormente
        #os.system('wget -P '+ruta+ ' -d --header="Authorization: Token a2d815357d35aa5773ba65f1c5537b267eb1c9eb" '+link)
        os.system('wget -P '+ruta+ ' -d --header="Authorization: Token '+token+'" '+link)
        #Se renombran los archivos que se deescargan a los nombres de su respectivo formulario
        os.rename(ruta+'data.csv',ruta_salida+nom_formulario+'.csv' )
        #print('\n \n'+'\n'+link+'\n \n \n')


def anulacion_data(des_sinop,des_conven,des_auto,des_total,name_anulacion):

    #documentos des cargados y sin columnas basura
    df_sinop=pd.read_csv(des_sinop,index_col=False)
    df_con=pd.read_csv(des_conven,index_col=False)
    df_auto=pd.read_csv(des_auto,index_col=False)
    df_total=pd.read_csv(des_total,index_col=False)

    #carga de docuemntos crudos
    df_anulacion=pd.read_csv(name_anulacion,delimiter=';',index_col=False)
    df_anulacion['estacion_id']=df_anulacion['estacion_auto'].replace(np.nan,'')+df_anulacion['estacion_conven'].replace(np.nan,'')+df_anulacion['estacion_sinop'].replace(np.nan,'')
    con_esta=df_anulacion['estacion_id'].str.upper()
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])
    df_anulacion['estacion_id']=con_esta

    #carga de documentos de anulacion de datos 
    fecha_anulacion=pd.to_datetime(df_anulacion['fecha']).dt.strftime('%d/%m/%Y')

    for i in range(len(df_anulacion["Tipo_de_Estacion"])):
        if df_anulacion['Tipo_de_Estacion'][i] == "esta_conven":
  
            con_esta_anulacion=df_anulacion['estacion_id'].str.upper()

            filtro1 = df_con['estacion'] == con_esta_anulacion[i]
            df1_con=df_con[filtro1]
        
            filtro2 = df1_con['fecha'] == df_anulacion['fecha'][i]
            df2_con = df1_con[filtro2]

            filtro3 = df2_con['hora'] == df_anulacion['Hora'][i]
            df3_con = df2_con[filtro3]

            filtro4=(df3_con.index)
            df_con.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan

            filtro1_total=df_total['estacion'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan

            print("Estacion_convencionales_anulacion "+con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i])

        if df_anulacion['Tipo_de_Estacion'][i] == "esta_auto":
  
            con_esta_anulacion=df_anulacion['estacion_id'].str.upper()

            filtro1 = df_auto['estacion'] == con_esta_anulacion[i]
            df1_auto=df_auto[filtro1]

            filtro2 = df1_auto['fecha'] == df_anulacion['fecha'][i]
            df2_auto = df1_auto[filtro2]

            filtro3 = df2_auto['hora'] == df_anulacion['Hora'][i]
            df3_auto = df2_auto[filtro3]

            filtro4=(df3_auto.index)
            df_auto.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan


            filtro1_total=df_total['estacion'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]#.dt.strftime('%d/%m/%Y')
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan

            print("Estacion_automaticas_anulacion " + con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i])

        if df_anulacion['Tipo_de_Estacion'][i] == "esta_sinop":
  
            con_esta_anulacion=df_anulacion['estacion_id'].str.upper()

            filtro1 = df_sinop['estacion'] == con_esta_anulacion[i]
            df1_sinop=df_sinop[filtro1]

            filtro2 = df1_sinop['fecha'] == df_anulacion['fecha'][i]
            df2_sinop = df1_sinop[filtro2]

            filtro3 = df2_sinop['hora'] == df_anulacion['Hora'][i]
            df3_sinop = df2_sinop[filtro3]

            filtro4=(df3_sinop.index)
            df_sinop.loc[filtro4 , df_anulacion["variable_anular"][i] ]= np.nan



            filtro1_total=df_total['estacion'] == con_esta_anulacion[i]
            df1_total=df_total[filtro1_total]

            filtro2_total=df1_total['fecha'] == fecha_anulacion[i]
            df2_total=df1_total[filtro2_total]

            filtro5=(df2_total.index)
            df_total.loc[filtro5,df_anulacion["variable_anular"][i]]=np.nan
            print("Estacion_sinopticas_anulacion " + con_esta_anulacion[i]+ "  "+ df_anulacion['fecha'][i] )
     

    df_con.to_csv(des_conven,index=False)
    df_auto.to_csv(des_auto,index=False)
    df_sinop.to_csv(des_sinop,index=False)
    df_total.to_csv(des_total,index=False)
    
#anulacion_data(des_sinop,des_conven,des_auto,des_total,name_anulacion)

"""Las funciones 'data' son las que limpian los archivos crudos que se descargaron con la funcion 'descarga_datos()'""" 
#Funcion "data_sinop(name_sinop)" es la que limpia el archivos con el nombre "Formulario_estaciones_sinopticas.csv"
def data_sinop(name_sinop,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_sinopticas.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_sinop,delimiter=';',usecols=['start','end','Fecha' , 'estacion', 'Hora','lluvia','tmin'
    ,'tseca', 'tmax','eva_tan','hum_rel','rad_solar','bri_solar', 'nub', 'vel_viento','dir_viento', 'pre_atmos','tsuelo_100','Fenomenos',"_submission_time",'deviceid','_id','_submitted_by'
    ,'Fenomenos/0','Fenomenos/1','Fenomenos/2','Fenomenos/3','Fenomenos/4','Fenomenos/5','Fenomenos/6','Fenomenos/7','Fenomenos/8','Fenomenos/9','Fenomenos/10'],low_memory=False)
    con_esta=df['estacion'].str.upper()

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])
        try: con_esta[i]=ID[con_esta[i]]
        except: i=1
    df['estacion']=con_esta

    df['eva_tan'].replace(-1,np.nan,inplace=True)
    df['bri_solar'].replace(-1,np.nan,inplace=True)
    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"],'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"], 'hora':df["Hora"],'lluvia':df["lluvia"],'tmin':df["tmin"]\
    , 'tseca':df["tseca"], 'tmax':df["tmax"], 'eva_tan':df["eva_tan"],  'hum_rel':df["hum_rel"],  'rad_solar':df["rad_solar"] \
    ,'bri_solar':df["bri_solar"], 'nub':df["nub"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"], 'pre_atmos':df["pre_atmos"]\
    ,'tsuelo_100':df["tsuelo_100"],'submission_time':df["_submission_time"],'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"],'Fenomenos/0':df['Fenomenos/0'],'Fenomenos/1':df['Fenomenos/1'],'Fenomenos/2':df['Fenomenos/2']
    ,'Fenomenos/3':df['Fenomenos/3'],'Fenomenos/4':df['Fenomenos/4'],'Fenomenos/5':df['Fenomenos/5'],'Fenomenos/6':df['Fenomenos/6'],'Fenomenos/7':df['Fenomenos/7']
    ,'Fenomenos/8':df['Fenomenos/8'],'Fenomenos/9':df['Fenomenos/9'],'Fenomenos/10':df['Fenomenos/10']} 

    #se crea el dataframe y se crea el siguiente archivo con este "data_sinop.csv"
    da=pd.DataFrame(nom)
    
    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)

    da.loc[ da['estacion'] == 'INS050901CV'] = np.nan

    name_fil = ruta_salida+'data_sinop.csv'
    da.to_csv(name_fil,index=False)
    return da
    
#Funcion "data_conven(name_con)" es la que limpia el archivos con el nombre "Formulario_estaciones_climaticas_convencionales.csv"
def data_conven(name_con,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_climaticas_convencionales.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_con,delimiter=';',usecols=['start', 'end','Fecha' ,'estacion','Hora','lluvia', 'tmin','tseca'
    ,'tmax', 'eva_tan','eva_piche', 'hum_rel','bri_solar', 'nub', 'vel_viento','dir_viento', 'pre_atmos','tsuelo_5', 'tsuelo_50','tsuelo_100','fenomenos',"_submission_time",'deviceid','_id','_submitted_by'
    ,'fenomenos/0','fenomenos/1','fenomenos/2','fenomenos/3','fenomenos/4','fenomenos/5','fenomenos/6','fenomenos/7','fenomenos/8','fenomenos/9','fenomenos/10'],low_memory=False)
    con_esta=df['estacion'].str.upper()

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])
    df['estacion']=con_esta

    df['eva_tan'].replace(-1,np.nan,inplace=True)
    df['eva_piche'].replace(-1,np.nan,inplace=True)
    df['bri_solar'].replace(-1,np.nan,inplace=True)
    
    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"], 'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"],'hora':df["Hora"],'lluvia':df["lluvia"], 'tmin':df["tmin"],'tseca':df["tseca"]\
    ,'tmax':df["tmax"], 'eva_tan':df["eva_tan"],'eva_piche':df["eva_piche"], 'hum_rel':df["hum_rel"],'bri_solar':df["bri_solar"]\
    , 'nub':df["nub"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"], 'pre_atmos':df["pre_atmos"]\
    ,'tsuelo_5':df["tsuelo_5"], 'tsuelo_50':df["tsuelo_50"],'tsuelo_100':df["tsuelo_100"],'submission_time':df["_submission_time"],'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"]
    ,'Fenomenos/0':df['fenomenos/0'],'Fenomenos/1':df['fenomenos/1'],'Fenomenos/2':df['fenomenos/2']
    ,'Fenomenos/3':df['fenomenos/3'],'Fenomenos/4':df['fenomenos/4'],'Fenomenos/5':df['fenomenos/5'],'Fenomenos/6':df['fenomenos/6'],'Fenomenos/7':df['fenomenos/7']
    ,'Fenomenos/8':df['fenomenos/8'],'Fenomenos/9':df['fenomenos/9'],'Fenomenos/10':df['fenomenos/10']} 

    #se crea el dataframe y se crea el siguiente archivo con este "data_con.csv"
    da=pd.DataFrame(nom)
    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)

    name_fil =ruta_salida + 'data_con.csv'
    da.to_csv(name_fil,index=False)
    return da
      
#Funcion "data_auto(name_auto)" es la que limpia el archivos con el nombre "Formulario_estaciones_automaticas.csv"
def data_auto(name_auto,ruta_salida):
    #se carga en df el dataframe "Formulario_estaciones_automaticas.csv"  
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario 
    df=pd.read_csv(name_auto,delimiter=';',usecols=["nub",'start','end','Fecha','estacion',"Hora","lluvia","lluvia_total","vel_viento","dir_viento", "eva_tan","_submission_time",'deviceid','_id','_submitted_by'
                                                    ,'Fenomenos/0','Fenomenos/1','Fenomenos/2','Fenomenos/3','Fenomenos/4','Fenomenos/5','Fenomenos/6','Fenomenos/7','Fenomenos/8','Fenomenos/9','Fenomenos/10'],low_memory=False)
    con_esta=df['estacion'].str.upper()
    
    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: pass#print("Fallo para ", con_esta[i])
    df['estacion']=con_esta


    df['lluvia'].replace(np.nan,0,inplace=True)
    df['lluvia_total'].replace(np.nan,0,inplace=True)

    df['Lluvia']=df['lluvia']+df['lluvia_total']

    df['Lluvia'] = np.where(((df['Hora']=='13_00')), np.nan ,df['Lluvia'])
    df['Lluvia'] = np.where(((df['Hora']=='18_00')), np.nan ,df['Lluvia'])


    #Se guarda y se renombran las columnas que se necesitan, tomadas del dataframe df
    nom={'start':df["start"], 'end':df['end'],'fecha':df['Fecha'], 'estacion':df["estacion"],'hora':df["Hora"],'lluvia':df["Lluvia"],\
         'nub':df["nub"],'eva_tan':df["eva_tan"], 'vel_viento':df["vel_viento"],'dir_viento':df["dir_viento"],'submission_time':df["_submission_time"]
        ,'deviceid':df["deviceid"],'id':df["_id"],'submitted_by':df["_submitted_by"],'Fenomenos/0':df['Fenomenos/0'],'Fenomenos/1':df['Fenomenos/1'],'Fenomenos/2':df['Fenomenos/2']
        ,'Fenomenos/3':df['Fenomenos/3'],'Fenomenos/4':df['Fenomenos/4'],'Fenomenos/5':df['Fenomenos/5'],'Fenomenos/6':df['Fenomenos/6'],'Fenomenos/7':df['Fenomenos/7']
        ,'Fenomenos/8':df['Fenomenos/8'],'Fenomenos/9':df['Fenomenos/9'],'Fenomenos/10':df['Fenomenos/10']} 

    
    #se crea el dataframe y se crea el siguiente archivo con este "data_auto.csv"
    da=pd.DataFrame(nom)

    carga_nombre_estacion=orden_id_des(da)
    da.insert(4,'Nombre',carga_nombre_estacion)

    name_fil =ruta_salida+ 'data_auto.csv'
    da.to_csv(name_fil,index=False)
    return da

def data_anulacion(name_auto,ruta_salida):
    df=pd.read_csv(name_auto,delimiter=';',index_col=False,parse_dates = ['start'], dayfirst = True)

    
    df['start']=pd.to_datetime(df['start']).dt.strftime('%Y-%m-%d')

    con_esta=df['variable_anular']

    #Ciclo que hace el cambio del nombre de la estacion por su ID
    for i in range(len(con_esta)):
        try: con_esta[i]=ID[con_esta[i]]
        except: print("Fallo para ", con_esta[i])

    df['variable_anular']=con_esta

    #print(df.info())
    df.to_csv(ruta_salida+'anulacion_datos.csv',index=False)
    
    

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

    merged_left.to_csv(ruta_salida+'data_control_hora.csv',index=False)
    
    #cambia de formato la columna 'start' a un nuevo formato de hora
    #merged_left['fecha']=pd.to_datetime(merged_left['fecha']).dt.strftime('%Y-%m-%d')
    #merged_left['fecha'] = pd.to_datetime(merged_left['fecha'],format='%Y-%m-%d')
    #se filtran los datos a traves de un groupby tanto por estacion como por hora de inicio de formulario
    df_prueba_prom=merged_left.groupby(['fecha','estacion']).mean(numeric_only=True)

    #Se se toman las columnas de los dataframe y se aproximan a la cantidad de desimales que sean necesarias
    df_prueba_prom["lluvia"] = round(df_prueba_prom["lluvia"],1)
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
    df_prueba_prom.insert(2,'Nombre',carga_nombre_estacion)

    df_prueba_prom.to_csv(ruta_salida+"data_total.csv",index=False)


def correccion_fecha_lluvia(des_total,ruta_contenedora):
    df_data=pd.read_csv(des_total, header = 0)

    df_lluvia = df_data[['estacion','lluvia','fecha','Nombre']]

    #df_lluvia['fecha']=pd.to_datetime(df_lluvia['fecha']).dt.strftime('%Y-%m-%d')
    df_lluvia['fecha'] = pd.to_datetime(df_lluvia['fecha'],format='%Y-%m-%d')
    df_lluvia["fecha"] = df_lluvia["fecha"] - dt.timedelta(days=1)

    df_lluvia_melt = pd.melt(df_lluvia, id_vars = ["fecha", "estacion",'Nombre'], value_vars = ['lluvia'])

    df_data = df_data.drop(columns = ["lluvia"])
    df_data['fecha'] = pd.to_datetime(df_data['fecha'],format='%Y-%m-%d')

    #print(df_data.info())

    df_data_melt = pd.melt(df_data, id_vars = [ "fecha", "estacion",'Nombre'], value_vars = ['tmin',
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
               'tsuelo_100',
               'rad_solar',
               'tsuelo_5',
               'tsuelo_50',
               'Fenomenos/0',
               'Fenomenos/1',
               'Fenomenos/2',
               'Fenomenos/3',
               'Fenomenos/4',
               'Fenomenos/5',
               'Fenomenos/6',
               'Fenomenos/7',
               'Fenomenos/8',
               'Fenomenos/9',
               'Fenomenos/10'
               ])
    
    

    #df_total_nueva = df_data_melt.append(df_lluvia_melt)
    df_total_nueva = pd.concat([df_data_melt,df_lluvia_melt], ignore_index=True)
    print(df_total_nueva.info())

    df_valida_var_pivot = df_total_nueva
    df_valida_var_pivot = df_valida_var_pivot.pivot_table(index = ["fecha", "estacion",'Nombre'], columns = 'variable', aggfunc = 'first')['value']

    df_valida_var_pivot = df_valida_var_pivot.reset_index()
    #df_valida_var_pivot['fecha']=pd.to_datetime(df_valida_var_pivot['fecha']).dt.strftime('%d/%m/%Y')


    df_valida_var_pivot=df_valida_var_pivot[['fecha', 'estacion','Nombre','lluvia','tmin',
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
                'rad_solar',
               #'tsuelo_5',
               'tsuelo_50',
               'tsuelo_100',
               'Fenomenos/0',
               'Fenomenos/1',
               'Fenomenos/2',
               'Fenomenos/3',
               'Fenomenos/4',
               'Fenomenos/5',
               'Fenomenos/6',
               'Fenomenos/7',
               'Fenomenos/8',
               'Fenomenos/9',
               'Fenomenos/10']]
    """
    df_valida_var_pivot.to_csv(ruta_salida+'data_total2.csv')
    
    now = dt.datetime.now()
    fecha_inicio= (now - dt.timedelta(days=4)).strftime("%Y-%m-%d")
    fecha_final = now.strftime("%Y-%m-%d")

    fecha_inicio=pd.to_datetime(fecha_inicio, format="%Y-%m-%d")
    fecha_final=pd.to_datetime(fecha_final, format="%Y-%m-%d")

    dfE1=df_valida_var_pivot

    filtrofecha1=dfE1['fecha'] >= fecha_inicio
    dfE2=dfE1[filtrofecha1]
      

    filtrofecha2=dfE2['fecha'] <= fecha_final
    dfE=dfE2[filtrofecha2]

    """

    df_metadata=pd.read_excel(ruta_contenedora+'EstacionesConvencionales.xlsx',usecols=['ID','ID_INSIVUMEH','Latitud','Longitud','Altitud'])
    df_data=df_valida_var_pivot

    df_data=df_data.rename(columns={'estacion':'ID'})

    df_merge=pd.merge(df_data,df_metadata,how='outer')
    df_merge.to_csv(ruta_contenedora+'data_diaria_salida_nueva.csv',index=False)


    #df_valida_var_pivot.to_csv(ruta_salida+'df_datadiaria_formato.csv',index=False)
    df_valida_var_pivot.to_csv(des_total,index=False)
    


def validacion_rangos_data_KOBO(df_datatotal,df_historicos,ruta_salida):
    #La fecha queda en el formato de la Google Sheet 
    #Se crea nuevas columnas con la fecha y mes en Data cruda
    #df_verificacion_de_rangos = validacion_rangos_data_KOBO(df_datatotal,df_historicos)
    df_datatotal['Fecha'] = pd.to_datetime(df_datatotal['fecha'])
    #df_datatotal['Fecha']=pd.to_datetime(df_datatotal['fecha'], format="%d/%m/%Y")

    df_datatotal['Fecha_dt'] = pd.to_datetime(df_datatotal['Fecha'])
    df_datatotal['Mes'] = df_datatotal['Fecha_dt'].dt.month
    #Se renombra la columna de la 'Estacion' para que tenga el mismo nombre 
    #al documento de los historicos


    df_datatotal = df_datatotal.rename(columns = {'estacion': 'Estacion'})

    #Se arregla el formato del mes para poder usarlo en documento de historicos
    df_historicos_mensual = df_historicos[df_historicos['MesD'] != 'Anual']
    df_historicos_mensual = df_historicos_mensual.astype({'Mes': int})

    #eliminación columnas innecesarias del historico
    df_historicos_impor = df_historicos_mensual.drop(columns = ["No.","Departamento","Municipio","Region ","Latitud","Longitud","Altitud MSNM","Elevación","MesD","Instrumento","Código de instrumento","Jerarquia de humedad","Jerarquía de temperatura","Tipo de distribución de lluvia","Tipo de variación de la temp","Observador actual","Código de observador ","Edad","Caracteristicas fícas de la estación","Dicultad para el obervador(1 a 10)","Comentarios","Unnamed: 52"])


    #print(df_historicos_mensual.info())



    #PARA MIN
    #Headers para min de historico
    headers_min = ['Mes',
        'Estacion',
        'ID ESTACIONES',
        'MIN_brillo_solar_total_total/Hrs',
        'MIN_direccion_viento',
        'MIN_evaporacion_tanque_total',
        "MIN_evaporacion_piche_total ",
        'MIN_humedad_relativa_promedio',
        'MIN_nubosidad',
        'MIN_precipitacion_total_mm',
        'MIN_presion_atmosferica',
        'MIN_radiacion',
        'MIN_temp_max_oC',
        'MIN_temp_min_oC',
        'MIN_temp_suelo',
        'MIN_temperatura_media_oC',
        'MIN_velocidad_viento']

    #Creación de Tabla Min Historico
    df_historicos_min = df_historicos_impor[headers_min]
    #print(df_historicos_min.info())
    #cambio nombre MIN Historico igual al de Data Cruda
    df_historicos_min = df_historicos_min.rename({
        'MIN_brillo_solar_total_total/Hrs':'bri_solar',
        'MIN_direccion_viento':'dir_viento',
        'MIN_evaporacion_tanque_total':'eva_tan',
        "MIN_evaporacion_piche_total ":'eva_piche' ,
        'MIN_humedad_relativa_promedio':'hum_rel',
        'MIN_nubosidad':'nub',
        'MIN_precipitacion_total_mm':"lluvia",
        'MIN_presion_atmosferica':'pre_atmos',
        'MIN_radiacion':'rad_solar',
        'MIN_temp_max_oC':'tmax',
        'MIN_temp_min_oC':'tmin',
        'MIN_temp_suelo':'tsuelo_100',
        'MIN_temperatura_media_oC':'tseca',
        'MIN_velocidad_viento':'vel_viento'}, axis='columns')

    #Transposicion de columnas MIN Historico
    df_historicos_min_unpivot = pd.melt(df_historicos_min, id_vars = ["Mes", "Estacion", "ID ESTACIONES"], value_vars = [
       'lluvia',
        'tmax',
        'tmin',
        'tseca',
        'bri_solar',
        'rad_solar',
        'eva_tan',
        'eva_piche' ,
        'nub',
        'dir_viento',
        'vel_viento',
        'hum_rel',
        'pre_atmos',
        'tsuelo_100'
        ])

    #cambio nombre variable columna MIN Historico
    df_historicos_min_unpivot.rename(columns = {'value':'rango_min'}, inplace = True)

    #PARA MAX
    #Header MAX Historico
    headers_max = ['Mes', 
        'Estacion',
        'ID ESTACIONES',
        'MAX _brillo_solar_total_total/Hrs',
        'MAX _temp_max_oC', 
        'MAX_direccion_viento',
        'MAX_evaporacion_tanque_total ',
        "MAX_evaporacion_piche_total " ,
        'MAX_humedad_relativa_promedio ', 
        'MAX_nubosidad',
        'MAX_precipitacion_total_mm',
        'MAX_presion_atmosferica',
        'MAX_radiacion',
        'MAX_temp_media_oC',
        'MAX_temp_min_oC',
        'MAX_temp_suelo',
        'MAX_velocidad_viento']

    #Creación de MAX Historico
    df_historicos_max = df_historicos_impor[headers_max]

    #cambio nombre columnas MAX Historico
    df_historicos_max = df_historicos_max.rename({
        'MAX _brillo_solar_total_total/Hrs':'bri_solar',
        'MAX _temp_max_oC':'tmax',
        'MAX_direccion_viento':'dir_viento',
        "MAX_evaporacion_piche_total ":'eva_piche',
        'MAX_evaporacion_tanque_total ':'eva_tan',
        'MAX_humedad_relativa_promedio ':'hum_rel',
        'MAX_nubosidad':'nub',
        'MAX_precipitacion_total_mm':'lluvia',
        'MAX_presion_atmosferica':'pre_atmos',
        'MAX_radiacion':'rad_solar',
        'MAX_temp_media_oC':'tseca',
        'MAX_temp_min_oC':'tmin',
        'MAX_temp_suelo':'tsuelo_100',
        'MAX_velocidad_viento':'vel_viento'}, axis='columns')

    #Trasposicion de columnas MAX Historico

    df_historicos_max_unpivot = pd.melt(df_historicos_max, id_vars = ["Mes", "Estacion", "ID ESTACIONES"], value_vars = [
        'lluvia',
        'tmax',
        'tmin',
        'tseca',
        'bri_solar',
        'rad_solar',
        'eva_tan',
        'eva_piche' ,
        'nub',
        'dir_viento',
        'vel_viento',
        'hum_rel',
        'pre_atmos',
        'tsuelo_100'
        ])
    #print(df_historicos_max_unpivot.info())
    #cambio nombre variable columna MAX Historico
    df_historicos_max_unpivot.rename(columns = {'value':'rango_max'}, inplace = True)
    #df_historicos_max_unpivot.to_csv('prueba.csv')


    #Unión df_historicos_max_unpivot + df_historicos_min_unpivot
    historios_comparativo = df_historicos_min_unpivot.merge(df_historicos_max_unpivot, how = 'left', on = ['Mes', 'Estacion','ID ESTACIONES','variable'])
    historios_comparativo=historios_comparativo.replace({'Na':np.nan,'NA':np.nan,'NA ':np.nan})
    #print(historios_comparativo.info())

    historios_comparativo = historios_comparativo.drop(columns = ["Estacion"])
    historios_comparativo.rename(columns = {'ID ESTACIONES':'Estacion'}, inplace = True)
    #historios_comparativo.to_csv('historicofinal.csv')
    #PARA DATA CRUDA
    #eliminación columnas inecesarias
    df_datatotal_impor = df_datatotal

    #df_datatotal_impor.to_csv("data_impor_drop.csv")
    #Transposición variables
    df_datatotal_impor_unpivot = pd.melt(df_datatotal_impor, id_vars = ["Mes", "Estacion",'Fecha'], value_vars = [ 

        'lluvia',
        'tmax',
        'tmin',
        'tseca',
        'bri_solar',
        'rad_solar',
        'eva_tan',
        'eva_piche' ,
        'nub',
        'dir_viento',
        'vel_viento',
        'hum_rel',
        'pre_atmos',
        'tsuelo_100'
        ])
    #name_fil = 'data_total' +    '.csv'

    #todo bien data cruda

    #df_datatotal_impor_unpivot=df_datatotal_impor_unpivot.replace({'Na':np.nan})
    #df_datatotal_impor_unpivot.to_csv("sinan.csv")
    #Unión de Tabla con rangos y valores de data cruda
    validacion_variables = historios_comparativo.merge(df_datatotal_impor_unpivot, how = 'left', on = ['Mes', 'Estacion','variable'])
    #print(validacion_variables.info())
    #validacion_variables.to_csv('merge_final.csv')
    #Eliminacion de NAN
    validacion_variables_nan = validacion_variables
    '''print(validacion_variables_nan.info())
    #esto de replace se hizo en historico final
    #validacion_variables_nan=validacion_variables_nan.replace({'Na':np.nan,'NA':np.nan,'NA ':np.nan})
    validacion_variables_nan.to_csv("borrar.csv")
    #Rangos y valor de variables en el mismo formato'''
    validacion_variables_nan = validacion_variables_nan.astype(
    {
    'value': float,
    'rango_min': float,
    'rango_max': float
    }
    )
    #print(validacion_variables_nan.info())
    #validacion_variables_nan.to_csv("confloat.csv")
    #Validacion de rango para todas las variables
    validacion_variables_nan['Fuera_rango'] = np.where(
    (validacion_variables_nan['value'] <= validacion_variables_nan['rango_max']) & 
    (validacion_variables_nan['value'] >= validacion_variables_nan['rango_min']),
    'no',
    'si')


    validacion_variables_nan = validacion_variables_nan.reset_index()
    #Reset index
    #print(validacion_variables_nan.reset_index())
    #validacion_variables_nan.to_csv("comparativo.csv")
    #print(validacion_variables_nan.info())
    #print(validacion_variables_nan.info())
    #Creacion de documento con fueras de rango 



    carga_nombre_estacion=orden_id(validacion_variables_nan)
    validacion_variables_nan.insert(3,'Nombre',carga_nombre_estacion)

    


    df_fueraderango = validacion_variables_nan [['Mes','Estacion','Nombre',
                            'Fecha', 'variable','value','Fuera_rango','rango_max','rango_min']]
    df_fueraderango = df_fueraderango[df_fueraderango['Fuera_rango'] == 'si']
    #print(type(df_fueraderango.info()))

    
    df_fueraderango.to_excel(ruta_salida+'Fueraderango.xlsx',index=False)

    #pivotear las estaciones para documento Drive
    #validacion_variables_nan = validacion_variables_nan[validacion_variables_nan['Fuera_rango'] == 'no']
    
    validacion_variables_nan['Fecha']=pd.to_datetime(validacion_variables_nan['Fecha'], format="%d/%m/%Y")
    documento_drive:pd.DataFrame = validacion_variables_nan.pivot_table(values= ['value'], index = ['Fecha','variable'], columns = {'Nombre'}, aggfunc = 'first',sort=False)
    #Creación de documento para Drive
    documento_drive.columns.set_names('', level=1, inplace=True)
    documento_drive.columns = documento_drive.columns.droplevel(0)
    documento_drive.to_csv(ruta_salida+'documento_drive.csv')

"""
def salida_base_datos(df_data_diaria_formato,df_EstacionesConvencionales):

    df_data_diaria=pd.read_csv(df_data_diaria_formato, header=0, parse_dates = ['fecha'], dayfirst = True)
    df_info_general=pd.read_excel(df_EstacionesConvencionales, header=0)

    df_info_general.drop(columns=['No.','Estatus','Estacion','Departamento','Municipio','Nombre_estacion','Tipo','Departamento_INSIVUMEH','Sección'],inplace=True)

    df_data_diaria.rename(columns = {'estacion':'ID'}, inplace = True)

    #
    df_pruebafecha_unpivot = pd.melt(df_data_diaria, id_vars = ["fecha", "ID"], value_vars = ['bri_solar',
               'dir_viento', 'lluvia', 'tmax',
               'eva_piche', 'nub','tmin','vel_viento',
               'eva_tan', 'pre_atmos','tseca',
                'hum_rel', 'rad_solar','tsuelo_100'])
    
    print(df_pruebafecha_unpivot.info())
    
    df_prueba_pivot:pd.DataFrame=df_pruebafecha_unpivot.pivot_table( index = ['fecha','ID'], columns = ['variable'], aggfunc = 'first',sort=False)['value']

    df_prueba_pivot.to_csv('prueba_pivot.csv')
    print(df_prueba_pivot.info())
    df_prueba_pivot_2=df_prueba_pivot.reindex()

    #df_data_diaria_full=df_prueba_pivot_2.merge(df_info_general, how = 'left', on = ['ID'])
    df_data_diaria_full=pd.merge(df_prueba_pivot_2,df_info_general, how = 'outer', on = ['ID'])
   
    print(df_data_diaria_full.info())
    #df_data_diaria_full_2=df_data_diaria_full.drop(columns =['No.','Estatus','Estacion','Departamento','Municipio','Nombre_estacion','Tipo','Departamento_INSIVUMEH','Sección'])

    #print(df_data_diaria_full_2.info())
    header_nuevo = df_data_diaria_full.rename(columns={"ESTA": "ESTACION", "fecha": "date","lluvia":"PREP","hum_rel":"HUMR","tmin":"TMIN","tmax":"TMAX","tseca":"TMED","rad_solar":"RADI","bri_solar":"BRIS","eva_tan":"EVAT","pre_atmos":"PRES","eva_piche":"EVAP","vel_viento":"VELV","dir_viento":"DIRV","nub":"NUBO","tsuelo_100":"TSUELO100","ID":"CODIGO","Latitud":"LAT","Longitud":"LON","Altitud":"ALT"})


    #print(header_nuevo.info())
    salida_final = header_nuevo.reindex(columns=['date','CODIGO','ID_INSIVUMEH','LAT',
                                       'LON','ALT','PREP','TMED',
                                       'TMIN','TMAX','DIRV','VELV',
                                       'HUMR','EVAT','EVAP','BRIS',
                                       'NUBO','PRES','TSUELO5','TSUELO50',
                                       'TSUELO100','RADI','FENO'])
    #print(salida_final.info())
    salida_final.to_csv('data_diaria_full.csv',index=False)
"""

def orden_columnas(ruta_documento,ruta_contenedora):

    docrango= pd.read_excel(ruta_documento+"Fueraderango.xlsx")
    docdrive = pd.read_csv(ruta_documento+'documento_drive.csv')
    #docdrive=docdrive.sort_values(by="Fecha")

    now = dt.datetime.now()
    fecha = now.strftime("%d/%m/%Y")


    #docdrive.sort_values(by=['variable','Fecha'],inplace=True, key=lambda x: x.apply(myfunc))

    #Remplazo de nombres de variables
    docdrive['variable']=docdrive['variable'].replace({'lluvia':'LLUVIA',
            'tmax':'TMAX',
            'tmin':'TMIN',
            'tseca':'TMED',
            'bri_solar':'BRILLO SOLAR',
            'rad_solar':'RADIACION',
            'eva_tan':'EVA_TANQUE',
            'eva_piche':'EVA_PICHE' ,
            'nub':"NUBOSIDAD",
            'dir_viento':"DIR_VIENTO",
            'vel_viento':"VEL_VIENTO",
            'hum_rel':"HUMEDAD_REL",
            'pre_atmos':"PRESION_ATM",
            'tsuelo_100':"TSUELO100"}, regex=False)



    docdrive.drop(1, axis=0, inplace=True)

    #Remplaza de diferentes titulos de documento_drive
    #docdrive["Unnamed: 0"].replace({'Nombre':'FECHA'}, regex=True, inplace=True)
    #docdrive["Unnamed: 1"].replace({np.NAN:'VARIABLE'}, regex=True, inplace=True)


    """docdrive['INS210601CV']=np.nan
    docdrive['INS050901CV']=np.nan
    docdrive['INS-010331']=np.nan
    docdrive['INS-130527']=np.nan
    docdrive['PLUVIOMÉT']=np.nan
    docdrive['INS210101AT']=np.nan
    docdrive['INS090401AT']=np.nan
    docdrive['INS140101AT']=np.nan"""

    #['INS210601CV', 'INS050901CV', 'INS210101AT', 'INS-010331', 'INS-130527', 'PLUVIOMÉT', 'INS090401AT', 'INS140101AT']    
    """docdrive_orden = docdrive[['Unnamed: 0','Unnamed: 1','INS200501CV','INS121601CV',
                                            'INS110701CV','INS160101CV','INS071301CV','INS200701CV',
                                            'INS170101CV','INS010102CV','INS210601CV','INS190201CV',
                                            'INS090301CV','INS090101CV','INS100101CV','INS221401CV',
                                            'INS160701CV','INS180101CV','INS110101CV','INS141601CV',   
                                            'INS120101CV','INS041001CV','INS131501CV','INS010101CV',
                                            'INS040101CV','INS220501CV','INS-140301','INS070101CV', 
                                            'INS130101CV','INS190901CV','INS180201CV','INS141301CV',
                                            'INS171201CV','INS221701CV','INS050101CV','INS020302CV',
                                            'INS150701CV','INS050901CV','INS130601CV','INS161201CV',  
                                            'INS071901CV','INS030801CV','INS121701CV','INS141901CV',
                                            'INS150401CV','INS020301CV','INS060101CV','INS190301CV', 
                                            'INS210101CV','INS040301CV','INS011401AT','INS030101AT',
                                            'INS050101AT','INS122301AT','INS210101AT','INS122101AT',
                                            'INS-010331','INS010801AT','INS180501AT','INS180401AT',
                                            'INS-130527','PLUVIOMÉT','INS090401AT','INS142201AT', 
                                            'INS010301AT','INS010701AT','INS140101AT','INS040801AT','INS080101AT'   
                                            ] ]"""
    
    docdrive_orden=docdrive.reindex(columns=['Fecha','variable','CAMOTAN', 'CATARINA', 'CHAMPERICO_FEGUA', 'COBAN', 
                          'EL_CAPITAN', 'ESQUIPULAS', 'FLORES_AEROPUERTO', 
                          'LA_AURORA', 'LA_CEIBITA','LA_FRAGUA', 'LABOR_OVALLE', 
                          'LOS_ALTOS', 'MAZATENANGO', 'MONTUFAR', 
                          'PANZOS_PHC_ALTA_VERAPAZ', 'PUERTO_BARRIOS_PHC', 
                          'RETALHULEU_AEROPUERTO', 'SACAPULAS', 'SAN_MARCOS_PHC', 
                          'SANTA_CRUZ_BALANYA', 'TODOS_SANTOS', 'INSIVUMEH', 
                          'ALAMEDA_ICTA', 'ASUNCION_MITA', 'CHINIQUE', 'EL_TABLON', 
                          'HUEHUETENANGO', 'LA_UNION', 'LAS_VEGAS_PHC', 'NEBAJ', 
                          'POPTUN', 'QUEZADA', 'SABANA_GRANDE', 'SAN_AGUSTIN_ACASAGUASTLAN', 
                          'SAN_JERONIMO_R_H', 'SAN_JOSE_AEROPUERTO', 'SAN_PEDRO_NECTA', 
                          'SANTA_MARIA_CAHABON', 'SANTIAGO_ATITLAN', 'SUIZA_CONTENTA', 
                          'TECUN_UMAN', 'CHIXOY_PCH', 'CUBULCO', 'LOS_ALBORES', 
                          'LOS_ESCLAVOS', 'PASABIEN', 'POTRERO_CARRILLO', 
                          'SAN_MARTIN_JILOTEPEQUE', 'AMATITLAN', 'ANTIGUA_GUATEMALA', 
                          'CONCEPCION', 'IXCHIGUAN', 'JALAPA', 'LA_REFORMA', 'LAS_NUBES', 
                          'LO_DE_COY', 'MARISCOS', 'MORALES_MET', 'NENTON', 'NUEVA_CONCEPCION', 
                          'PACHUTE', 'PLAYA_GRANDE_IXCAN', 'SAN_JOSE_PINULA', 'SAN_PEDRO_AYAMPUC', 
                          'SANTA_CRUZ_DEL_QUICHE', 'SANTA_MARGARITA', 'TOTONICAPAN'])


    #Creacionde de docuementos fuera de rango
    #docdrive_orden=docdrive_orden.sort_values(by="Fecha")

    docdrive_orden.to_csv(ruta_documento+'documento_drive.csv',index=False)
    docdrive_orden.to_excel(ruta_documento+'documento_drive.xlsx',index=False)

    docdrive_orden.to_csv(ruta_contenedora+'documento_drive.csv',index=False)
    docdrive_orden.to_excel(ruta_contenedora+'documento_drive.xlsx',index=False)



    #Filtro de fecha actual para fueraderango
    docrango=docrango.dropna(subset="value")
    docrango=docrango.sort_values(by="Nombre")
    filtro=docrango["Fecha"]==fecha
    docrango=docrango[filtro]
    docrango.to_excel(ruta_documento+"Fueraderango.xlsx",index=False)
    docrango.to_excel(ruta_contenedora+"Fueraderango.xlsx",index=False)
    print("Termino la creacion de los docuemntos")




def fenomeno_tratamiento(df_salida_fenomenos_crudo,df_datadiaria_formato,df_EstacionesConvencionales,ruta_salida):

    df_feno_crudo= pd.read_csv(df_salida_fenomenos_crudo,header=0, parse_dates = ['fecha'], dayfirst = True)
    
    df_info_esta_conve= pd.read_excel(df_EstacionesConvencionales,header=0)


    nuevos_nombres = {
    'Fenomenos/0': '0',
    'Fenomenos/1': '1',
    'Fenomenos/2': '2',
    'Fenomenos/3': '3',
    'Fenomenos/4': '4',
    'Fenomenos/5': '5',
    'Fenomenos/6': '6',
    'Fenomenos/7': '7',
    'Fenomenos/8': '8',
    'Fenomenos/9': '9',
    'Fenomenos/10': '10',
    'estacion': 'ID',
  }
    df_feno_crudo.rename(columns=nuevos_nombres, inplace=True)



    df_FENO_drop = df_feno_crudo.drop(columns =['lluvia','tmin','tseca','tmax','eva_tan','eva_piche','hum_rel','bri_solar','nub','vel_viento',
                                                'dir_viento','pre_atmos','tsuelo_50','tsuelo_100','rad_solar'])

   
    df_feno_unpivot = pd.melt(df_FENO_drop, id_vars = ['ID','fecha'], value_vars = ['1','2',
               '3','4','5',
               '6','7','8',
               '9','10'])
    
    
    df_feno_unpivot['variable'] = pd.to_numeric(df_feno_unpivot['variable'],errors='coerce')

    df_feno_unpivot['FENO'] = np.where(
    (df_feno_unpivot['value'] == 1),
    df_feno_unpivot['variable'],
    np.nan)

    #print(df_feno_unpivot)

    #print(df_feno_unpivot.info())

    df_FENO_final = df_feno_unpivot.drop(columns =['variable','value'])


    df_FENO_final.dropna(inplace=True)


    df_FENO_final['FENO']=(df_FENO_final['FENO']).astype(int)

    df_FENO_final.rename(columns={'ID':'CODIGO','fecha':'FECHA','FENO':'FENÓMENOS'}, inplace=True)

    df_FENO_melt= pd.melt(df_FENO_final, id_vars = ["FECHA", "CODIGO"], value_vars = ['FENÓMENOS'])

    df_FENO_melt.rename(columns={'variable':'VARIABLE','value':'VALOR'}, inplace=True)
    df_FENO_melt.to_csv(ruta_salida+'info_feno.csv',index=False)
        

    #print(df_FENO_melt)
    #print(df_FENO_final)

    #df_full_info_feno=df_FENO_final.merge(df_info_esta_conve, how = 'left', on = ['ID'])

    #df_info_feno=df_full_info_feno.drop(columns =['No.','Estatus','Estacion','Departamento','Municipio','Nombre_estacion','Tipo','Departamento_INSIVUMEH','Sección'])
    #df_info_feno.to_csv('info_feno.csv')



def salida_base_datos(des_total,des_info_feno,ruta_salida):
    df_correccion_lluvia=pd.read_csv(des_total)
    print(df_correccion_lluvia.info())

    df_info_FENO=pd.read_csv(des_info_feno)

    #cambio nombre variable columna
    df_correccion_lluvia.rename(columns = {'estacion':'CODIGO','fecha':'FECHA'}, inplace = True)
    df_correccion_lluvia.drop(columns=['Nombre','Fenomenos/0',
               'Fenomenos/1',
               'Fenomenos/2',
               'Fenomenos/3',
               'Fenomenos/4',
               'Fenomenos/5',
               'Fenomenos/6',
               'Fenomenos/7',
               'Fenomenos/8',
               'Fenomenos/9',
               'Fenomenos/10'],inplace=True)
    

    header_nuevo = df_correccion_lluvia.rename(columns={"lluvia":"PRECIPITACIÓN","hum_rel":"HUMEDAD_RELATIVA",
                                                        "tmin":"TEMPERATURA_MÍNIMA","tmax":"TEMPERATURA_MÁXIMA","tseca":"TEMPERATURA_MEDIA","rad_solar":"RADIACIÓN","bri_solar":"BRILLO_SOLAR","eva_tan":"EVAPORACIÓN_TANQUE","pre_atmos":"PRESIÓN_ATMOSFÉRICA","eva_piche":"EVAPORACIÓN_PICHE","vel_viento":"VELOCIDAD_VIENTO",
                                                        "dir_viento":"DIRECCIÓN_VIENTO","nub":"NUBOSIDAD","tsuelo_100":"TEMPERATURA_SUELO100CM","tsuelo_50":"TEMPERATURA_SUELO50CM"})

    #Transposicion de columnas
    datadiaria_pivot = pd.melt(header_nuevo, id_vars = ["FECHA", "CODIGO"], value_vars = ['PRECIPITACIÓN',
                'HUMEDAD_RELATIVA', 'TEMPERATURA_MÍNIMA', 'TEMPERATURA_MÁXIMA',
                'TEMPERATURA_MEDIA', 'RADIACIÓN','BRILLO_SOLAR','EVAPORACIÓN_TANQUE',
                'PRESIÓN_ATMOSFÉRICA', 'EVAPORACIÓN_PICHE','VELOCIDAD_VIENTO',
                    'DIRECCIÓN_VIENTO', 'NUBOSIDAD','TEMPERATURA_SUELO100CM','TEMPERATURA_SUELO50CM'])

    datadiaria_pivot.rename(columns = {'variable':'VARIABLE','value':'VALOR'}, inplace = True)

    vertical_concat = pd.concat([datadiaria_pivot,df_info_FENO], axis=0)
    vertical_concat.to_csv(ruta_salida+'000_data_diaria_cruda.csv',index=False)



def modificacion_datos():

    df_data_diaria_cruda = pd.read_csv('/home/clima/salida_auto_kobo/000_data_diaria_cruda.csv', header = 0,parse_dates = ['FECHA'], dayfirst = True)
    df_data_diaria_cruda['FECHA']=df_data_diaria_cruda['FECHA'].astype('datetime64[ns]')
    df_ids_nombres_esta = pd.read_excel('/home/clima/Desktop/NO_BORRAR/EstacionesConvencionales - copia.xlsx', header = 0)
    df_modificacion_datos = pd.read_csv('/home/clima/Desktop/NO_BORRAR/C.C_MODIFICACIÒN_DE_DATOS.csv', header = 0,parse_dates = ['FECHA'], dayfirst = True)
    #Data modificada historica que se actualizará
    df_data_cruda_hist = pd.read_csv('/home/clima/Desktop/NO_BORRAR/000_datos_clima_his_sinfeno.csv', header = 0, encoding='latin1' ,parse_dates = ['FECHA'], dayfirst = True, delimiter=';')


    #print(df_data_diaria_cruda.info())
    #print(df_data_cruda_hist.info())
    df_data_cruda_hist['FECHA']=df_data_cruda_hist['FECHA'].astype('datetime64[ns]')
    df_data_hist_y_diaria_actual1 = df_data_cruda_hist.merge(df_data_diaria_cruda, how = 'outer', on = ['CODIGO','FECHA','VARIABLE'])

    df_data_hist_y_diaria_actual2 = df_data_hist_y_diaria_actual1
    #Validacion de rango para todas las variables
    df_data_hist_y_diaria_actual2['valor_nuevo'] = np.where(
          (df_data_hist_y_diaria_actual2['VALOR_y'].isnull()) &
          (df_data_hist_y_diaria_actual2['VALOR_x'] != df_data_hist_y_diaria_actual2['VALOR_y']),
          df_data_hist_y_diaria_actual2['VALOR_x'], 
          df_data_hist_y_diaria_actual2['VALOR_y'])

    df_data_hist_y_diaria_actual2_drop = df_data_hist_y_diaria_actual2.drop(columns = ["VALOR_x","VALOR_y"])
    df_data_hist_y_diaria_actual2_drop.rename(columns = {'VAR':'VARIABLE','valor_nuevo':'VALOR'}, inplace = True)

    df_data_hist_y_diaria_actual = df_data_hist_y_diaria_actual2_drop
    print(df_data_hist_y_diaria_actual.head())

    #MERGE CON CODIGO Y NOMBRE DE ESTACION
    df_data_hist_y_diaria_actual_nombre = df_data_hist_y_diaria_actual.merge(df_ids_nombres_esta, how = 'left', on = ['CODIGO'])


    #cambio de nombre
    df_data_hist_y_diaria_actual_nombre.rename(columns = {'VARIABLE':'VAR'}, inplace = True)
    df_modificacion_datos.rename(columns = {'VARIABLE':'VAR'}, inplace = True)

    print(df_modificacion_datos.info())

    df_data_hist_y_diaria_actual_nombre['FECHA'] = pd.to_datetime(df_data_hist_y_diaria_actual_nombre['FECHA'],format='%Y-%m-%d')

    df_modificacion_datos['FECHA'] = pd.to_datetime(df_modificacion_datos['FECHA'],format='%Y-%m-%d')
    #Eliminación de dato con -99.9 del doc df_modificacion_datos
    df_modificacion_datos['VALOR'] = df_modificacion_datos['VALOR'].replace(-99.9,np.nan)
    df_modificacion_datos2 = df_modificacion_datos

    print(df_data_hist_y_diaria_actual_nombre.info())

    #merge con los datos por modificar
    df_data_con_modificacion = df_data_hist_y_diaria_actual_nombre.merge(df_modificacion_datos2, how = 'left', on = ['NOMBRE_ESTACIÓN', 'FECHA','VAR'])
    #eliminacion de columnas extras
    df_data_con_modificacion_drop= df_data_con_modificacion.drop(columns = ["borrar","ID_INSIVUMEH","No.","LATITUD","LONGITUD","ALTITUD","DEPARTAMENTO","MUNICIPIO","NO","RAZÓN","ANALISTA"])


    df_modificación_val = df_data_con_modificacion_drop
    #Validacion de rango para todas las variables
    df_modificación_val['valor_nuevo'] = np.where(
        (df_modificación_val['VALOR_y'].isnull()) &
        (df_modificación_val['VALOR_x'] != df_modificación_val['VALOR_y']),
        df_modificación_val['VALOR_x'],
        df_modificación_val['VALOR_y'])


    df_data_con_modificacion_val_drop = df_modificación_val.drop(columns = {"VALOR_x","VALOR_y","NOMBRE_ESTACIÓN","FECHA_ACTUAL"})
    print(df_data_con_modificacion_val_drop)

    df_data_con_modificacion_val_drop.rename(columns = {'VAR':'VARIABLE','valor_nuevo':'VALOR'}, inplace = True)


    #df_data_con_modificacion_val_drop.to_csv('data_modificada_actualizada.csv',index=False)
    df_data_con_modificacion_val_drop.to_csv('001_data_modificada_actualizada.csv',index=False)

    
def salida_variable_base_datos():

    df_000_data_diaria_cruda=pd.read_csv('001_data_modificada_actualizada.csv')
    salida_temp_min = df_000_data_diaria_cruda.query("VARIABLE == 'TEMPERATURA_MÍNIMA'")
    salida_hum_rel = df_000_data_diaria_cruda.query("VARIABLE == 'HUMEDAD_RELATIVA'")
    salida_bri_solar = df_000_data_diaria_cruda.query("VARIABLE == 'BRILLO_SOLAR'")
    salida_temp_max= df_000_data_diaria_cruda.query("VARIABLE == 'TEMPERATURA_MÁXIMA'")
    salida_temp_med = df_000_data_diaria_cruda.query("VARIABLE == 'TEMPERATURA_MEDIA'")
    salida_radiacion = df_000_data_diaria_cruda.query("VARIABLE == 'RADIACIÓN'")
    salida_eva_tan = df_000_data_diaria_cruda.query("VARIABLE == 'EVAPORACIÓN_TANQUE'")
    salida_pre_atm = df_000_data_diaria_cruda.query("VARIABLE == 'PRESIÓN_ATMOSFÉRICA'")
    salida_eva_pi = df_000_data_diaria_cruda.query("VARIABLE == 'EVAPORACIÓN_PICHE'")
    salida_vel_viento = df_000_data_diaria_cruda.query("VARIABLE == 'VELOCIDAD_VIENTO'")
    salida_dir_viento= df_000_data_diaria_cruda.query("VARIABLE == 'DIRECCIÓN_VIENTO'")
    salida_nub = df_000_data_diaria_cruda.query("VARIABLE == 'NUBOSIDAD'")
    salida_tem_100 = df_000_data_diaria_cruda.query("VARIABLE == 'TEMPERATURA_SUELO100CM'")
    salida_temp_50 = df_000_data_diaria_cruda.query("VARIABLE == 'TEMPERATURA_SUELO50CM'")
    salida_precipitacion = df_000_data_diaria_cruda.query("VARIABLE == 'PRECIPITACIÓN'")

    salida_temp_min2 = salida_temp_min.drop(columns = ["VARIABLE"])
    salida_hum_rel2 = salida_hum_rel.drop(columns = ["VARIABLE"])
    salida_bri_solar2 = salida_bri_solar.drop(columns = ["VARIABLE"])
    salida_temp_max2 = salida_temp_max.drop(columns = ["VARIABLE"])
    salida_temp_med2 = salida_temp_med.drop(columns = ["VARIABLE"])
    salida_radiacion2 = salida_radiacion.drop(columns = ["VARIABLE"])
    salida_eva_tan2 = salida_eva_tan.drop(columns = ["VARIABLE"])
    salida_pre_atm2 = salida_pre_atm.drop(columns = ["VARIABLE"])
    salida_eva_pi2 = salida_eva_pi.drop(columns = ["VARIABLE"])
    salida_vel_viento2 = salida_vel_viento.drop(columns = ["VARIABLE"])
    salida_dir_viento2 = salida_dir_viento.drop(columns = ["VARIABLE"])
    salida_nub2 = salida_nub.drop(columns = ["VARIABLE"])
    salida_tem_1002 = salida_tem_100.drop(columns = ["VARIABLE"])
    salida_temp_502 = salida_temp_50.drop(columns = ["VARIABLE"])
    salida_precipitacion2 = salida_precipitacion.drop(columns = ["VARIABLE"])


    salida_temp_min2.to_csv('/home/clima/automatizacion_kobo/001_temperatura_minima_datos_clima.csv',index=False)
    salida_hum_rel2.to_csv('/home/clima/automatizacion_kobo/001_humedad_relativa_datos_clima.csv',index=False)
    salida_bri_solar2.to_csv('/home/clima/automatizacion_kobo/001_brillo_solar_datos_clima.csv',index=False)
    salida_temp_max2.to_csv('/home/clima/automatizacion_kobo/001_temperatura_maxima_datos_clima.csv',index=False)
    salida_temp_med2.to_csv('/home/clima/automatizacion_kobo/001_temperatura_media_datos_clima.csv',index=False)
    salida_radiacion2.to_csv('/home/clima/automatizacion_kobo/001_radiacion_datos_clima_his.csv',index=False)
    salida_eva_tan2.to_csv('/home/clima/automatizacion_kobo/001_evaporacion_tanque_datos_clima.csv',index=False)
    salida_pre_atm2.to_csv('/home/clima/automatizacion_kobo/001_presion_atmosferica_datos_clima.csv',index=False)
    salida_eva_pi2.to_csv('/home/clima/automatizacion_kobo/001_evaporacion_piche_datos_clima.csv',index=False)
    salida_vel_viento2.to_csv('/home/clima/automatizacion_kobo/001_velocidad_viento_datos_clima.csv',index=False)
    salida_dir_viento2.to_csv('/home/clima/automatizacion_kobo/001_direccion_viento_datos_clima.csv',index=False)
    salida_nub2.to_csv('/home/clima/automatizacion_kobo/001_nubosidad_datos_clima.csv',index=False)
    salida_tem_1002.to_csv('/home/clima/automatizacion_kobo/001_temperatura_100cm_datos_clima.csv',index=False)
    salida_temp_502.to_csv('/home/clima/automatizacion_kobo/001_temperatura_50cm_datos_clima.csv',index=False)
    salida_precipitacion2.to_csv('/home/clima/automatizacion_kobo/001_precipitacion_datos_clima.csv',index=False)

def salida_base_antigua():
    #salida antigua
    df_000_data_diaria_cruda = pd.read_csv('/home/clima/salida_auto_kobo/000_data_diaria_cruda.csv')
    tabla_final_header_estacion = df_000_data_diaria_cruda.pivot_table(values= ['VALOR'], index = ['FECHA','VARIABLE'], columns = {'CODIGO'}, aggfunc = 'first',sort=False)
    tabla_final_header_estacion.columns.set_names('', level=1, inplace=True)
    tabla_final_header_estacion.columns = tabla_final_header_estacion.columns.droplevel(0)
    tabla_final_header_estacion.to_csv('/home/clima/salida_auto_kobo/salida_baseantigua.csv')

def salida_nueva():
    data_procesada_2 = pd.read_csv('/home/clima/automatizacion_kobo/data_diaria_salida_nueva.csv')
    #eliminaci[on de columnas inecesarias]
    data_procesada_2.drop(columns=['Fenomenos/0',
        'Fenomenos/1',
        'Fenomenos/2',
        'Fenomenos/3',
        'Fenomenos/4',
        'Fenomenos/5',
        'Fenomenos/6',
        'Fenomenos/7',
        'Fenomenos/8',
        'Fenomenos/9',
        'Fenomenos/10'],inplace=True)
    data_procesada_2_drop = data_procesada_2
    # Aplica los reemplazos en las columnas del DataFrame
    data_procesada_2_drop.rename(columns={
        "fecha": "FECHA",
        "lluvia": "PRECIPITACIÓN",
        "hum_rel": "HUMEDAD_RELATIVA",
        "tmin": "TEMPERATURA_MÍNIMA",
        "tmax": "TEMPERATURA_MÁXIMA",
        "tseca": "TEMPERATURA_MEDIA",
        "rad_solar": "RADIACIÓN",
        "bri_solar": "BRILLO_SOLAR",
        "eva_tan": "EVAPORACIÓN_TANQUE",
        "pre_atmos": "PRESIÓN_ATMOSFÉRICA",
        "eva_piche": "EVAPORACIÓN_PICHE",
        "vel_viento": "VELOCIDAD_VIENTO",
        "dir_viento": "DIRECCIÓN_VIENTO",
        "nub": "NUBOSIDAD",
        "tsuelo_100": "TEMPERATURA_SUELO_100CM",

        "tsuelo_50": "TEMPERATURA_SUELO_50CM",
        "Latitud": "LATITUD",
        "Longitud": "LONGITUD",
        "Altitud": "ALTITUD",
        "Nombre": "NOMBRE",
    }
    , inplace=True)

    data_procesada_3 = data_procesada_2_drop
    data_procesada_3['FECHA']=pd.to_datetime(data_procesada_3['FECHA'])
    # Calcular la fecha actual
    hoy_fecha = dt.datetime.now()

    # Calcular la fecha de hace 30 días
    hace_30_dias = hoy_fecha - dt.timedelta(days=30)

    # Filtrar las filas con fechas dentro de los últimos 30 días
    df_filtrado_data_procesada3 = data_procesada_3[(data_procesada_3['FECHA'] >= hace_30_dias) & (data_procesada_3['FECHA'] <= hoy_fecha)]



    df_filtrado_orden=df_filtrado_data_procesada3.reindex(columns=['FECHA','ID_INSIVUMEH','NOMBRE','LATITUD', 'LONGITUD', 'ALTITUD', 'PRECIPITACIÓN', 
                          'TEMPERATURA_MÁXIMA', 'TEMPERATURA_MEDIA', 'TEMPERATURA_MÍNIMA', 
                          'DIRECCIÓN_VIENTO', 'VELOCIDAD_VIENTO','HUMEDAD_RELATIVA', 'EVAPORACIÓN_TANQUE', 
                          'EVAPORACIÓN_PICHE', 'BRILLO_SOLAR', 'NUBOSIDAD', 
                          'PRESIÓN_ATMOSFÉRICA', 'TEMPERATURA_SUELO_50CM', 
                          'TEMPERATURA_SUELO_100CM', 'RADIACIÓN', 'ID'
    ])
    df_filtrado_orden.to_csv('/home/clima/salida_auto_kobo/salida_interna_diaria.csv',index=False)

#modificacion_datos()
#data_anulacion(name_auto,ruta_salida)
ruta_salida="/home/clima/salida_auto_kobo/"
name_anulacion=ruta_salida+"Formulario_anulacion_datos_climaticos.csv"
#salida_variable_base_datos()
#data_anulacion(name_anulacion,ruta_salida)
#modificacion_datos()
#des_info_feno='info_feno.csv'
"""
descarga_datos(ruta_salida,ruta_salida)
salida_diaria(data_auto(name_auto,ruta_salida),data_conven(name_con,ruta_salida),data_sinop(name_sinop,ruta_salida),ruta_salida)
anulacion_data(des_sinop,des_conven,des_auto,des_total,name_anulacion)
viento.extraccion_viento(des_auto,des_sinop,des_conven,des_total,ruta_salida)
correccion_fecha_lluvia(des_total)

df_datatotal = pd.read_csv(ruta_salida+'df_datadiaria_formato.csv', header = 0)
df_historico = pd.read_csv(ruta_contenedora+'MasterData Estaciones - MasterData.csv', header = 0)

validacion_rangos_data_KOBO(df_datatotal,df_historico,ruta_salida)
orden_columnas(ruta_salida,ruta_contenedora)
"""
#fenomeno_tratamiento('/home/joshc/automatizacion_kobo_v2/salida_fenomenos_crudo.csv','/home/joshc/automatizacion_kobo_v2/df_datadiaria_formato.csv','/home/joshc/automatizacion_kobo_v2/EstacionesConvencionales.xlsx')
#salida_base_datos('/home/joshc/automatizacion_kobo_v2/df_datadiaria_formato.csv','/home/joshc/automatizacion_kobo_v2/EstacionesConvencionales.xlsx')
#conexion_base_datos()

