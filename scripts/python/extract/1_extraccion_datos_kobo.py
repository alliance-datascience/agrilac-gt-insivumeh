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

token='e6f030307158e83c473c76789f11159a4c945e50'

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



def descarga_datos(ruta_salida,ruta):


    #Se le dan las credenciales y el token del propitario del formulario de KOBO
    #ruta='/home/joshc/automatizacion_kobo/'
    os.system('curl -X GET https://kobo.humanitarianresponse.info/api/v2/assets.json -H "Authorization: Token e6f030307158e83c473c76789f11159a4c945e50" >'+ ruta+'apiDic.json')
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


descarga_datos(ruta_salida,ruta_contenedora)