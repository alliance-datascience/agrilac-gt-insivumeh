
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



def validacion_rangos_data_KOBO(df_datatotal,df_historicos,ruta_salida):
    #La fecha queda en el formato de la Google Sheet 
    #Se crea nuevas columnas con la fecha y mes en Data cruda
    #df_verificacion_de_rangos = validacion_rangos_data_KOBO(df_datatotal,df_historicos)
    #df_datatotal['Fecha'] = pd.to_datetime(df_datatotal['fecha']).dt.strftime('%d/%m/%Y')
    df_datatotal['Fecha']=pd.to_datetime(df_datatotal['fecha'], format="%d/%m/%Y")

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
    df_datatotal_impor = df_datatotal.drop(columns = ["Unnamed: 0"])

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

df_datatotal = pd.read_csv(ruta_salida+'data_total.csv', header = 0)
df_historico = pd.read_csv(ruta_contenedora+'MasterData Estaciones - MasterData.csv', header = 0)

validacion_rangos_data_KOBO(df_datatotal,df_historico,ruta_salida)