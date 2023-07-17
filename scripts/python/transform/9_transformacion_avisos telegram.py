import json, os
import datetime as dt
import shutil
import pandas as pd 
import smtplib
import telepot 
import validacion_variables as validacion 


ruta="/home/joshc/automatizacion_kobo/"


#Generacion de los mensajes de salida dde los mensajes de salida de telegram
def mensaje_telegram(ID2,nombre_archivo05,nombre_archivo04):

    
    #Token obtenido del bot (a traves de @botfather)
    #climatología
    token= '6193923076:AAEJ2FAUJZBQ0MwKPafWfm3OhH1_aEOEvGQ'

    #ID del grupo. (Futuro: Grupo de Climatología)
    receiver_id = -949733848
    bot = telepot.Bot(token)

    #mensaje de salida para estaciones faltantes
    mensaje="Estaciones Faltantes por ingresar su Formulario por KOBO \n"
    m=1
    for i in range(0,len(ID2)):
        if ((ID2['Estado'].iloc[i]=='CAPACITADA')):
            mensaje=mensaje + str(m) +") "+ str((ID2['Nombre'].iloc[i]))  + '\n \n' #+ "=" + str((ID2['Estado'].iloc[i])) + '\n \n'
            m=m+1
    mensaje=mensaje+str(dt.datetime.now())+'\n' +"Final" +"\n"
    bot.sendMessage(receiver_id,text= mensaje, parse_mode= 'HTML')
    print(mensaje)
    #Mensaje para control de variables climaticas y control de cantidad de ingresos de formularios
    mensaje2="Revisar los datos de las siguientes estaciones:" + "\n \n"
    control_val1=validacion.control_variables(nombre_archivo04)
    control_val2=validacion.control_variables(nombre_archivo05)
    mensaje2=mensaje2+control_val1+'\n'+control_val2
    #Control el envio del mensaje de control de las variables
    if (control_val2!="") or (control_val1!=""): 
        bot.sendMessage(receiver_id,text= mensaje2, parse_mode= 'HTML')
        print(mensaje2)

#Control de horarios de salida de 
def control_hora(df_control1,df_control2,df_control3,nombre_archivo05,nombre_archivo04):

    #carga del documento de control para las estaciones faltantes 
    ID=pd.read_csv(ruta+"Nombre.csv",index_col=False)
    #carga de datos descargados de KOBO para el control horario
    df1=pd.read_csv(df_control1,delimiter=';',usecols=['Fecha','estacion',"Hora"],low_memory=False)
    df2=pd.read_csv(df_control2,delimiter=';',usecols=['Fecha','estacion',"Hora"],low_memory=False)
    df3=pd.read_csv(df_control3,delimiter=';',usecols=['Fecha','estacion',"Hora"],low_memory=False)

    #definicion ded Fecha Actual
    now = dt.datetime.now()
    fecha = now.strftime("%d/%m/%Y")
    now = dt.time(now.hour,now.minute)

    #Horarios de control de ingreso de datos a KOBO
    #Control de ingreso de datos 7:00 horas
    if now > dt.time(7,14) and now < dt.time(8,10):
        filtro=df1['Hora'] == "7_00"
        df1=df1[filtro]
        
        filtro=df2['Hora'] == "7_00"
        df2=df2[filtro]

        filtro=df3['Hora'] == "7_00"
        df3=df3[filtro]

        df1['Fecha']=pd.to_datetime(df1['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df1['Fecha']==fecha
        df1=df1[filtro]
        eliminacion=df1["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df2['Fecha']=pd.to_datetime(df2['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df2['Fecha']==fecha
        df2=df2[filtro]
        eliminacion=df2["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df3['Fecha']=pd.to_datetime(df3['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df3['Fecha']==fecha
        df3=df3[filtro]
        eliminacion=df3["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        #print("Hola_mundo")
        mensaje_telegram(ID,nombre_archivo05,nombre_archivo04)

    #Control de ingreso datos 13:00 horas
    if now > dt.time(13,20) and now < dt.time(14,10):
        filtro=df1['Hora'] == "13_00"
        df1=df1[filtro]
        
        filtro=df2['Hora'] == "13_00"
        df2=df2[filtro]

        filtro=df3['Hora'] == "13_00"
        df3=df3[filtro]

        df1['Fecha']=pd.to_datetime(df1['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df1['Fecha']==fecha
        df1=df1[filtro]
        eliminacion=df1["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df2['Fecha']=pd.to_datetime(df2['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df2['Fecha']==fecha
        df2=df2[filtro]
        eliminacion=df2["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df3['Fecha']=pd.to_datetime(df3['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df3['Fecha']==fecha
        df3=df3[filtro]
        eliminacion=df3["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)
        
        mensaje_telegram(ID,nombre_archivo05,nombre_archivo04)

    #Control de ingreso datos 18:00 horas
    if now > dt.time(18,20) and now < dt.time(19,10):
        filtro=df1['Hora'] == "18_00"
        df1=df1[filtro]
        
        filtro=df2['Hora'] == "18_00"
        df2=df2[filtro]

        filtro=df3['Hora'] == "18_00"
        df3=df3[filtro]

        df1['Fecha']=pd.to_datetime(df1['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df1['Fecha']==fecha
        df1=df1[filtro]
        eliminacion=df1["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df2['Fecha']=pd.to_datetime(df2['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df2['Fecha']==fecha
        df2=df2[filtro]
        eliminacion=df2["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        df3['Fecha']=pd.to_datetime(df3['Fecha']).dt.strftime('%d/%m/%Y')
        filtro=df3['Fecha']==fecha
        df3=df3[filtro]
        eliminacion=df3["estacion"].str.upper()
        for i in eliminacion:
            ID.drop(ID[ID["Nombre"]==i].index, axis=0, inplace=True)

        mensaje_telegram(ID,nombre_archivo05,nombre_archivo04)



import pandas as pd
import json, os
import datetime as dt
import shutil
import pandas as pd 
import smtplib
import numpy as np
ruta='/home/joshc/codigos_kobo/'
ruta_pruebas="/home/joshc/holamundo/"
ruta_auto=ruta+"data_auto.csv"




#Diccionario que remplaza todos los nombres de las estaciones por los codigos de las mismas 
ID={"CHAMPERICO_FEGUA":"INS110701CV","COBAN":"INS160101CV","ESQUIPULAS":"INS200701CV","FLORES_AEROPUERTO":"INS170101CV",\
    "LA_AURORA":"INS010102CV","LA_FRAGUA":"INS190201CV","LOS_ALTOS":"INS090101CV","MONTUFAR":"INS221401CV",\
    "PUERTO_BARRIOS_PHC":"INS180101CV","RETALHULEU_AEROPUERTO":"INS110101CV","HUEHUETENANGO":"INS130101CV",\
    "POPTUN":"INS171201CV","SAN_JOSE_AEROPUERTO":"INS050901CV","TECUN_UMAN":"INS121701CV"," RETALHULEU_AREOPUERTO":"INS110101CV",\
    "FLORES_AEREOPUERTO":"INS170101CV","RETALHULEU_AREOPUERTO":"INS110101CV","SAN_JOSE_AREOPUERTO":"INS050901CV",
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
    "PACHUTÉ":"INS090401AT","PLAYA_GRANDE_IXCAN":"INS142201AT","SAN_JOSE_PINULA":"INS010301AT","SAN_PEDRO_AYAMPUC":"INS010701AT","SANTA_CRUZ_DEL_QUICHE":"INS140101AT",\
    "SANTA_MARGARITA":"INS040801AT","TOTONICAPAN":"INS080101AT","CHINIQUE":"INS-140301"
    }


# Funcion devuelve una columna con los nombres de las estaciones remplazando los ID, con el indice "Nombre"  para analisis posteriores y para el archivo fuera de rango
# Para que funcion "orden_id(df)" df debe de ser un dataframe y debe tener una columna con nombre de indice "Estacion"
def orden_id(df_orden):
    #Se asigna y se le da vuelta al diccionario ID para poder guardarlo en d_swap
    d=ID
    d_swap = {v: k for k, v in d.items()}
    #df.rename({"Estacion":"estacion"},inplace=True)
    #Toma la columna "Estacion" del dataframe df y la comvierte en un string y lo pasa todo a mayusculas si es necesario
    con_esta=df_orden['estacion'].str.upper()
    #Este ciclo compara los valores de la columna "Estacion" con los valores del diccionario d_saw 
    # y si no encuentra algun valor en el diccionario mostrara en pantalla "Fallo para " y el valor que hace falta en el diccionario
    for i in range(len(con_esta)):
        try: con_esta[i]=d_swap[con_esta[i]]
        except: hola=''#print("Fallo para ", con_esta[i])
    #Renombra el indice de la columna del dataframe de "Estacion" a "Nombre"
    con_esta.rename({'Nombre':'estacion'},inplace=True)
    return con_esta


#Devuelve el Nombre de la estacion, horario y la cantidad de reportes extra respecto a los anteriores, para las estaciones automaticas
def control_reportes_auto(df_nombre_auto):

        df=pd.read_csv(df_nombre_auto)
        df['fecha']=pd.to_datetime(df['fecha']).dt.strftime('%d/%m/%Y')

        #Agregamos el nombre de las estaciones a travez de la funcion "orden_id"
        carga_nombre_estacion=orden_id(df)
        df.insert(3,'Nombre',carga_nombre_estacion)
        
        now = dt.datetime.now()
        fecha = now.strftime("%d/%m/%Y")

        #filtro por fecha actual 
        filtro1=df['fecha'] == fecha
        df_fil=df[filtro1]

        uniestatacion=df_fil['estacion'].unique()

        #Almacena los mensajes de las diferentes estaciones 
        cant_report=""
       
        for i in uniestatacion:

                cant_report1=""
                cant_report2=""
                cant_report3=""
              
                filtro1=df_fil['estacion'] == i
                dfE=df_fil[filtro1]

                filtro1=dfE['hora'] == '7_00'
                df1=dfE[filtro1]

                filtro1=dfE['hora'] == '13_00'
                df2=dfE[filtro1]

                filtro1=dfE['hora'] == '18_00'
                df3=dfE[filtro1]


        
        #Creacion de los diferentes avisos de la cantidad de reportes extra ingresados en Kobo para las estaciones automaticas
        con_estaciones=len(df1['estacion'])
        if (con_estaciones!=1) and (con_estaciones!=0):
                cant_report1='Estacion: '+str(df1['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 7:00 horas: '+str(con_estaciones)+'\n'
                        
        con_estaciones=len(df2['estacion'])
        if (con_estaciones!=1) and (con_estaciones!=0):
                cant_report2='Estacion: '+str(df2['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 13:00 horas: '+str(con_estaciones)+'\n'
                        
        con_estaciones=len(df3['estacion'])
        if (con_estaciones!=1) and (con_estaciones!=0):
                cant_report3='Estacion: '+str(df3['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 18:00 horas: '+str(con_estaciones)+'\n'
        
        cant_report = cant_report1+cant_report2+cant_report3
        return cant_report


#Devuelve el Nombre de la estacion, horario y la cantidad de reportes extra respecto a los anteriores, para las estaciones concencionales y automaticas
def control_variables(df_nombre):
        df=pd.read_csv(df_nombre,usecols=['fecha','hora', 'estacion','Nombre','tseca','tmax','tmin','hum_rel','vel_viento','dir_viento' ],index_col=False)
        df['fecha']=pd.to_datetime(df['fecha']).dt.strftime('%d/%m/%Y')


        now = dt.datetime.now()
        fecha = now.strftime("%d/%m/%Y")
        tiempo=dt.time(now.hour,now.minute)

        #filtro por fecha actual 
        filtro1=df['fecha'] == fecha
        df_fil=df[filtro1]
        uniestatacion=df_fil['estacion'].unique()

        #Almacena los mensajes de las diferentes estaciones 
        mensaje1=""
        mensaje2=""
        mensaje3=""
        mensaje4=''
        mensaje5=''

        for i in uniestatacion:

                mensajetem1=""
                mensajetem2=""
                mensajetem3=""
                
                mensaje_diferencia_min=""
                mensaje_diferencia_max=""
                
                cant_report1=""
                cant_report2=""
                cant_report3=""
                
                mensajehume7=""
                mensajehume13=""
                mensajehume18=""

                mensajedir7=''
                mensajedir13=''
                mensajedir18=''

                mensajevel7=''
                mensajevel13=''
                mensajevel18=''

                filtro1=df_fil['estacion'] == i
                dfE=df_fil[filtro1]

                filtro1=dfE['hora'] == '7_00'
                df1=dfE[filtro1]

                filtro1=dfE['hora'] == '13_00'
                df2=dfE[filtro1]

                filtro1=dfE['hora'] == '18_00'
                df3=dfE[filtro1]

                #Renombre de las diferentes mensajes para el control de las variables para las diferentes estaciones.
                df1=df1.rename(columns={'tseca':'tseco_7:00','hum_rel':'hum_rel_7:00','vel_viento':'vel_viento_7:00','dir_viento':'dir_viento_7:00'})
                df2=df2.rename(columns={'tseca':'tseco_13:00','hum_rel':'hum_rel_13:00','vel_viento':'vel_viento_13:00','dir_viento':'dir_viento_13:00'})
                df3=df3.rename(columns={'tseca':'tseco_18:00','hum_rel':'hum_rel_18:00','vel_viento':'vel_viento_18:00','dir_viento':'dir_viento_18:00'})


                df1=df1.drop(columns={'hora','tmax'})
                df2=df2.drop(columns={'hora','tmin','tmax'})
                df3=df3.drop(columns={'hora','tmin'})


                merged_left=pd.merge(df1,df2,how='outer')
                merged_left=pd.merge(merged_left,df3,how='outer')

                #Eleccion de las diferentes variables para el control de las mismas
                df4=merged_left[['fecha', 'estacion','Nombre','tseco_7:00','tseco_13:00','tseco_18:00','tmax','tmin','hum_rel_7:00','hum_rel_13:00','hum_rel_18:00','dir_viento_7:00','dir_viento_13:00'
                                 ,'dir_viento_18:00','vel_viento_7:00','vel_viento_13:00','vel_viento_18:00' ]]
                df4.to_csv("/home/joshc/prueba_salidas_telegram/"+i+".csv",index=False)
                
                
                if tiempo > dt.time(7,00) and tiempo < dt.time(8,00):

                        #Control de las temperatura minima y temperatura seca 7:00 (tmin > tseco7:00)
                        if ((df4["tseco_7:00"].values[0] < df4["tmin"].values[0])) :
                                mensajetem1="Estacion: "+ str((df4['Nombre'].values[0]))+"\n"+"Tseco_7:00: "+str((df4["tseco_7:00"].values[0]))+"\n"+"Tmin: "+str((df4["tmin"].values[0]))+"\n \n"
                         #Controla la diferencai entre las temperaturas seca 7:00 y la temperatura minima
                        if (df4["tseco_7:00"].values[0] - df4["tmin"].values[0])>=4:
                                mensaje_diferencia_min="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+"Tseco_7:00: "+str(df4["tseco_7:00"].values[0])+"\n"\
                                +"Tmin: "+str(df4["tmin"].values[0])+"\n"+ "Diferencia: "+ str(round((df4["tseco_7:00"].values[0] - df4["tmin"].values[0]),1))+"\n \n"

                        #Control de la Humedad Relativa en los diferentes horarios de medicion      
                        if (df4['hum_rel_7:00'].values[0]<=75):
                                mensajehume7="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Humedad Relativa del reporte de 07:00h es de " + str(df4['hum_rel_7:00'].values[0])+"%"+'\n \n'

                        #Control de la Direccion de viento de las 7:00 horas
                        """if ( (df4['dir_viento_7:00'].values[0] % 5) !=0) and (df4['dir_viento_7:00'].values[0] != np.nan):
                                mensajedir7="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Direccion de viento de las 7:00h es de " + str(df4['dir_viento_7:00'].values[0])+'\n \n'
                        """
                        #Control de la Velocidad de viento de las 7:00 horas
                        if  (df4['vel_viento_7:00'].values[0] >=100):
                                mensajevel7="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Velocidad de viento de las 7:00h es de " + str(df4['vel_viento_7:00'].values[0])+'\n \n'

                      
                if tiempo > dt.time(13,00) and tiempo < dt.time(14,10):
                        #Control de las temperatura seco 13:00 y temperatura seca 7:00 (tseco13 < tseco7:00)
                        if  ((df4["tseco_7:00"].values[0] > df4["tseco_13:00"].values[0])):
                                mensajetem2="Estacion: "+str(df4['Nombre'].values[0])+"\n"+"Tseco_13:00: "+str(df4["tseco_13:00"].values[0])+"\n"+"Tseco_7:00: "+str(df4["tseco_7:00"].values[0])+"\n \n"
                        #Control de la Humedad Relativa en los diferentes horarios de medicion
                        if (df4['hum_rel_13:00'].values[0]<=40):
                                mensajehume13="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Humedad Relativa del reporte de 13:00h es de " + str(df4['hum_rel_13:00'].values[0])+'\n \n'

                        #Control de la Direccion de viento de las 13:00 horas
                        """if ( (df4['dir_viento_13:00'].values[0] % 5) !=0):
                                mensajedir13="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Direccion de viento de las 13:00h es de " + str(df4['dir_viento_13:00'].values[0])+'\n \n'
                        """
                        #Control de la Velocidad de viento de las 13:00 horas
                        if  (df4['vel_viento_13:00'].values[0] >=100):
                                mensajevel13="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Velocidad de viento de las 13:00h es de " + str(df4['vel_viento_13:00'].values[0])+'\n \n'


                if tiempo > dt.time(18,00) and tiempo < dt.time(19,10):

                        #Control de las temperatura seco 13:00, temperatura seca 18:00 y temperatura maxima  (tseco13:00 < tseco18:00) y (tmax < tseco13:00)
                        if ((df4["tseco_13:00"].values[0] <= df4["tseco_18:00"].values[0])) or (df4["tmax"].values[0] < df4["tseco_13:00"].values[0]):
                                mensajetem3="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+"Tmax: "+str(df4["tmax"].values[0])+"\n"+"Tseco_13:00: "+str(df4["tseco_13:00"].values[0])+"\n"+"Tseco_18:00: "+str(df4['tseco_18:00'].values[0])+"\n \n" 
                                                
                        #Controla la diferencai entre las temperaturas seca 13:00 y la temperatura maxima 
                        if (df4["tseco_13:00"].values[0] - df4["tmax"].values[0])>=4:
                                mensaje_diferencia_max="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+"Tseco_13:00: "+str(df4["tseco_13:00"].values[0])+"\n"\
                                        +"Tmax: "+str(df4["tmax"].values[0])+"\n"+ "Diferencia: "+ str(round((df4["tseco_13:00"].values[0] - df4["tmax"].values[0]),1))+"\n \n"
                
                        #Control de la Humedad Relativa en los diferentes horarios de medicion          
                        if (df4['hum_rel_18:00'].values[0]<=60):
                                mensajehume18="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Humedad Relativa del reporte de 18:00h es de " + str(df4['hum_rel_18:00'].values[0])+'\n \n'

                        #Control de la Direccion de viento de las 18:00 horas
                        """if ( (df4['dir_viento_18:00'].values[0] % 5) !=0) and (df4['dir_viento_18:00'].values[0]!=np.nan) :
                                mensajedir18="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Direccion de viento de las 18:00h es de " + str(df4['dir_viento_18:00'].values[0])+'\n \n'
                        """
                        #Control de la Velocidad de viento de las 13:00 horas
                        if  (df4['vel_viento_18:00'].values[0] >=100):
                                mensajevel18="Estacion: "+ str(df4['Nombre'].values[0])+"\n"+ "Velocidad de viento de las 18:00h es de " + str(df4['vel_viento_18:00'].values[0])+'\n \n'
                        
		
        #Control de la cantidad de reportes por estacion en cada horario de medicion
                con_estaciones=len(df1['estacion'])
                if (con_estaciones!=1) and (con_estaciones!=0):
                        cant_report1='Estacion: '+str(df1['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 7:00 horas: '+str(con_estaciones)+'\n \n'
                        
                con_estaciones=len(df2['estacion'])
                if (con_estaciones!=1) and (con_estaciones!=0):
                        cant_report2='Estacion: '+str(df2['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 13:00 horas: '+str(con_estaciones)+'\n \n'
                        
                con_estaciones=len(df3['estacion'])
                if (con_estaciones!=1) and (con_estaciones!=0):
                        cant_report3='Estacion: '+str(df3['Nombre'].values[0])+'\n'+'Cantidad de reportes de las 18:00 horas: '+str(con_estaciones)+'\n \n'
                
                #Mensaje1 = contiene todos los mensajes de control de las temperaturas
                #Mensaje2 = contiene la cantidad de reportes por cada horario de medicion
                #Mensaje3 = Contiene los mensajes de control para la humedad relativa 
                #Mensaje4 = Contiene los mensajes de control para la direccion de viento
                #Mensaje5 = Contiene los mensajes de control para la velocidad de viento
                mensaje1=mensaje1+mensajetem1+mensajetem2+mensajetem3+mensaje_diferencia_min+mensaje_diferencia_max
                mensaje2=mensaje2+cant_report1+cant_report2+cant_report3#+control_reportes_auto(ruta_auto)
                mensaje3=mensaje3+mensajehume7+mensajehume13+mensajehume18
                mensaje4=mensaje4+mensajedir7+mensajedir18+mensajedir13
                mensaje5=mensaje5+mensajevel7+mensajevel18+mensajevel13
        return mensaje1 + mensaje2 + mensaje3 + mensaje4 + mensaje5 


