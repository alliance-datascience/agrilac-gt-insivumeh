import numpy as np
import pandas as pd
import datetime as dt
import time
#ruta_documento="/home/joshc/upload_google_drive/"
#ruta="/home/joshc/automatizacion_kobo/"

now = dt.datetime.now()
fecha = now.strftime("%d/%m/%Y")

def myfunc(curr):
  if curr in ('lluvia','tmax','tmin','tseca','bri_solar','rad_solar','eva_tan','eva_piche','nub','dir_viento',
                                                               'vel_viento','hum_rel','pre_atmos','tsuelo_100'):
    return "0"+curr
  else:
    return curr



def orden_columnas(ruta_documento,ruta_contenedora):

    docrango= pd.read_excel(ruta_documento+"Fueraderango.xlsx")
    docdrive = pd.read_csv(ruta_documento+'documento_drive.csv')
    #docdrive=docdrive.sort_values(by="Fecha")




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

