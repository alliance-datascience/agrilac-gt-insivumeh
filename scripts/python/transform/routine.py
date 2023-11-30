#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import time
import os
import pandas as pd

def  carga_automatica():
    #print("ya espera la creacion de los documentos")
    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    #Para reemplazar se debe de extraer el código de identificación 
    file2 = drive.CreateFile({'id': ''})
    file2.SetContentFile('documento_drive.csv')
    file2.Upload()

    file3 = drive.CreateFile({'id': ''})
    file3.SetContentFile('Fueraderango.xlsx')
    file3.Upload() 


    file4 = drive.CreateFile({'id': ''})
    file4.SetContentFile('documento_drive.xlsx')
    file4.Upload() 



    file7 = drive.CreateFile({'id': ''})
    file7.GetContentFile('C.C_MODIFICACIÒN_DE_DATOS.xlsx')  

    file8 = drive.CreateFile({'id': ''})
    file8.GetContentFile('DB_CLIMA_C.C_MODIFICACIÒN_DE_DATOS.xlsx') 





    df=pd.read_excel('/C.C_MODIFICACIÒN_DE_DATOS.xlsx', index_col=False)
    df.to_csv('C.C_MODIFICACIÒN_DE_DATOS.csv', index=False)
    os.system(' C.C_MODIFICACIÒN_DE_DATOS.xlsx') 
                                            
                                        
    df2=pd.read_excel('DB_CLIMA_C.C_MODIFICACIÒN_DE_DATOS.xlsx', index_col=False)
    df2.to_csv('DB_CLIMA_C.C_MODIFICACIÒN_DE_DATOS.csv', index=False)
    os.system('DB_CLIMA_C.C_MODIFICACIÒN_DE_DATOS.xlsx') 



