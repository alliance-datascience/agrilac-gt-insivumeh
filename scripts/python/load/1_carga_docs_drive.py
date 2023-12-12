from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import time
import os

os.system(")
def  carga_automatica():
    #print("ya espera la creacion de los documentos")
    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    #Para reemplazar se debe de extraer el código de identificación 
    file2 = drive.CreateFile({'id': })
    file2.SetContentFile('documento_drive.csv')
    file2.Upload()

    file3 = drive.CreateFile({'id': })
    file3.SetContentFile('Fueraderango.xlsx')
    file3.Upload() 


    file4 = drive.CreateFile({'id': })
    file4.SetContentFile('documento_drive.xlsx')
    file4.Upload() 

    file5 = drive.CreateFile({'id': })
    file5.SetContentFile('data_procesada.csv')
    file5.Upload()

    file6 = drive.CreateFile({'id': })
    file6.SetContentFile('ICC.csv')
    file6.Upload()                                          
                                          
