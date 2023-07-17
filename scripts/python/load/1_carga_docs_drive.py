from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import time
import os

os.system("cd /home/joshc/automatizacion_kobo")
def  carga_automatica():
    #print("ya espera la creacion de los documentos")
    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    #Para reemplazar se debe de extraer el código de identificación 
    file2 = drive.CreateFile({'id': '1tRXZkzLgQJ36xkuMItTEadh0HYrYrxy1'})
    file2.SetContentFile('documento_drive.csv')
    file2.Upload()

    file3 = drive.CreateFile({'id': '1oBHf4cmrQS-wE6ovY3ZeDYMler4-BVNK'})
    file3.SetContentFile('Fueraderango.xlsx')
    file3.Upload() 


    file4 = drive.CreateFile({'id': '1bnoGT30P1J-OG_-2tMlZw3-o_eMRu5yN'})
    file4.SetContentFile('documento_drive.xlsx')
    file4.Upload() 

    file5 = drive.CreateFile({'id': '19gcM1e5rb-HvJ-MVhNSZgsinNhN0S79Y'})
    file5.SetContentFile('data_procesada.csv')
    file5.Upload()

    file6 = drive.CreateFile({'id': '13YXOl9K1yVtaUco7oUbC00zso4sSv4jz'})
    file6.SetContentFile('ICC.csv')
    file6.Upload()                                          
                                          
