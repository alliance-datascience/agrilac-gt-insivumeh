#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

#escriba los archivos que desea subir
#upload_file_list = ['documento_drive.csv','Fueraderango.xlsx','documento_drive.xlsx','data_procesada.csv']
#upload_file_list = ['C.C_MODIFICACIÒN_DE_DATOS.xlsx']
upload_file_list=['Estándar_de_estaciones_INSIVUMEH.xlsx']
for upload_file in upload_file_list:
	#gfile = drive.CreateFile({'parents': [{'id': ''}]}) #id is folder link google drive
	gfile = drive.CreateFile({'parents': [{'id': ''}]}) #id is folder link google drive
	# Read file and set it as the content of this instance.
	gfile.SetContentFile(upload_file)
	gfile.Upload() # Upload the file.
