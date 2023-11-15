# -*- coding: utf-8 -*-

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime as date
import os
from dotenv import load_dotenv
load_dotenv()
d=date.now()
FECHA = d.strftime("%Y-%m-%d %H:%M:%S")

# Datos del remitente
correo_remitente = 'hasagastume@insivumeh.gob.gt'

password_remitente = os.getenv("PASSWORD")

# Datos del destinatario

toaddrs=['insivumehconsultoriaciat@gmail.com']


for i in range(len(toaddrs)):
    # Crear mensaje
    mensaje = MIMEMultipart()
    mensaje['From'] = correo_remitente
    mensaje['To'] = toaddrs[i]
    mensaje['Subject'] = 'Datos diarios Climatologia '+FECHA
    # Agregar cuerpo del mensaje
    cuerpo = MIMEText('Buen día, comparto los datos diarios de las estaciones convencionales de la sección de Climatología hasta el '+FECHA+'.' 'Este es un correo automático, por favor no contestar.')
    mensaje.attach(cuerpo)

    # Agregar archivo CSV adjunto 1
    #archivo_csv = MIMEApplication(open('/home/clima/automatizacion_kobo/datos-diarios_prelim_temp-celsi.csv', 'rb').read())
    #archivo_csv.add_header('Content-Disposition', 'attachment', filename='datos-diarios_prelim_temp-celsi.csv')
    #mensaje.attach(archivo_csv)

    # Agregar archivo CSV adjunto 2
    #archivo_csv2 = MIMEApplication(open('/home/clima/automatizacion_kobo/datos-diarios_prelim__prep-milim.csv', 'rb').read())
    #archivo_csv2.add_header('Content-Disposition', 'attachment', filename='datos-diarios_prelim__prep-milim.csv')
    #mensaje.attach(archivo_csv2)
    
    # Agregar archivo CSV adjunto 3
    archivo_csv3 = MIMEApplication(open('/home/clima/salida_auto_kobo/salida_interna_diaria.csv', 'rb').read())
    archivo_csv3.add_header('Content-Disposition', 'attachment', filename='datos_diarios_Climatologia.csv')
    mensaje.attach(archivo_csv3)


    # Enviar correo
    servidor_smtp = smtplib.SMTP('smtp.gmail.com', 587)
    servidor_smtp.starttls()
    servidor_smtp.login(correo_remitente, password_remitente)
    #servidor_smtp.sendmail(correo_remitente, correo_destinatario, mensaje.as_string())
    servidor_smtp.sendmail(correo_remitente, toaddrs[i], mensaje.as_string())
    servidor_smtp.quit()

    print("proceso finalizado")

