import warnings, requests, os, time, traceback, re, sched, logging
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

# Esto evita que las respuestas de las API tengan warnings.
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

def switches():
    mydb = mysql.connector.connect(
    host="db",
    user="candelaria",
    password="candelaria",
    database="dcs"
    )
    cursor = mydb.cursor()

    # Realizar una consulta para leer información de la base de datos
    query = "SELECT * FROM dcs.Software_data_switches"
    cursor.execute(query)

    # Obtener los nombres de las columnas
    column_names = [column[0] for column in cursor.description]

    # Convertir los resultados a una lista de diccionarios
    allSwitches = []
    for row in cursor:
        row_dict = {}
        for i in range(len(column_names)):
            row_dict[column_names[i]] = row[i]
        allSwitches.append(row_dict)
        

    print('Inicia bucle de consulta de Switches')
    try:
        for switch in allSwitches:
            ip = switch['ip']
            group = switch['group']
            print(switch['dispositivo'] + ' ' + ip)
            load_dotenv()
            
            # Inicia bloque de consulta a la API PRTG
            URL_PRTG_IP = os.getenv('URL_PRTG_IP').format(ip=ip)
            prtg_ip_response = requests.get(URL_PRTG_IP, verify=False).json()
            if len(prtg_ip_response['devices']) == 0:
                ip=ip,
                name=switch['dispositivo'],
                status='Not Found',
                last_up='Not Found',
                last_down='Not Found',
                
            else:
                id_device_prtg = prtg_ip_response['devices'][0]['objid']
                #! Pendiente definir URL para UPS
                URL_PRTG_ID = os.getenv('URL_PRTG_ID').format(id_device=id_device_prtg)
                prtg_data = requests.get(URL_PRTG_ID, verify=False).json()
                sensor = prtg_data['sensors'][0]
                status = sensor['status']
                name = sensor['device']
                last_up = sensor['lastup']
                last_down = sensor['lastdown']
                
                patron = re.compile(r'<.*?>') # Se usa para formatear el last_up y last_down
                last_up =  re.sub(patron, '', last_up)
                last_down =  re.sub(patron, '', last_down)

            # Inicia bloque para consultar datos a la API CISCO PRIME
            # Si no hay ID se define status_cisco_device como Not Found.
            URL_CISCO_IP_DEVICE = os.getenv('URL_CISCO_IP_DEVICE').format(ip=ip)
            cisco_ip_response = requests.get(URL_CISCO_IP_DEVICE, verify=False).json()
            
            if 'errorDocument' in cisco_ip_response or cisco_ip_response['queryResponse']["@count"] == 0:
                print('Entro al condicional del errorDocument')
                status_cisco_device = 'Not Found'
                
            else:
                cisco_id_client = cisco_ip_response['queryResponse']['entityId'][0]['$']
                URL_CISCO_ID_DEVICE = os.getenv('URL_CISCO_ID_DEVICE').format(id_device=cisco_id_client)
                cisco_data_device = requests.get(URL_CISCO_ID_DEVICE, verify=False).json()
                status_cisco_device = cisco_data_device['queryResponse']['entity'][0]['devicesDTO']['reachability']
                
            # sql = "INSERT INTO Software_cnp (`group`, name, importancia, clave, description, ip, id_prtg, id_cisco) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            sql = "INSERT INTO Software_switches (dispositivo, ip, tipo, status_prtg, lastup_prtg, lastdown_prtg, reachability, ups1, ups2, status_ups1, status_ups2, `group`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (name, ip, 'Switch', status, last_up, last_down, status_cisco_device, 'NA', 'NA', 'NA', 'NA', group)
            cursor.execute(sql, val)

            mydb.commit()
            
        now = datetime.now()
        fecha_y_hora = now.strftime("%Y-%m-%d %H:%M:%S")

        sql_datetime = "INSERT INTO Software_fechas_consultas_switches (ultima_consulta) VALUES (%s)"
        val_datetime = (fecha_y_hora,)
        cursor.execute(sql_datetime, val_datetime)
        mydb.commit()   
        cursor.close()
        mydb.close()
        
        with open("/app/Logs/switches.txt", "a") as archivo:
            archivo.write(str(fecha_y_hora) + '\n')
        print('Terminado con exito 2.0')
            
    except Exception:
        now = datetime.now()
        fecha_y_hora = now.strftime("%Y-%m-%d %H:%M:%S")
        fecha_y_hora = str(fecha_y_hora)
        with open("/app/Logs/switches.txt", "a") as archivo:
            archivo.write('Fecha y hora del error: ' + str(fecha_y_hora) + ' Dispositivo del error ---> ' + str(ip) + '\n')
            archivo.write(traceback.format_exc())
            archivo.write("\n")
        


def bucle(scheduler):
    switches()
    scheduler.enter(10, 1, bucle, (scheduler,))

if __name__ == '__main__':
    s = sched.scheduler(time.time, time.sleep)
    s.enter(0, 1, bucle, (s,))
    s.run()


