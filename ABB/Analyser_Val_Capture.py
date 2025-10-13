import csv
from datetime import datetime
import sys
sys.path.append('..')
from pylogix import PLC
from time import sleep
def Val():
    tag_list = ['ST01_LVL','ST01_PRESS','ST02_LVL','ST02_PRESS','PV_LVL','PV_PRESS'
           ]
    with PLC() as comm:
    comm.Micro800 = True
    comm.IPAddress = '192.168.43.226'
    ret = comm.Read(tag_list)
    l = []
    for r in ret:
        a = r.Value
        a=float(a)           
        l.append(a)
 
        print(l)
    return(l)


def CSV():
 
   l=Val()
   now = datetime.now()
   date_time = now.strftime("%d/%m/%y,%H:%M:%S")
   field_names = ['NAME', 'LEVEL','PRESSURE','TEMP', 'TIME']
   VAL = [
         {'NAME': 'STORAGE_TANK1', 'LEVEL': l[0],'PRESSURE': l[1],'TEMP': 0.0, 'TIME': now.strftime("%d/%m/%y,%H:%M:%S")},
         {'NAME': 'STORAGE_TANK2','LEVEL': l[2],'PRESSURE': l[3],'TEMP': 0.0, 'TIME': now.strftime("%d/%m/%y,%H:%M:%S")},
         {'NAME': 'PV_TANK','LEVEL': l[4],'PRESSURE': l[5],'TEMP': 0.0, 'TIME': now.strftime("%d/%m/%y,%H:%M:%S")}]     
   with open('GKN.csv', 'w') as csvfile:
      writer = csv.DictWriter(csvfile, fieldnames=field_names)
      writer.writeheader()
      writer.writerows(VAL) 
      print('writeen') 
      return()

 
while True:
    while 1:
         try:
             CSV()
             sleep(30)
         except Exception as e:
             print('Error:',e )
             sleep(3)
 
  
     
