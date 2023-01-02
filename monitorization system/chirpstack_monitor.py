from paho.mqtt import client as mqtt_client
import requests
import json
import pandas as pd
import numpy as np
import random
import time
from time import sleep
from datetime import datetime


#HTTP Hearders
headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    }

#New Session
s = requests.Session()

#Send a ping if the script is running

#This funtion will authenticate us inside the ChirpStack API
def get_jwt():
    #Credentials
    data = '{"email": "admin", "password": "admin"}'
    #Petition with the credentials
    response = requests.post('http://158.49.112.171:8081/api/internal/login', headers=headers, data=data)
    #Saving the answer and parse it into a JSON
    response_data = json.loads(response.content)
    #Analyze the result
    if (response_data.get('jwt')):
        headers['Authorization'] = response_data.get('jwt')
        print('login success!!')
        return True
    else:
        print("Error Login")
        return False


#In this function we are gonna ask and process the data asked to the ChirpStack API
def stream_data(client):

        #We are gonna do this petition to know how many devices are saved inside the cloud
        get_app=s.get('http://158.49.112.171:8081/api/devices', headers=headers, stream=True)
        data2 = json.loads(str(get_app.content.decode("utf-8")))  # total de sensores
        totalcount= data2['totalCount']
        #totalcount= '30' This is for debugging bc already are 283 devices

        #Now are gonna do the request to know all the data of all the devices
        url='http://158.49.112.171:8081/api/devices?limit='+ totalcount
        get_app2=s.get(url=url, headers=headers, stream=True)
        data = json.loads(str(get_app2.content.decode("utf-8")))  # recogida mensaje
       
       #Save it into an array
        result_list= data ['result']
    





        index=0


        #Obtain the data of each of them and save it into an array for
        #process and detect the ones who are Inactive
        name_array=[d['name'] for d in result_list]
        lastSeenAt_array=[d['lastSeenAt'] for d in result_list]
        applicationID_array=[d['applicationID'] for d in result_list]
        
        #Lógica para sacar los parámetros de control de cada sensor
        for x in lastSeenAt_array:
            if x is not None:

                #Parse the date format to one we can calculate the period with
                time1=datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ")
                timenow1= datetime.now()
                period=timenow1-time1

                #Calculate the period in seconds to detect when the is an anomalie
                seconds_in_day = 24 * 60 * 60
                period2= divmod(period.days * seconds_in_day + period.seconds, 60)
                period_minutes=period2[0]
                period_seconds=period2[1]
                print('periodo segundos' )
                period_total=period_minutes*60+period_seconds
                print(period_total)


                #Here there is the trigger to detect when is a 
                #device considered Inactive
                if period_minutes > 3600:

                    #Create the JSON  with the data of the sensor
                    anomalie = {
                        "where": "chirpstarck",
                        "name": name_array[index],
                        "aplicationid": applicationID_array[index],
                        "lastseen":  datetime.strftime(time1,"%Y-%m-%d %H:%M:%S.%f"),
                        "period": period_minutes,
                        "anomalies": "inactive"
                     }

                    jsonanomalie = json.dumps(anomalie)
                    print(jsonanomalie)

                    #Publish the data of the sensor
                    publish(client, jsonanomalie)
                    

                

            #     else : 
            #         if period_minutes > 40 and period_minutes <60:
            #             anomalies_array.append('3')
            #         else:
            #             if period_minutes > 10 and period_minutes < 40:
            #                 anomalies_array.append('2')
                            
            #             else:
            #                 if period_minutes > 5 and period_minutes < 10:
            #                     anomalies_array.append('1')
            #                 else:
            #                     anomalies_array.append('Active')

            # else:
            #     lastSeenAt_array2.append('Never')
            #     period_array.append('-')
            #     anomalies_array.append('-')

            index=index+1


        # print(name_array)       
        # table={}
        # table['Name']= name_array
        # table['ApplicationID']= applicationID_array
        # table['Last Seen']= lastSeenAt_array2
        # table['Period (Minutes)']= period_array
        # table['Anomalies']= anomalies_array


        #Desconect from the broker
        client.disconnect()


        # final_table=pd.DataFrame(table)
        # return final_table.to_csv('repository_chirpstack.csv')                



#MQTT Parameters
#
broker = "10.253.247.201"
port = 1883
topic = '/anomalies/'
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
#username = 'monitor'
#password = 'control'
#
#MQTT FUNCTIONS
#
#With this function we are gonna achieve to send a ping everytime the script is executed
def publishprocesses(client):
    msg_count = 0
    msg1 = f"messages: {msg_count}"
    lastseen=datetime.now()
    
    ping = {
            "where": "chirpstack",
            "lastseen": datetime.strftime(lastseen,"%Y-%m-%d %H:%M:%S.%f"),
            }

    msg = json.dumps(ping)
    result = client.publish("/pingprocesses/", msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg1}`º ping: `{msg}`")
    else:
        print(f"Failed to send message to topic {topic}")
    msg_count += 1
                
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client, jsonanomalie):
    msg_count = 0
    msg1 = f"messages: {msg_count}"
    msg = jsonanomalie
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg1}`º anomalie: `{msg}`")
    else:
        print(f"Failed to send message to topic {topic}")
    msg_count += 1


def main():
    client = connect_mqtt()
    publishprocesses(client)
    get_jwt()
    stream_data(client)
    client.loop_start()

    

if __name__ == '__main__':

    while True:
        main()
        sleep(1800)
        


