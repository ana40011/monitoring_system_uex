# python 3.6
from datetime import datetime
import paho.mqtt.client as mqtt  # import the client1
import json
import random
import numpy as np
from numpy import var




devices={}
lastexecution= datetime.now()


#MQTT Parameters for subscribing
broker_address = "158.49.112.171"
topic1 = 'application/#'
client = mqtt.Client('monitoring-m2qtt')

#This function is called everytime an anomalie is wanted to be reported
def sendanomalie(dict):
    #Convert the datetime into a string to be able to read it
    lastseen=datetime.strftime(dict['lastseen'], "%Y-%m-%d %H:%M:%S.%f")

    #Create the JSON object
    anomalie = {
                        "where": "mqtt",
                        "devicename": dict['name'],
                        "aplicationid": dict['applicationid'],
                        "lastseen": lastseen,
                        "period": dict['periodseconds'],
                        "anomalies": "inactive",
                     }
            
    jsonanomalie = json.dumps(anomalie)
    print(jsonanomalie)
    clientpublish=connect_mqtt_publish()
    #Publish the anomalie into the broker
    publish(clientpublish, jsonanomalie)



##Because is inside a loop the function sleep doesnt work
# and the control must be done manually 

#The Timer will be executed everytime a messsage is recieved

#This var will be called in the main and is the one used for 
#decide how much time will be btw check all the devides
everytimeseconds=3600

def manualtimer(everytimeseconds):
    #This var will be called in the main and is the one used for 
    #decide how much time will be btw check all the devices
    timenow= datetime.now()
    #calculate the period
    periodcal=timenow-lastexecution
    seconds_in_day = 24 * 60 * 60
    period2= divmod(periodcal.days * seconds_in_day + periodcal.seconds,60)
    period_minutes=period2[0]
    period_seconds=period2[1]
    period_total=period_minutes*60+period_seconds

    if period_total % everytimeseconds == 0:
        checkall()



# This function is created to detect the inactive sensors which have been
# without sending data for more than a hour
def checkall():

    for x in devices.keys():

        lastseen_stored=devices[x]["lastseen"]
        #we need to put it in string and later transform to datetime
        time2=datetime.strftime(lastseen_stored, "%Y-%m-%d %H:%M:%S.%f")
        time1=datetime.strptime(time2, "%Y-%m-%d %H:%M:%S.%f")
        timenow1= datetime.now()

        #calculate the period
        periodcal=timenow1-time1
        seconds_in_day = 24 * 60 * 60
        period2= divmod(periodcal.days * seconds_in_day + periodcal.seconds,60)
        period_minutes=period2[0]
        period_seconds=period2[1]
        period_total=period_minutes*60+period_seconds

        if period_total > 3600:
            sendanomalie(devices[x])
            print('Check all function')





#MQTT FUNCTIONS


def on_message(client, userdata, message):

    manualtimer(10)
    #To send a ping of the process is still alive
    clientprocesses=connect_mqtt_publish()
    publishprocesses(clientprocesses)

    print("in-message")
    #read the data of mqtt message
    data = json.loads(str(message.payload.decode("utf-8"))) 
    #print(data)
    print([data['deviceName']])

    #Save the parameters which are useful 
    deviceName = str(data['deviceName']) 
    deviceEUI= str(data['devEUI']) 
    applicationID=data['applicationID']

    #Check if the dictionary is empty
    if bool(devices) is False:   
        lastseen= datetime.now()
        period_without= '-'
        anomalies_withot='-'

        #If is empty, save it 
        devices[deviceEUI]= {'name': deviceName,'applicationid' : applicationID ,'lastseen':lastseen , 'periodseconds': period_without , 'measures': 1,'average':0,'variances':[]}
    
    
    #If the dictionary already got some devices saved
    else:

        #Check if the device is registered by the device EUI
        registrado=0

        for x in devices.keys():
            if ((x == deviceEUI)):
                print(x)
                registrado=1



        #In case is registered
        if registrado == 1:
            device=devices[deviceEUI]
            #Check when was the last time the sensor sent data and obtain the period
            #in seconds
            lastseen_stored=devices[deviceEUI]["lastseen"]
            #we need to put it in string and later transform to datetime
            time2=datetime.strftime(lastseen_stored, "%Y-%m-%d %H:%M:%S.%f")
            time1=datetime.strptime(time2, "%Y-%m-%d %H:%M:%S.%f")
            timenow1= datetime.now()
            print(devices)
            #calculate the period
            periodcal=timenow1-time1
            seconds_in_day = 24 * 60 * 60
            period2= divmod(periodcal.days * seconds_in_day + periodcal.seconds,60)
            period_minutes=period2[0]
            period_seconds=period2[1]
            period_total=period_minutes*60+period_seconds

            #Save the anomalies number to use it to calculate the new media
            nmedidas=devices[deviceEUI]["measures"]

            #obtaining the values for 
            media1=devices[deviceEUI]["average"]
            variances=devices[deviceEUI]["variances"]
            empty=[[]]
            media2= period_total

            if media1 == 0 :
                device.update({"average": media2})
            

            #This condition is to have a mínimum of legal variances
            if len(variances) <= 10:
                 #We have add this condition in order not break the average if the period is too big
                if period_total < 3600:
                    #Calculate the regresive average and store the variances 
                    now_var=abs(media1-period_total)
                    #Calculate the regresive average 
                    media2=(media1*(nmedidas)+period_total)/(nmedidas+1)
                    time2=datetime.strftime(lastseen_stored, "%Y-%m-%d %H:%M:%S.%f")
                    device.update({"average": media2})
                    #To don't have the array with the first element like this []
                    if variances==empty:
                            variances = variances.pop(0)

                    variances.append(now_var)
                    device.update({"variances": variances})

                else:
                    sendanomalie(devices[deviceEUI])

                
            else:
                #We have add this condition in order not break the average if the period is too big
                if period_total < media1*15:
                    #Take the new variance to add it to the array
                    now_var=abs(media1-period_total)
                    #remove the first element if the array
                    variances = variances.pop(0)
                    #add to the end the new one
                    variances.append(now_var)
                    #save the new variances array
                    device.update({"variances": variances})
                    #calculate the typical desviation to identify the anomalies
                    desv=np.sqrt(var(variances))

                    #sending an anomalie everythime is out of the bounds
                    if period_total>desv+media1 or period_total<media1-desv:
                        sendanomalie(devices[deviceEUI])


                    #Calculate the regresive average 
                    media2=(media1*(nmedidas-1)+period_total)/nmedidas
                    device.update({"average": media2})
                
                else:
                    sendanomalie(devices[deviceEUI])
                

            nmedidas=nmedidas+1

            device.update({"lastseen": timenow1})
            device.update({"periodseconds": period_total})
            device.update({"measures": nmedidas})

            #record the data changes in the dictionary
            devices.update({deviceEUI:device})
           

        else:
            #This happends when the device is not registered but the dict exits
            deviceName=data['deviceName']
            applicationID=data['applicationID']
            lastseen= datetime.now()
            period_without= '-'

            devices[deviceEUI]= {'name': deviceName,'applicationid' : applicationID ,'lastseen':lastseen , 'periodseconds': period_without ,'measures': 1,'average':0,'variances':[]}
    
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_connect(client, userdata, flags, rc):
    if rc!=0:
        print("Bad connection Returned code=",rc)

def connect_mqtt_publish():

    broker = "10.253.247.201"
    port = 1883
    # generate client ID with pub prefix randomly
    client_id = f'python-mqtt-{random.randint(0, 100)}'
    username = 'monitor'
    password = 'control'
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client, jsonanomalie):
    msg_count = 0
    topic = '/anomalies/'
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

#With this function we are gonna achieve to send a ping everytime the script is executed
def publishprocesses(client):
    msg_count = 0
    msg1 = f"messages: {msg_count}"
    lastseen=datetime.now()
    
    ping = {
            "where": "mqtt",
            "lastseen": datetime.strftime(lastseen,"%Y-%m-%d %H:%M:%S.%f"),
            }

    msg = json.dumps(ping)
    result = client.publish("/pingprocesses/", msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg1}`º ping: `{msg}`")
    else:
        print(f"Failed to send message to topic pingprocesses")
    msg_count += 1







def run():    
    client.username_pw_set(username='persianas', password='opticalflow')
    print("pre-on-message")
    client.on_message = on_message
    client.on_connect=on_connect 
    client.on_subscribe= on_subscribe
    client.connect(broker_address)
    client.subscribe(topic1, qos=1)
    print("pre-loop")
    client.loop_forever()
    

   

if __name__ == '__main__':
    run()

