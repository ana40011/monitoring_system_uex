# python 3.6
from datetime import datetime
import paho.mqtt.client as mqtt  # import the client1
import json
import random
import numpy as np
from numpy import var




processes={}
lastexecution= datetime.now()


#MQTT Parameters for subscribing
broker_address = "10.253.247.201"
topic1 = '/pingprocesses/'
client = mqtt.Client('monitoring-mqtt3')


#This function is called everytime an anomalie is wanted to be reported
def sendanomalie(dict):
    #Convert the datetime into a string to be able to read it
    lastseen=datetime.strftime(dict['lastseen'], "%Y-%m-%d %H:%M:%S.%f")

    #Create the JSON object
    anomalie = {
                        "where": "processes",
                        "devicename": dict['name'],
                        "aplicationid": "-",
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

    for x in processes.keys():

        lastseen_stored=processes[x]["lastseen"]
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
            sendanomalie(processes[x])
            print('Check all function')





#MQTT FUNCTIONS


def on_message(client, userdata, message):

    manualtimer(10)


    print("in-message")
    #read the data of mqtt message
    data = json.loads(str(message.payload.decode("utf-8"))) 
    #print(data)
    print([data['where']])

    #Save the parameters which are useful 
    processName = str(data['where']) 

    #Check if the dictionary is empty
    if bool(processes) is False:   
        lastseen= datetime.now()    
        period_without= '-'
        anomalies_withot='-'

        #If is empty, save it 
        processes[processName]= {'name': processName,'applicationid' : "-" ,'lastseen':lastseen , 'periodseconds': period_without , 'measures': 1,'average':0}
    
    
    #If the dictionary already got some devices saved
    else:

        #Check if the device is registered by the device EUI
        registrado=0

        for x in processes.keys():
            if ((x == processName)):
                print(x)
                registrado=1



        #In case is registered
        if registrado == 1:
            process=processes[processName]
            #Check when was the last time the sensor sent data and obtain the period
            #in seconds
            lastseen_stored=processes[processName]["lastseen"]
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
            print(period_total)
            #Save the anomalies number to use it to calculate the new media
            nmedidas=processes[processName]["measures"]
            
            

                
            
            #We have add this condition in order not break the average if the period is too big
            if period_total > 0:
                sendanomalie(processes[processName])

                
            nmedidas=nmedidas+1

            process.update({"lastseen": timenow1})
            process.update({"periodseconds": period_total})
            process.update({"measures": nmedidas})

            #record the data changes in the dictionary
            processes.update({processName:process})

           

        else:
            #This happends when the device is not registered but the dict exits
            processName=data['where']
            applicationID="-"
            lastseen= datetime.now()
            period_without= '-'

            processes[processName]= {'name': processName,'applicationid' : applicationID ,'lastseen':lastseen , 'periodseconds': period_without ,'measures': 1}
    
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

