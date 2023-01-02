import paho.mqtt.client as mqtt  # import the client1
import docker
from datetime import datetime
import json
import random
from time import sleep

#Connect to docker to obtain the data of 
client_docker = docker.DockerClient(base_url='unix:///var/run/docker.sock')

containers={}
#
# 
# LOGIC FUNCTIONS
# 
#This fuction is for calculates and retrieves the cpu and memory of the containers
def containers_stadictics(client):
    print(containers)

    for i in client_docker.containers.list():
        container_stats= i.stats(stream=False)

        #Control the memory AND CPU usage and send anomalie
        print(container_stats["name"])        
        container_memory=container_stats["memory_stats"]
        container_cpu=container_stats["cpu_stats"]
        container_cpu_current=container_cpu["cpu_usage"]
        id=container_stats["id"]
        try:
            container_memory_perc=container_memory["usage"]*100/container_memory["limit"]
        except:
            container_cpu_perc="MEMORY PROBLEM"
            container=save_containers((container_stats["id"]),(container_stats["name"]),container_cpu_perc,container_memory_perc)
            sendanomalie(container,client,id) 

        try:   
            container_cpu_perc=container_cpu_current["total_usage"]*100/container_cpu["system_cpu_usage"]

            if container_cpu_perc > 75 or container_memory_perc >75:
                container=save_containers(container_stats["id"],container_stats["name"],container_cpu_perc,container_memory_perc)
                container_cpu_perc="CPU OR MEMORY PROBLEM"
                sendanomalie(container,client,id)
                print("CPU OR MEMORY PROBLEM")
                print(container_cpu_perc)
            else:
                container=save_containers((container_stats["id"]),(container_stats["name"]),container_cpu_perc,container_memory_perc)


        except:
            container_cpu_perc="CPU PROBLEM"
            container=save_containers((container_stats["id"]),(container_stats["name"]),container_cpu_perc,container_memory_perc)
            sendanomalie(container,client,id) 





#Save the data of the dictionaries and return the new item saved
def save_containers(id, name, cpu, memory):

    #If the container is emputy bevause the script just restart
    if bool(containers) is False:   
        containers[id]= { "Name":name, "CPU (%)":cpu,"Memory (%)":memory, "lastseen":datetime.now(),"period": "-"}

    else:
        registrado=0
        #Check if the container is registered by ID
        for x in containers.keys():
            if (x == id):
                print(x)
                registrado=1
                print(registrado)
                print("REGISTRADO")
            

        print(id)
        if registrado==1:
            container=containers[id]
            print(container)
            container["CPU (%)"]=cpu
            container["Memory (%)"]=memory
            print("TRUE")
            #Calculate the period
            lastseenstored=container["lastseen"]
            periodcal=datetime.now()-lastseenstored
            seconds_in_day = 24 * 60 * 60
            period2= divmod(periodcal.days * seconds_in_day + periodcal.seconds,60)
            period_minutes=period2[0]
            period_seconds=period2[1]
            period_total=period_minutes*60+period_seconds
            container["period"]=period_total
            container["lastseen"]=datetime.now()

            containers[id]=container

        else:
            containers[id]= { "Name":name, "CPU (%)":cpu,"Memory (%)":memory, "lastseen":datetime.now(), "period": "-"}

    return(containers[id])


#This function is called everytime an anomalie is wanted to be reported
def sendanomalie(dict, clientpublish,id):
    #Convert the datetime into a string to be able to read it
    lastseen=datetime.strftime(dict['lastseen'], "%Y-%m-%d %H:%M:%S.%f")

    #Create the JSON object
    anomalie = {
                        "where": "docker_containers",
                        "devicename": dict['Name'],
                        "aplicationid": id,
                        "lastseen": lastseen,
                        "period": dict['period'],
                        "anomalies": "inactive",
                     }
            
    jsonanomalie = json.dumps(anomalie)
    print(jsonanomalie)
    #Publish the anomalie into the broker
    publishanomalies(clientpublish, jsonanomalie)
#
#
#
#MQTT FUNCTIONS
#
# 
#Function to connect with the  topic
def connect_mqtt():
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

#Function to publish the anomalies
def publishanomalies(client, jsonanomalie):
    msg_count = 0
    msg1 = f"messages: {msg_count}"
    msg = jsonanomalie
    result = client.publish('/anomalies/', msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg1}`º anomalie: `{msg}`")
    else:
        print(f"Failed to send message to topic {topic}")
    msg_count += 1

#Function to publish the pings still alive of the script
def publishprocesses(client):
    msg_count = 0
    msg1 = f"messages: {msg_count}"
    lastseen=datetime.now()
    
    ping = {
            "where": "docker_containers",
            "lastseen": datetime.strftime(lastseen,"%Y-%m-%d %H:%M:%S.%f"),
            }

    msg = json.dumps(ping)
    result = client.publish("/pingprocesses/", msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg1}`º anomalie: `{msg}`")
    else:
        print(f"Failed to send message to topic pingprocesses")
    msg_count += 1
          
#
#
#
# RUNNING FUNCTIONS
# 
def run():    
    client_mqtt=connect_mqtt()
    print("pre-loop")
    containers_stadictics(client_mqtt)
    publishprocesses(client_mqtt)

if __name__ == '__main__':
    while True:
        run()
        sleep(5)
        








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


lastexecution= datetime.now()

# This function is created to detect the inactive sensors which have been
# without sending data for more than a hour
def checkall():

    for x in containers.keys():

        lastseen_stored=containers[x]["lastseen"]
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
            sendanomalie(containers[x])
            print('Check all function')









     
     