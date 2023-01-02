# python3.6
import json
import random
from pysondb import db
from paho.mqtt import client as mqtt_client

database_file=db.getDb("db.json")

broker = "10.253.247.201"
port = 1883
topic = '/anomalies/'
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
client = mqtt_client.Client(client_id)



def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client.on_connect = on_connect
    client.connect(broker, port)
    client.subscribe(topic)

    return client



def on_message(client, userdata, msg):
    anomalie= json.loads(str(msg.payload.decode("utf-8")))
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    #with open('db.txt', 'a') as f:
    #   f.write(msg.payload.decode()+'\n')
    
    q={"devicename": anomalie['devicename']}

    queryresponse=database_file.getByQuery(query=q)
    #We use two if instead of an if-else to control better the data that is ebing indexed to the database
    if not queryresponse:
        database_file.add(anomalie)
    if queryresponse:
        update={"devicename": anomalie['devicename'], "lastseen": anomalie['lastseen']}
        database_file.updateByQuery(db_dataset=q, new_dataset=update)



def run():
    client = connect_mqtt()
    client.on_message = on_message
    client.loop_forever()


if __name__ == '__main__':
    run()
