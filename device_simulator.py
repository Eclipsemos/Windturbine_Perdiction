import boto3
import time
import json
import argparse
import pandas as pd
import numpy as np
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# default
global control
control = "on"


# Close the device
def customCallback(client, userdata, message):
    jsonState = json.loads(message.payload)
    state = jsonState ['state'] 
    desired = state ['desired']
    switch = desired['switch']


    if switch == "off":
        print("  ")
        print("  ")
        print("*******************************************************************")
        print("Machine Learning Predicted Failure...")
        print("Initiating turbine shutdown ....\n")
        print("Turbine shutdown completed...")
        print("*******************************************************************")
        print("  ")
        print("  ")

        global control
        control = "off"

    if switch == "on":
        print("Activating turbine...\n....\n")
        time.sleep(1)
        print("Starting to sent telemetry data to AWS IoT...\n"  )       
        control = "on"

# Cert
host = "ai67dyh51qewl-ats.iot.us-east-1.amazonaws.com"
rootCAPath = "cert/AmazonRootCA1.pem" 
certificatePath = "cert/4263533e2bbe8d46b931ce7d8983a90c642b74656a9174f94ab0789d0aec5380-certificate.pem.crt"
privateKeyPath = "cert/4263533e2bbe8d46b931ce7d8983a90c642b74656a9174f94ab0789d0aec5380-private.pem.key" 


print(" ")
print("*************************************")
print("Activating turbines...\n")
print(" ")
time.sleep(2)
print("Starting to sent telemetry to AWS IoT...")
print("*************************************")
print(" ")

time.sleep(2)

# Initialize IoT Client
myAWSIoTMQTTClient = None

myAWSIoTMQTTClient = AWSIoTMQTTClient("windturbine/xgboost")
myAWSIoTMQTTClient.configureEndpoint(host, 8883)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# Connect IoT Core
myAWSIoTMQTTClient.connect()

# subscribe shadow customCallback
myAWSIoTMQTTClient.subscribe("$aws/things/windturbine/shadow/update", 1, customCallback) 

#  
parser = argparse.ArgumentParser()
parser.add_argument('datafile', help="read lines from csv file")
args = parser.parse_args()

# 
data = pd.read_csv(args.datafile)

# 
while len(data) >= 1:

    time.sleep (3) 

    # use variable control to check if it is set to "on", 
    # then only proceed with sending the telemetry, in case it is "off" we will not. 
    # this variable is switched to "off" in the customCallback function, 
    # when device recives a message to stop the device.
   
    if control == "on":
        
        #reading the first line from the csv file
        line = data.iloc[0, :]
        
        # remove the first line from the csv file 
        data.drop(data.index[0], inplace=True)
        
        
        print("----------------------------------------------------------------------------------------------------------------")
        

        jpayload = {}
 
        # prepare json payload
       
        jpayload ['wind_speed'] = int(line['wind_speed'])
        jpayload ['RPM_blade'] = int(line['RPM_blade'])
        jpayload ['oil_temperature'] = int(line['oil_temperature'])
        jpayload ['oil_level'] = int(line['oil_level'])
        jpayload ['temperature'] = int(line['temperature'])
        jpayload ['humidity'] = int(line['humidity'])
        jpayload ['vibrations_frequency'] = int(line['vibrations_frequency'])
        jpayload ['pressure'] = int(line['pressure'])
        jpayload ['wind_direction'] = int(line['wind_direction'])

        json_data = json.dumps(jpayload)    

        print(json_data)
        print("\n" )

        # publish to the topic
        myAWSIoTMQTTClient.publish("windturbine/xgboost", json_data, 1)
        

