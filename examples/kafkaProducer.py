# THis is the Kafka producer program

###########################################################################################################
#						IMPORTS							                          #
###########################################################################################################

# Append to python path
import sys
sys.path.append('../')

########## please change this as required...

# this package is used for sending messages to Kafka broker.
from kafka import KafkaProducer
# import the datetime package to generate the current datetime. 
import datetime
# used for json serializing and deserializing
import json
# below package is used for generating a random integer 
from random import randint
# Below package is used in this program for sleeping 30 seconds 
import time
# import the functions defined in the function kit.
from functionkit import utils as ut



###########################################################################################################
#						Main Program						                          #
###########################################################################################################

# Read the various configurations from the config file.
kafka_broker = ut.readConfig('kafka','kafkabroker')
kafka_input_topic = ut.readConfig('kafka','kafkainputtopic')
#print(kafka_broker)
#print(kafka_input_topic)


#Now create the kafka producer object...
producer = KafkaProducer(bootstrap_servers=kafka_broker, \
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'), \
                            key_serializer=str.encode)
      

# run in a ever ending loop to produce messages every 30 seconds.
exitFlag = False
while True:
    
    print("Sleeping for 30 seconds now...")
    # Sleep for 30 seconds. 
    time.sleep(30)

    # Every 30 seconds generate 5 messages since there are 5 keys.
    keys = ['key1','key2','key3','key4','key5']

    # generate message in kafka for each of the keys..
    for key in keys:
        # Message format 
        # Key1 val: {"TIMESTAMP": "2017-02-25T04:44:18", "val": 0, "key": "Key1”}

        # Use the producer.send method to send the data to kafka topic and to a particular partition. 
        try:
            print("Sending message with key >>> "+key+" to the kafka Broker...")  
            partition_number = int(key[-1]) -1
            
            #print(partition_number)
            
            future = producer.send(kafka_input_topic, \
                           key=key, value={'key': key,'val': randint(0,20),'timestamp': datetime.datetime.now().isoformat()}, \
                           partition=partition_number)
            
        except Exception as ex:
            print('Exception Caught: Unable to send the message to the Kafka Broker')
            print(ex)
            # break out of the loop. 
            exitFlag = True
            break
        
        # below code is to check if the parititioning is working correctly..Block for synchronous sends..
        '''
        from kafka.errors import KafkaError
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            print("Kafka Error")
            pass

        print (record_metadata.topic)
        print (record_metadata.partition)
        '''
    
    # Break out of the forever loop if there is any exceptions raised..
    if exitFlag:
        break



######################################################################################################
###################  END OF PROGRAM ##################################################################
######################################################################################################