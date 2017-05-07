# -*- coding: utf-8 -*-

###########################################################################################################
#						IMPORTS							                          #
###########################################################################################################
import sys
# os is required for creating the directory path where the config.properties file exist
import os
# below import is required for executing commands
import subprocess
# below is needed for parsing the dictionary
import json
# below package is used for date manipulations
import datetime
# config parser is required for reading the configurations from the configuration file
# Check for the python version based on which import the configparser..
python_version = sys.version_info[0]
if python_version == 3:
    import configparser as ConfigParser
else:
    import ConfigParser

from kafka import KafkaProducer



###########################################################################################################
#						Functions						                               #
###########################################################################################################

# Function 1
# this function is used for REading the configurations needed for the programs .. 
# Returns the value for the property..
def readConfig(topic,property):
    configParser = ConfigParser.RawConfigParser()
    filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)),'config','Configfile.properties')
    #print(filepath)
    try:
        configParser.read(filepath)
        value=configParser.get(topic,property)
    except Exception as e:
        print("Caught exception while reading the property file..")
        print(e)
        return
        
    return value
    
    
# Function 2
# This is the function which calls the Popen function and executes the command..
# Returns the STDERR and STDOUT of the command.. 
def execute_command(command,arg_cwd=None):
	p=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,cwd=arg_cwd)
	stdout,stderr=p.communicate()
	return(stdout,stderr)



# Function 3
# This functin is used in Spark Streaming..
# this function gets the desired output as per the assignment..
def get_desired_assignment_output(k,v):
	# second argument to this function is the value which is actually a list of dictionary of below format 
	#['{"key": "key3", "val": 15, "timestamp": "2017-05-07T11:12:13.896974"}', '{"key": "key3", "val": 5, "timestamp": "2017-05-07T11:12:43.907046"}', '{"key": "key3", "val": 19, "timestamp": "2017-05-07T11:13:13.930210"}', '{"key": "key3", "val": 18, "timestamp": "2017-05-07T11:13:43.964014"}']

	# Create a dictionary which contains the final results which is required for the assignement
	final_dict = {}
	
	# Get the length of the value list
	final_dict['count'] = len(v)

	# Assign key with the value of k
	final_dict['key'] = k

	# Get all the val present in each dictionary to a list 
	# this is required so that average can be computed..
	val_list = [ json.loads(str1)['val'] for str1 in v]
	final_dict['vals'] = val_list
	
	# Get al the timestamps and convert it to a list and assign to the dictionary key ts
	final_dict['ts'] = [json.loads(str1)['timestamp'] for str1 in v]
	
	# Find the mean of the val_list and assign to the dictionary key mean
	final_dict['mean'] = sum(val_list) / len(v)

	# Find the sum of the val_list and assign to the dictionary key sum
	final_dict['sum'] = sum(val_list)

	# The below piece of code is used to get the current batch window Start timestamp        
	if len(v) == 4:
		timestamp = json.loads(v[0])['timestamp']
	else:
		timestamp = json.loads(v[-1])['timestamp']

	# Call the function which return the current batch window Start timestamp..
	final_dict['TIMESTAMP'] = get_window_start_timestamp(timestamp,len(v))
	
	# Return the tuple containing the key and the final_dictionary..
	return (k,final_dict)


# Function 4
# THis function is used for computing the current batch window start timestamp
def get_window_start_timestamp(timestamp,length):
	# convert the timestamp in to datetime
	dt = datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%S.%f")
	# if the length is four for a particular key
	if length == 4:
		# replace only the seconds & micro seconds to zero.
		dtwithoutseconds = dt.replace(second=0, microsecond=0)
	else:
		# subtract one min
		new_dt = dt - datetime.timedelta(minutes=1)
		# replace the seconds & micro seconds to zero.
		dtwithoutseconds = new_dt.replace(second=0, microsecond=0)
	# convert back the datetime to string
	final_timestamp = dtwithoutseconds.strftime("%Y-%m-%dT%H:%M:%S.%f")
	# return the final window timestamp.
	return final_timestamp



# Function 5 
# THis function is used to push the computed result in to the Output topic in kafka.. 
# Write the data back to kafka Output Topic..
def handler(message,kafka_broker,kafka_output_topic):
	# collect all the messages
	records = message.collect()
	# create the producer object
	producer = KafkaProducer(bootstrap_servers=kafka_broker,key_serializer=str.encode,\
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
	# if the number of records is greater than zero send each record to kafka.
	if len(records) > 0:
		for record in records:
			producer.send(kafka_output_topic,key=record[0],value=record[1])
			producer.flush()

