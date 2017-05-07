# Spark Realtime Streaming program.
###########################################################################################################################
#                               Python IMports 
###########################################################################################################################
# Spark context is required for streaming context which is the starting point for any spark streaming program
from pyspark import SparkContext
# import the streamingContext used to create the ssc object whihc is the starting point of Spark Streaming program
from pyspark.streaming import StreamingContext
# Import the kafkautils used to read the data from kafka topics
from pyspark.streaming.kafka import KafkaUtils
# needed for adding the utils.py file to Pyfile
import os
import sys

# append project root dir to python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append("../")

# import the utils so that we can read the configurations
from functionkit import utils as util
# import Kafkaproducer needed for sending the message to kafka
from kafka import KafkaProducer
# Import Json 
import json

#########################################################################################################################
#                               Main Code
##########################################################################################################################

if __name__ == "__main__":
	
	# Read the kafka Configurations
	kafka_broker = util.readConfig('kafka','kafkabroker')
	kafka_input_topic = util.readConfig('kafka','kafkainputtopic')
	batch_window = util.readConfig('spark','streamingbatchwindow')
	checkpointpath = util.readConfig('spark','checkpointpath')
	spark_master = util.readConfig('spark','sparkmaster')	
	kafka_output_topic = util.readConfig('kafka','kafkaoutputtopic')

	# create spark and streaming contexts
	sc = SparkContext(spark_master,"Spark Streaming Example")
	
	# add the Functionkit util function using addpyfile..
	#print(os.path.join(os.path.dirname(os.path.dirname(__file__)),'functionkit','utils.py'))
	# this file will be available to all worker nodes by doing this..
	sc.addPyFile(os.path.join(os.path.dirname(os.path.dirname(__file__)),'functionkit','utils.py'))


	# import the utility functions so that they can be called..
	import utils as ut


	# Create the streaming context object which is the starting point for DStreams.
	ssc = StreamingContext(sc, int(batch_window))

	# defining the checkpoint directory
	ssc.checkpoint(checkpointpath)

        # Read the syslog from the Kafka Broker .. 
	kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_input_topic], {"metadata.broker.list": kafka_broker})


	# Perform the GroupbyKey operation and convert the values in to a list..
	groupedStream = kafkaStream.groupByKey().mapValues(list)
	#groupedStream.cache()

	# Using the map transformation call the function which gets the desired output for each key as per the assignment
	groupedStream = groupedStream.map(lambda tuple1: ut.get_desired_assignment_output(tuple1[0],tuple1[1]))
	

	# Write the data in to Kafka
	groupedStream.foreachRDD(lambda message: ut.handler(message,kafka_broker,kafka_output_topic))
	
	# Print the resulting data 
	groupedStream.pprint()
	
	# Start the computation
	ssc.start()            

	# Wait for the computation to terminate.
	ssc.awaitTermination()

