# -*- coding: utf-8 -*-
# This is the python program which creates the necessary Topics needed for the this assignment to work

###########################################################################################################
#						IMPORTS							                          #
###########################################################################################################
# needed for executing commands
import sys
sys.path.append('../')

# import the functions defined in the function kit.
from functionkit import utils as ut

###########################################################################################################
#						Main Program						                          #
###########################################################################################################


if __name__ == "__main__":
    
        # Below are the Parms which needs to be got as Input from user.. 
        if len(sys.argv) == 2:
            # Get which command to execute..
            command_name = sys.argv[1]
            print("Command received is >>> "+command_name+"\n\n")            
            
            # check if the command which is got is eiether create or delete or list if not exit the program
            if command_name not in ['create','delete','list']:
                print("Illegal Arguments passed... pass eiether create or delete or list")
                print("Exiting the program.....")
                exit()
        else:
            # illegal number of arguments passed..
            print("Illegal number of Arguments passed... pass eiether create or delete or list")
            print("Exiting the program.....")
            exit()
        
        #print(command_name)
        
        
        # Read the various configurations from the config file.
        print("Reading the properties from the property file..")
        kafka_broker = ut.readConfig('kafka','kafkabroker')
        kafka_input_topic = ut.readConfig('kafka','kafkainputtopic')
        kafka_input_partitions = ut.readConfig('kafka','kafkainputpartitions')
        kafka_output_topic = ut.readConfig('kafka','kafkaoutputtopic')
        zookeeper_address = ut.readConfig('zookeeper','zookeeperaddress')
        
        
        # Print the configurations if needed
        print("Kafka broker >>> "+kafka_broker)
        print("Kafka Input Topic partitions >>> "+str(kafka_input_partitions))
        print("Kafka Input Topic name >>> "+kafka_input_topic)
        print("Kafka Output Topic name >>> "+kafka_output_topic)
        print("Zookeeper Address >>> "+zookeeper_address+"\n\n")
        
        
        # If create was passed as argument execute the create topic command 
        if command_name == 'create':
            print('Executing the create topic command ')
            COMMAND1 = "kafka-topics --zookeeper "+zookeeper_address+" --create --topic "+kafka_input_topic+" --partitions "+kafka_input_partitions+" --replication-factor 1"
            COMMAND2 = "kafka-topics --zookeeper "+zookeeper_address+" --create --topic "+kafka_output_topic+" --partitions 1 --replication-factor 1"
            print("\n\n"+COMMAND1)
            tuple1=ut.execute_command(COMMAND1)
            print(tuple1[0],tuple1[1])
            
            print("\n\n"+COMMAND2)
            tuple1=ut.execute_command(COMMAND2)
            print(tuple1[0],tuple1[1])
        
        
        # if list was passed as argument execute the list topics command. 
        elif command_name == 'list':
            print("Executing the list topic command")
            COMMAND3 = "kafka-topics --zookeeper "+zookeeper_address+" --list"
            print("\n\n"+COMMAND3)
            tuple1=ut.execute_command(COMMAND3)
            print(tuple1[0],tuple1[1])
            
            
        # if delete was passed as argument execute the delete topic command
        elif command_name == 'delete':
            print("Executing the delete topic command")
            COMMAND4 = "kafka-topics --zookeeper "+zookeeper_address+" --delete --topic "+kafka_input_topic
            COMMAND5 = "kafka-topics --zookeeper "+zookeeper_address+" --delete --topic "+kafka_output_topic
            print("\n\n"+COMMAND4)
            tuple1=ut.execute_command(COMMAND4)
            print(tuple1[0],tuple1[1])            
            
            print("\n\n"+COMMAND5)
            tuple1=ut.execute_command(COMMAND5)
            print(tuple1[0],tuple1[1])


###################################### End of Program ###########################################################
#################################################################################################################