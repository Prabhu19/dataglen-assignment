# -*- coding: utf-8 -*-

###########################################################################################################
#						IMPORTS							                          #
###########################################################################################################
import sys
import os
import subprocess

# Check for the python version based on which import the configparser..
python_version = sys.version_info[0]
if python_version == 3:
    import configparser as ConfigParser
else:
    import ConfigParser




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