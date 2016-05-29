
####
#import libraries
####

#webservices and apis
import json
import urllib2

#generic libs
import csv
from time import time,sleep
import logging
from os import getcwd

###
#now setup config options
###
print "Running "+__file__

delay = 30 #how many seconds do you want to wait before polling again
errdelay = 0.25 #how many seconds to wait to repoll after error
max_attempts = 10 # max consecutive attempts before quit

#https://docs.python.org/2/howto/logging.html
logging.basicConfig(filename='errors.log',level=logging.WARNING) #WARNING

outputfolder = getcwd()+"/"# Worth testing this on Windows,may not be same
outputdoc = "data.csv"
outputfile = outputfolder+outputdoc

contract = "Dublin"

class jcCity:
    "Class to query JC Decaux Bikes Info"

    output_fields = ['number','contract_name','last_update'] # default fields to download
    
    def __init__(self,contract,api_location=None):
        print "Init Object - %(contract)s %(api)s" % {'contract':contract,'api':api_location}
        # if a api location is specified use this - otherwise use the default
        if api_location is None:
            #print "No loca"
            self.key = self.getAPIKey()
        else:
            #print "loc"
            self.key = self.getAPIKey(api_location)
        
        self.contract = contract
        logging.info( str(time())+" API Key "+self.key)
        
    def getAPIKey(self,key_file="api_key"):
        "Function to return the API key - by default this is in file api_key"
        #noteto self this should belong to parent object - jcd -setup inheritance
        f = open(key_file,'r')    
        api_key = f.readline()
        logging.info( str(time())+" API Key "+api_key)
        return api_key

    def getStaticData(self):
        #get static data - may want to use this for iterating or as a reference table
        url = "https://developer.jcdecaux.com/rest/vls/stations/"+self.contract+".json"
        try:
            response = urllib2.urlopen(url).read()
        except:
            print "Error in getSaticData opening "+url
        return response

    def getStationsData(self,station_num=None):
        "Function to get station data - pass station number to get an individual station"
        contract = self.contract
        api_key = self.key
        
        logging.debug( str(time())+"In getallstationdata")
        if station_num is None:
            station_num=""
        
        url = "https://api.jcdecaux.com/vls/v1/stations/"+str(station_num)+"?contract="
        url = url+contract+"&apiKey="+api_key
        #print url
        try:
            response = urllib2.urlopen(url).read()       
        except urllib2.HTTPError,e:
            logging.error( str(time())+" HTTP Error -"+str(e))
        except urllib2.URLError,e:
            logging.debug( str(time())+"in url error")
            logging.debug( str(time())+str(e))
            logging.error(str(time())+" URL Error -"+str(e))
        except urllib2.HTTPException,e:
            logging.error( str(time())+" HTTP Exception ")
        except Exception:
            logging.error( str(time())+" Error in getAllStationsData opening "+url)
        return response

    

    def parseStationsData(self,str_data,*args):
        #function to create the result object
        #by default this will just get station number, contract and timestamp
        #additional arguments are passed by args
        obj_data = json.loads(str_data)
        #print type(obj_data)
        #if a dict then there is only one record - it is a single station
        if isinstance(obj_data , dict):
            obj_data = [obj_data]   #convert to a list
            print "RETYPE"
        return_data = {}
        i=0
        
        for o in obj_data:
            return_data[i] = {}
            """
            return_data[i]['number'] = o['number']
            return_data[i]['contract_name'] = o['contract_name']
            return_data[i]['last_update'] = o['last_update']
            """
            #get  arguments
            for out in self.output_fields:
                #print a
                return_data[i][out] = o[out] #add a test here
            #get optional arguments
            for a in args:
                #print a
                return_data[i][a] = o[a] #add a test here
            i=i+1
        return return_data

    def add_fields(self,*args):
        "Function to add fields to analysis"
        for a in args:
            self.output_fields.append(a)
            
    def list_fields(self,*args):
        "Function to list fields for analysis"
        return self.output_fields



class bikeWriter:

    def compareHeaders(self,inn,out):
        "compare headers - in is the input headers;out is the output headers"
        #compare myheaders and headers
        new_fields = set(inn).difference(out)
        redundant_fields = set(out).difference(inn)

        if len(new_fields)>0:
            #print "New fields in input"
            raise ValueError('New fields in input that do not match output')
            #raise a value error
            #create new file - external to this function

        if len(redundant_fields)>0:
            logging.info(str(time())+ " Fields not used - "+str(redundant_fields))
            #raise a warning

    def __init__(self,in_obj,out_file,local_timestamp=True):
        self.outfile = out_file
        self.timestamp = local_timestamp
        self.fields = in_obj.output_fields[:] #create a copy of old list
        #self.fields.append(in_obj.output_fields)
        
        if local_timestamp:
            self.fields.append('local_timestamp')
        
        #first try to open the file
        try:
            myfile = open(out_file,'r')
            headerline = myfile.readline().rstrip().split(",")
            myfile.close()
                    #then compare headers
            try:
                self.compareHeaders(in_obj.output_fields,headerline)
                
            except ValueError,e:
                logging.warning(str(time())+ "Value Error: "+str(e))
        except IOError:
            logging.warning(str(time())+  " No such file - creating new file")
            f = open(out_file, 'w')
            outputstr = ','.join(self.fields)
            f.write(outputstr+'\n')
            f.close()


    def write(self,input_obj):
        outputfile = self.outfile
        with open(outputfile, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fields)
            for line in input_obj:
                if self.timestamp:
                    input_obj [line]['local_timestamp']=str(time())
                writer.writerow(input_obj[line])
                    
            
def main(contract,delay,errdelay):        
    #set up classes for query and output
    logging.error(str(time())+" In Main") 
    bike =  jcCity(contract)
    bike.add_fields('available_bikes','available_bike_stands')
    out = bikeWriter(bike,outputfile)
    i=0
    attempts=0

    while True and attempts<=max_attempts:
        try:
            #now poll the data at intervals
            #print "Start "+str(time())
            get_data = bike.getStationsData()
            parse_data = bike.parseStationsData(get_data)
            out.write(parse_data)
            #print "Wait "+str(time())
            sleep(delay)
            attempts=0
            i=i+1
            #print i
            #print "End "+str(time())
        except:
            logging.error( str(time())+"Error in poll_data - error count "+str(attempts))
            sleep(errdelay)
            errdelay=errdelay* 2
            logging.debug(str(time())+"Delay is "+str(errdelay))
            attempts = attempts+1
            i=i+1
            #print i

    

#if this is being run directly then call main
#if __name__ == "__main__":
#    main(contract,delay,errdelay)



###TO DO
# Pass arguments from cmd
# External config file
# Write out to multiple file formats SQL,AWS etc..                       
# Some sort of status check server - maybe an ec2 instance?
# Create a top level object - may wish to query multiple cities


###DONE
# Setup error logging
#  Setup timer to query at intervals ; query error intervals as well
# Bring main running into a function
# Import API so it is not on GIT
# Make file handling a little bit less hard coded. eg  list of fields in config separated into all bonus
# Make it object oriented
# Assocaite city  with input params


##CANCEL
# Maybe join the parse and query functions? No, worth distinguishing these

