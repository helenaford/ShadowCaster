import csv
from queueOperator import *
from bucketOperator import *
import boto
import time
import boto.ec2
#systemInformation = [ SubMasterID, countOfWorkers, status ]
systemInformation = []
subMasterCounter = int(0)


def readConfigFileGlobal(config_file_path):
    config_file = open(config_file_path)
    reader = csv.DictReader(config_file, delimiter='~')
    for line in reader:
        reqWorkers = line['Workers']
   
    config_file.close()
    return reqWorkers

def get_next_submaster_id():
    global subMasterCounter
    ec2 = boto.connect_ec2()
   
    reservations = ec2.get_all_instances(filters={"tag:type":'Submaster',"instance-state-name":'running'})
    id_numbers = []
    # Loop reservations/instances.
    for r in reservations:
        for instance in r.instances:
            print 'runninginstances'
            if "idnumber" in instance.tags:
                id_numbers.append(instance.tags['idnumber'])
                print 'idnumber', id_numbers

    if len(id_numbers) != 0:
        subMasterCounter = max(id_numbers)
    print 'nextid', str(subMasterCounter)
    

def create_submaster():
    global subMasterCounter
    subMasterCounter = int(subMasterCounter) + 1
   
    ec2 = boto.connect_ec2()
    reservation = ec2.run_instances(image_id='ami-d05223b8', key_name='keyy',
    subnet_id='subnet-aaeb39f3',
    instance_type='t2.micro',
    instance_profile_name='workerProfile',
    user_data=str(subMasterCounter))
    
    ec2.create_tags(reservation.instances[0].id,{"Name":str(subMasterCounter)+'Submaster',"type":'Submaster',"idnumber":subMasterCounter})
    
    

def create_workers(subMasterID,workers_no):
    ec2 = boto.connect_ec2()
    reservation = ec2.run_instances(image_id='ami-ea552482', min_count=workers_no, max_count=workers_no,key_name='keyy',
    subnet_id='subnet-aaeb39f3',
    instance_type='t2.micro',
    instance_profile_name='workerProfile',
    user_data=str(subMasterID))
    ids = []
    for instance in reservation.instances:
        ids.append(instance.id)

    print ids
    #for instance in reservation:
    ec2.create_tags(ids,{"Name":str(subMasterID)+'Worker',"type":'Worker',"idnumber":subMasterID})

def terminate_submaster(subMasterID):
    ec2 = boto.connect_ec2()
    reservations = ec2.get_all_instances(filters={"tag:Name":str(subMasterID)+'Submaster',"instance-state-name":'running'})
    try:
        reservations[0].instances[0].terminate()
        
    except:
        print 'submaster is not running'
   


def terminate_workers(subMasterID,workers_no):
    ec2 = boto.connect_ec2()
    reservations = ec2.get_all_instances(filters={"tag:Name":str(subMasterID)+'Worker',"instance-state-name":'running'})
    terminateCounter = 0
    # Loop reservations/instances.
    for r in reservations:
        print 'del r'
        for instance in r.instances:
                if terminateCounter < workers_no:
                    print 'iid', instance.id
                    instance.terminate()
                    terminateCounter = terminateCounter +1
                else:
                    break
        
def adjustWorkers(requiredWorkers,currentWorkers,subMasterID):
    workerDiff = int(requiredWorkers) - int(currentWorkers)
    if workerDiff == 0:
        #noworkers to create
        print "no more or less workers needed"
    elif workerDiff > 0:
        #while workerDiff > 0:
            #CreateWorkers with inQ and outQ
            create_workers(subMasterID,workerDiff)
            #workerDiff = workerDiff - 1
    else:
        #kill some workers
        #while workerDiff < 0:
        print 'killed worker ###'
            ####
        terminate_workers(subMasterID,abs(workerDiff))
        #workerDiff = 0

def cloudWatcher(cloudq, globOutq):
    
    msg_data1,message1 = readMessageFromQueue(cloudq)
    #print 'msg_data',msg_data
    #print 'msg', message

    if msg_data1 is not None:
        for i in range(1,2):
            #read msg from output queue
            msg_data,message = readMessageFromQueue(globOutq)
            if msg_data is not None:
                        print 'msg received'
                        submasterID = msg_data['submasterid']
                        
                        #terminate by submasterid 
                        terminate_submaster(submasterID)
                        print 'terminated', submasterID
                        terminate_workers(submasterID,-1)
                        print 'terminated workers'
                         
                        #delete notification of idle submasters from watcher
                        deleteMessageFromQueue(globOutq,message)
        deleteMessageFromQueue(cloudq,message1)

    


if __name__ == "__main__":

    globInQ = connectToQueue("GlobalInputQueue")
    globOutQ = connectToQueue("GlobalOutputQueue")
       
    SubmasterWatchQueue = connectToQueue('SubmasterWatchQueue')
    subcribeToCloudWatch(SubmasterWatchQueue)
    snsConnection = boto.connect_sns()
    get_next_submaster_id()
    if subMasterCounter == 0:
        #create subMaster, and (2) workers for starters
        create_submaster()
        create_workers(subMasterCounter,2)
  
    while(True):
        #This emulates incoming data, here we will actually just listen to a port
        # waiting for the next conif file to be uploaded...
        clientQ = connectToQueue("ClientInputQueue");
        
        print 'connected to client queue'
        userid = None
        config_file_name = None
        while(True):
            cloudWatcher(SubmasterWatchQueue,globOutQ)
            msg_data,message = readMessageFromQueue(clientQ)
            if msg_data is not None:
                print 'received msg'
                userid = msg_data['userid']
                animName = msg_data['animname']
                email = msg_data['email']
                

                if(email != ''):
                    
                    topicName = userid
                    topicArn = subscribeToTopic(snsConnection,topicName, email)

                clientoutputQueue_str = userid + animName+ "Queue"
                clientOutQ = connectToQueue(clientoutputQueue_str)
                bucket = createuserbucket(userid)
                bucketName = getBucketName(userid)
                config_file_name = animName + '.config'
                sendMessageToClientQueue(clientOutQ,bucketName,'None')
                print 'sent message to client'
                while checkFileExists(animName+'.txt',userid) is False:
                    #waiting for file
                    print 'waitng for animation file to upload'
                    time.sleep(1)
                print 'upload complete'
                #delete_queue(clientOutQ)
                deleteMessageFromQueue(clientQ,message)
                while(True):
                    msg_data,message = readMessageFromQueue(clientOutQ)
                    if msg_data is not None:
                        try:
                            print msg_data['status']
                            print 'got upload complete message'
                            deleteMessageFromQueue(clientOutQ,message)
                            break;
                        except KeyError:
                            message.change_visibility(visibility_timeout=0)
                            print 'read own message, freeing message '
                            time.sleep(1)
                            
                break
        
        config_file_path = downloadfile(config_file_name,userid)
        print config_file_path
        print 'new animation from user incoming'
        createdSubmaster = False

        while(True):

           ###Maybe here we add the cloudwatcher than ensures only 1 submaster is free
           ##I.E it kills submasters (and their workers) if more than 1 message
           ## is on this queue? (after a certain time??)

           msg_data,message = readMessageFromQueue(globOutQ)
           if msg_data is not None:
               submasterID = msg_data['submasterid']
               currWorkers = msg_data['workercount']
                
               deleteMessageFromQueue(globOutQ,message)
               subMInQ = str(submasterID)+'InputQueue'
               subMOutQ = str(submasterID)+'OutputQueue'
               reqWorkers =  readConfigFileGlobal(config_file_path)
               adjustWorkers(reqWorkers,currWorkers,submasterID)
               sendMessageToGlobalInputQueue(globInQ,
                                             'Render',
                                             submasterID,
                                             config_file_name,
                                             reqWorkers,
                                             userid,
                                             topicArn)
               createdSubmaster = False
               break
           elif createdSubmaster is False:
               print 'need to create sub master'
               create_submaster()
               create_workers(subMasterCounter,2)
               createdSubmaster = True
        


