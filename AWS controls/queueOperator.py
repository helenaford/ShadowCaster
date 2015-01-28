import uuid
import time
from boto import *#import boto
import boto.sns
import json
from boto.sqs.message import RawMessage

 
userAnimationInputQueue = "InputQueue"
userAnimationOutputQueue = "OutputQueue"
#inputQueue = username_anim_name_

def connectToQueue(queueName):
    sqs = boto.connect_sqs()
    q = sqs.create_queue(queueName, 120) #create the queue if it does not exist
    #q = sqs.get_queue(queueName)
    return q

def sendMessageToClientQueue(q,bucketName,uploadProgress):
    data = {
        'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        'bucketname': str(bucketName),
        'uploadprogress': uploadProgress
    }
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)

def sendMessageToGlobalInputQueue(q,type,subMasterID,configFile,newWorkerCount,userid,topicarn):
    data = {
        'msgtype': str(type),
        'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        'key': str(uuid.uuid1()),#secret key/anwer to s3 instead of boto config file 
        'userid': str(userid), #or bucketname attribute
        'submasterid': int(subMasterID),
        'configfile': str(configFile),
        'newworkercount': int(newWorkerCount),
        'topicarn': str(topicarn)
    }
    # Connect to SQS and open the queue
 
    # Put the message in the queue
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)

def sendMessageToGlobalOutputQueue(q,subMasterID,workerCount):
    data = {
        'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        'key': str(uuid.uuid1()),#secret key/anwer to s3 instead of boto config file
        'submasterid': int(subMasterID),
        'workercount': str(workerCount)
    }
    # Connect to SQS and open the queue
 
    # Put the message in the queue
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)
    return m

# Pushes a message onto the queue
def sendMessageToInputQueue(q, anim_name,frame_file,type,userid):
    # Data required by the API
    if type == 'Frame':
        data = {
            'msgtype': str(type),
            'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
            'key': str(uuid.uuid1()),#secret key/anwer to s3 instead of boto config file 
            'userid': str(userid), #or bucketname attribute
            'anim_name':str(anim_name),
            'frame_file': str(frame_file)
        }
    elif type == 'killCommand':
        data = {
            'msgtype': str(type),
            'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
            'key': str(uuid.uuid1()),#secret key/anwer to s3 instead of boto config file 
            'userid': str(userid), #or bucketname attribute
            'command': str(message)
        }
    # Connect to SQS and open the queue
 
    # Put the message in the queue
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)
 # Polls the queue for messages
 # Pushes a message onto the queue

def sendMessageToOutputQueue(q,frame_no,status,userid):
    # Data required by the API
    data = {
        'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        'userid': str(userid), #or bucketname attribute
        'frame_no': frame_no,
        'status': status
    }
    # Connect to SQS and open the queue
 
    # Put the message in the queue
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)
 # Polls the queue for messages
def readMessagesFromOutputQueue(q):
    # Open the queue
    q.set_message_class(RawMessage)
 
    # Get all the messages in the queue
    results = q.get_messages()
    ret = "Got %s result(s) this time.\n\n" % len(results)
 
    for result in results:
        msg = json.loads(result.get_body())
        ret += "Message: %s\n" % msg['message']
    
    ret += "\n... done."
    return ret
 
# Polls the queue for messages
def readMessagesFromInputQueue(q):
    # Open the queue
    q.set_message_class(RawMessage)
 
    # Get all the messages in the queue
    results = q.get_messages()
    ret = "Got %s result(s) this time.\n\n" % len(results)
 
    for result in results:
        msg = json.loads(result.get_body())
        ret += "Message: %s\n" % msg['message']
    
    ret += "\n... done."
    return ret

# Polls the queue for messages
def readMessageFromQueue(q):
    # Open the queue
    q.set_message_class(RawMessage)
 
    message = q.read()
    ret = "Got %s result(s) this time.\n\n" 
    msg_data = None
    if message is not None:   # if it is continue reading until you get a message
        msg_data = json.loads(message.get_body())
        #key = boto.connect_s3().get_bucket(msg_data['bucket']).get_key(msg_data['key'])
        #data = simplejson.loads(key.get_contents_as_string())
        #do_some_work(data)
        #print q.delete_message(message)
    return msg_data,message

def releaseMessage(q,msg,visibility_timeout):
    sqs = boto.connect_sqs()
    return sqs.change_message_visibility(q, msg.receipt_handle, visibility_timeout)


def deleteMessageFromQueue(q,message):
    return q.delete_message(message)

    
def deleteQueue(q):
    return q.delete_queue(q)

def subscribeToTopic(conn,topicName,email):
    
    topicDetails = conn.create_topic(topicName)
    topicArn =  topicDetails['CreateTopicResponse']['CreateTopicResult']['TopicArn']
    all_subscriptions = conn.get_all_subscriptions_by_topic(topicArn)
    print all_subscriptions
    subs = []
    subs = all_subscriptions['ListSubscriptionsByTopicResponse']['ListSubscriptionsByTopicResult']['Subscriptions']
    isSubscribed = 0
    if len(subs)!=0:
        for subscription in subs:
            if email.lower() == subscription['Endpoint'].lower():
                print 'already subscribed'
                isSubscribed = 1
    
    if isSubscribed == 0:
        conn.subscribe(topicArn, "email", email)
   
    return topicArn


def sendMessageToClientTopic(topicarn,message):
    conn = boto.sns.connect_to_region("us-east-1")
    conn.publish(topicarn, message)

def sendCompleteToClientQueue(q,bucketName,status):
    data = {
        'submitdate': time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        'bucketname': str(bucketName),
        'status': status
    }
    m = RawMessage()
    m.set_body(json.dumps(data))
    status = q.write(m)

def subcribeToCloudWatch(q):
    conn = boto.sns.connect_to_region("us-east-1")
    
    conn.subscribe_sqs_queue('arn:aws:sns:us-east-1:508218073082:SubmasterAlarm', q)
