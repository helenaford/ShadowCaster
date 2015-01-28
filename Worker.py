#!/usr/bin/python
##poll the service at intervals to receive new messages
#when the worker ec2 has been launced , this script will be started from a batch script

# conf = {
#   "sqs-access-key": "",
#   "sqs-secret-key": "",
#   "sqs-queue-name": "",
#   "sqs-region": "us-east-1",
#   "sqs-path": "sqssend"
# }

import csv as csv
import os.path
import boto.sqs
from boto.sqs.message import RawMessage
from queueOperator import *
from bucketOperator import *
from Tracer import *
import json
import time
import boto.utils


def addSphere(scene,x,y,z,radius,r,g,b,md,ms,mr):
    scene.addObject(Sphere(Point(x,y,z),radius,Tuple(r,g,b)))

def addLight(scene,x,y,z,r,g,b):
    scene.addLight(Light(Point(x,y,z),Tuple(r,g,b)))

def addPlane(scene,x,y,z,d,r,g,b,md,ms,mr):
    pn = Vector(x,y,z)
    pn.normalize()
    scene.addObject(Plane(pn,d,Tuple(r,g,b)))

def readCSV(line):
#Frame,Object,X,Y,Z,radius,R,G,B,md,ms,mr
    X = float(line["X"])
    Y = float(line["Y"])
    Z = float(line["Z"])
    radius = float(line["radius"])
    R = float(line["R"])
    G = float(line["G"])
    B = float(line["B"])
    md = float(line["md"])
    ms = float(line["ms"])
    mr = float(line["mr"])
    print X,Y,Z,radius,R,G,B,md,ms,mr
    return X,Y,Z,radius,R,G,B,md,ms,mr

def readIn(frame_file_name, scene):
    file_obj = open(frame_file_name)
    reader = csv.DictReader(file_obj, delimiter=',')
    #Each line is a new frame.#
    for line in reader:
        if(line["Obj"] == "Sphere"):
            print "Reading Sphere"
            x,y,z,radius,r,g,b,md,ms,mr = readCSV(line)
            addSphere(scene,x,y,z,radius,r,g,b,md,ms,mr)
        elif(line["Obj"] == "Plane"):
            print "Reading Plane"
            x,y,z,d,r,g,b,md,ms,mr = readCSV(line)
            addPlane(scene,x,y,z,d,r,g,b,md,ms,mr)
        elif(line["Obj"] == "Light"):
            print "Reading Light"
            x,y,z,radius,r,g,b,md,ms,mr = readCSV(line)
            addLight(scene,x,y,z,r,g,b)
        else:
            print "Error Reading Object"
    print "Read file"
    file_obj.close()
    return scene


    
def extractFrameNo(str):
    strNoExt = str.split('.')[0]
    frameNo = strNoExt.split('_Frame')[-1]
    return frameNo
    
def ray_tracer(frame_file_name,message,q):
    scene = Tracer(400,400)
    scene = readIn(frame_file_name,scene)
    scene.trace(message,q)
    frame_name_split = frame_file_name.split('.')
    frame_output_file_name = frame_name_split[0]
    scene.w.outputPPM((frame_output_file_name+".ppm"))
    
    return 'done';

#continuously poll service, trying to read one message at a time


def workerRenderFrame(msg_data,message,inQ,outQ):
    frame_file = msg_data['frame_file']
    userid = msg_data['userid']
    animationName = msg_data['anim_name']
    #get the relevant frame file from s3
    data = downloadfile(frame_file,userid)
    #once file is retrieved , ray tace.
    ray_tracer(data,message,inQ)
    print 'File Rendered'
    frame_file_noExt = frame_file.split('.')[0]
    local_file = "/tmp/"+frame_file_noExt+".ppm"
    remote_file = animationName+"/"+frame_file_noExt+".ppm"
    success = uploadfile(local_file,remote_file,userid)
    if (success is 'done'):
            print deleteMessageFromQueue(inQ,message) #msg needs to be deleted before it is visble again
            frame_no = extractFrameNo(frame_file);
            print frame_no
            sendMessageToOutputQueue(outQ, frame_no,'done',userid)

def workerLoop(id):

    inQ = connectToQueue(id+"InputQueue")
    outQ = connectToQueue(id+"OutputQueue")
    while(True):
            msg_data,message = readMessageFromQueue(inQ)
            if msg_data is not None:   # if it is continue reading until you get a message
                if msg_data['msgtype'] == 'Frame':
                    workerRenderFrame(msg_data,message,inQ,outQ)
                elif msg_data['msgtype'] == 'killCommand':
                    #inQ = connectToQueue(msg_data['inqueuetoChange'])
                    #outQ = connectToQueue(msg_data['outqueuetoChange'])
                    ####SHUTDOWN WORKER#####
                    print msg_data['command']
                    print 'shutdown worker'
                  #make if wait before if reads in another message
            time.sleep(1)


if __name__ == "__main__":
    id=boto.utils.boto.utils.get_instance_userdata()
    f = open('/tmp/hasStarted','w')
    f.write(str(id)) # test userdata
    f.close() # you can omit
    if id == '':
        id = 1
    print 'id', id
    workerLoop(str(id))
