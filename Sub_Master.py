
import csv as csv
import os.path
from Tracer import *
from bucketOperator import *
from queueOperator import *
import filecmp
import boto.utils




def readCSVShort(line):
    return line["X"], line["Z"], line["Z"], line["radius"], line["R"], line["G"], line["B"], line["md"], line["ms"], line["mr"]

def csvToString(line):
    #string = line["Frame"]
    string = line["Obj"]
    string = string+","+line["X"]
    string = string+","+line["Y"]
    string = string+","+line["Z"]
    string = string+","+line["radius"]
    string = string+","+line["R"]
    string = string+","+line["G"]
    string = string+","+line["B"]
    string = string+","+line["md"]
    string = string+","+line["ms"]
    string = string+","+line["mr"]+"\n"
    return string;

def splitAnimation(anim_file_name,anim_name):
    currentFrame = 0;
    anim_file = open(anim_file_name,"r")
    CSVdetails = anim_file.readline();
    useless, CSVdetails = CSVdetails.split(",",1)
    anim_file.seek(0);
    reader = csv.DictReader(anim_file, delimiter=',')
    outfile = open(anim_name,"w")
    for line in reader:
        if(int(line["Frame"]) != currentFrame):
            currentFrame = currentFrame+1;
            outfile.close()
            outfile = open(anim_name+"_Frame"+line["Frame"]+".txt","w")
            outfile.write(CSVdetails)
            outfile.write(csvToString(line))
        else:
            outfile.write(csvToString(line))
    outfile.close()
    return currentFrame;


def findAnimRange(reader):
    frame = 0;
    for line in reader:
        #do nothing
        frame = line["Frame"]
    return frame;


def readFrameRange(frameRange_str,anim_file_name):
    frame_arr = []
    if(frameRange_str == ""):
        anim_file = open(anim_file_name)
        reader = csv.DictReader(anim_file, delimiter=',')
        lastFrame = findAnimRange(reader)
        for i in range(1,(int(lastFrame)+1)):
            frame_arr.append(int(i))
        return frame_arr
    else:
        frameRange_str_arr = frameRange_str.split(',')
        for i in frameRange_str_arr:
            #find the ranges and iterate through
            ranges_str_arr = i.split('-')
            if(len(ranges_str_arr) > 1):
                #Ranges are INCLUSIVE
                for j in range(int(ranges_str_arr[0]),int(ranges_str_arr[1])+1):
                    frame_arr.append(int(j))
            else:
                frame_arr.append(int(i))
        print frame_arr



def changedFrames(anim_name,frames_arr):
    new_frames_arr = []
    ## WHAT HAPPENS IF THE FRAME DOESNT EXIST????

    for i in range(0,len(frames_arr)):
        new_Frame_Name_str = "./" + anim_name + "_Frame" + str(frames_arr[i]) + ".txt";
        backup_Frame_Name_str = "./" + anim_name + "_Backup_Frame" + str(frames_arr[i]) + ".txt";
        #newFrame_file = open(new_Frame_Name_str,"r")
        #backup_Frame_file = open(backup_Frame_Name_str,"r")
        if(filecmp.cmp(new_Frame_Name_str,backup_Frame_Name_str, shallow=False)):
            print("Frame_"+str(frames_arr[i])+ " Did not change, therefore not adding to lists")
        else:
            new_frames_arr.append(frames_arr[i])
            print("Frame_"+str(frames_arr[i])+ " Changed, Adding To List")
    return new_frames_arr


def createBackup(anim_name,anim_file_name,userid):
    backup_file_name = anim_name + "_Backup" + ".txt"
    infile = open(anim_file_name)
    outfile = open(backup_file_name,"w")
    for inputline in infile:
        outfile.writelines(inputline)
    outfile.close()
    uploadfile(backup_file_name,backup_file_name,userid)
    infile.close()


def checkforBackup(anim_name,anim_file_name,userid):
    backup_file_name = anim_name + "_Backup" + ".txt"
    if (checkFileExists(backup_file_name,userid)):
        backup_file_name = downloadfile(backup_file_name,userid)
        splitAnimation(backup_file_name,(anim_name+"_Backup"))
        return backup_file_name,1
    else:
        print "no backup found, Creating backup"
        createBackup(anim_name,anim_file_name,userid);
        splitAnimation(backup_file_name,(anim_name+"_Backup"))
        return backup_file_name,None



def readConfigFile(config_file_name,userid):
    config_file_path = downloadfile(config_file_name, userid)
    config_file = open(config_file_path) # TBD: need to be uploaded from s3
    reader = csv.DictReader(config_file, delimiter='~')
   
    inQ = connectToQueue(str(SUBMASTER_ID)+"InputQueue")
    outQ = connectToQueue(str(SUBMASTER_ID)+"OutputQueue")
    for line in reader:
        anim_file_name = line["AnimationName"]
        anim_name_arr = anim_file_name.split('.')
        anim_name = anim_name_arr[0]
        #read backup file from s3
        anim_file_name = downloadfile(anim_file_name,userid)
        print 'filename = '
        print anim_file_name
        backup_file_name,backupExists = checkforBackup(anim_name,anim_file_name,userid)
        ## find frame range##
        splitAnimation(anim_file_name,anim_name);
        frame_range_arr = readFrameRange(line["FrameRange"], anim_file_name)
        autoRender = int(line['AutoRenderBool'])
        if autoRender == 1 :
            if(backupExists is not None):
                ## check for changed frames within the given frame range
                frame_range_arr = changedFrames(anim_name,frame_range_arr)
            else:
                print("Backup didnt exist, therefore all frames must be new")
        else:
            print("AutoRender is Off, therefore Re-render Frames, frame_range_arr doesn't change")
        print frame_range_arr
        print "\n\n\n"
        createBackup(anim_name,anim_file_name,userid);
        for i in frame_range_arr:
            # send frame to worker.
            framefilename = anim_name+"_Frame"+str(i)+".txt"
            uploadfile(framefilename,framefilename,userid)
            messageType = 'Frame'
            sendMessageToInputQueue(inQ,anim_name,framefilename,messageType,userid)
        while (len(frame_range_arr) > 0):
            #check output queue, and if frame is rendered, removing the frame from "frame_range_arr"
            #keep doing this until we are complete.      
            msg,message = readMessageFromQueue(outQ)
            if msg is not None:
                deleteMessageFromQueue(outQ,message)
                if msg['status'] == 'done':
                    try:
                        frame_range_arr.remove(int(msg['frame_no']))
                    except Exception:
						pass
                        #print "index number", frame_range_arr.index(int(msg['frame_no']))
    
        #send notification to user that work is done`
        # Close all workers.
        # write in the report, future plans could be to check if other users want to render, and instead of closing,
        # repurpose the worker to the new user.
    config_file.close()
    return anim_name





if __name__ == "__main__":
    globOutQ = connectToQueue("GlobalOutputQueue")
    globInQ = connectToQueue("GlobalInputQueue")
    workerCount = 2;
    SUBMASTER_ID=boto.utils.boto.utils.get_instance_userdata()
   
    if SUBMASTER_ID == '':
        SUBMASTER_ID = 1
   
    while True:
        #send message asking for work
        m = sendMessageToGlobalOutputQueue(globOutQ,SUBMASTER_ID,workerCount)
        while True:
            msg_data,message = readMessageFromQueue(globInQ)
            #wait for message from global with config file name.
            m.change_visibility(120);
            if msg_data is not None:
            #we have data
                if str(msg_data['submasterid'] )== str(SUBMASTER_ID):
                    if msg_data['msgtype'] == 'Render':
                        config_File_Name = msg_data['configfile']
                        userid = msg_data['userid']
                        workerCount = msg_data['newworkercount']
                        topicarn = msg_data['topicarn'] 
                        deleteMessageFromQueue(globInQ,message)
                        msg_data = None
                        #do work
                        print 'rendering'
                        starttime = time.time()
                        anim_name  = readConfigFile(config_File_Name,userid)
                        endtime = time.time()
                        runtime = endtime-starttime
                        #connect to client out queue
                        clientoutputQueue_str = userid + anim_name+ "Queue"
                        clientOutQ = connectToQueue(clientoutputQueue_str)
                        #animation finished , send message to topic to notify user
                        sendMessageToClientTopic(topicarn,'Animation rendered in: '+str(runtime)+'seconds')
                        break;
                    #elif msg_data['msgtype'] == 'killCommand':
                        #print 'kill submaster, from cloudwatcher'
                else:
                    print 'release message, not meant for me..'
                    releaseMessage(globInQ,message,0)
            
               

