import boto, time
from boto.s3.key import Key

s3 = boto.connect_s3()
UNIQUE_BUCKET_NAME = 'com.shadowcaster.'
WORKER_TEMP_DIR = '/tmp'

def userbucketexists():
    #TBD:check if user bucket exists
    a = 0

def getBucketName(userid):
    bucketName = UNIQUE_BUCKET_NAME+str(userid)
    return bucketName
    
def createuserbucket(userid):#having bucket per animation
    # Create a new bucket. Buckets must have a globally unique name.
    bucket = s3.create_bucket(UNIQUE_BUCKET_NAME+userid)
    time.sleep(2)
    return bucket

def deleteuserbucket(userid):
    #delete user bucket, only works if empty
    bucket = s3.get_bucket(UNIQUE_BUCKET_NAME+userid)
    bucket.delete()

def deletefile(filename):
    #delete user bucket, only works if empty
    bucket = s3.get_bucket(UNIQUE_BUCKET_NAME+userid)
    bucket.delete_key(filename)

def uploadfile(localFileName,remoteFileName,userid):
    #add frame file to user bucket
    bucket = createuserbucket(userid)
    k = Key(bucket)
    k.key = remoteFileName
    k.set_contents_from_filename(localFileName)
    return 'done';

def checkFileExists(filename,userid):
    bucket = s3.get_bucket(UNIQUE_BUCKET_NAME+userid)
    key = bucket.get_key(filename) #must include file extenstion .pem
    if key is None:
        return False
    else:
        return True
    

def downloadfile(filename,userid):
    #download frame file to user bucket
    bucket = s3.get_bucket(UNIQUE_BUCKET_NAME+userid)
    key = bucket.get_key(filename) #must include file extenstion .pem
    key.get_contents_to_filename( WORKER_TEMP_DIR + '/' + filename)
    return WORKER_TEMP_DIR + '/' + filename
    
#TBD:instead of printing  , values need to be returned in an object
def getAllObjectsInBucket():
    for key in bucket.list():
        print "{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified,
                )
