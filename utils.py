import boto3
def getListObjectBucket(bucket_name, access_key, secret_key):
    listObjects = []
    s3_client = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_key )
    response = s3_client.list_objects_v2(Bucket = bucket_name)
    
    if 'Contents' in response :
        objects = response['Contents']
    for object in objects:
        listObjects.append(object['Key'])
    return listObjects
