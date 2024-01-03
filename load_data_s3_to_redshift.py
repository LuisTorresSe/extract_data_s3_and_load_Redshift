
import psycopg2
from configparser import ConfigParser
from utils import getListObjectBucket

def extract_s3_and_load_redshift():
    parser = ConfigParser()
    parser.read('pipeline.conf')
    username = parser.get('aws_creds','username')
    password = parser.get('aws_creds','password')
    port = parser.get('aws_creds','port')
    host = parser.get('aws_creds','host')
    dbname = parser.get('aws_creds','database')
    iam_role = parser.get('aws_creds', 'iam_role')
    region = parser.get('aws_creds', 'region')
    squema_name = parser.get('aws_creds','squema')
    table_name = parser.get('aws_creds', 'table_name')

    rs_conn =  psycopg2.connect(
         'dbname=' + dbname + 
        ' user=' + username+
        ' password=' + password+
        ' host='+ host+
        ' port='+ port

    )
    parser = ConfigParser()
    parser.read('pipeline.conf')
    bucket_name = parser.get('aws_boto_credentials', 'bucket_name')
    account_id = parser.get('aws_boto_credentials', 'account_id')
    access_key = parser.get('aws_boto_credentials', 'access_key')
    secret_key = parser.get('aws_boto_credentials', 'secret_key')
    role_string = ('arn:aws:iam::'+ account_id +':role/'+iam_role)

    list_object_s3 = getListObjectBucket(bucket_name, access_key, secret_key)

    for path_object in list_object_s3:
        path_s3 = ('s3://'+ bucket_name + '/'+ path_object)
        sql = f'COPY {squema_name}.{table_name}'
        sql = sql + ' FROM %s'
        sql = sql + ' iam_role %s'
        sql = sql + ' FORMAT AS CSV DELIMITER' +f"';'" + 'IGNOREHEADER 1'
        sql = sql + ' region AS %s'

        cur = rs_conn.cursor()
        cur.execute(sql,( path_s3, role_string, region))
        cur.close()
        rs_conn.commit()
        rs_conn.close
        print('Cargado correctamente')

if __name__ == "__main__":
    extract_s3_and_load_redshift()

