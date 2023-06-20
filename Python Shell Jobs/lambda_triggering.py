import json
import boto3
glue_client = boto3.client('glue')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    table_name = key.split('/')[1]                         # Sales_Data/Customers/Customers.csv
    
    workflow_name = 'Saad-Data-Workflow'
    glue_client.update_workflow( Name=workflow_name, DefaultRunProperties={ 'table_name': table_name, 'bucket': bucket, 'key': key} )
    response = glue_client.start_workflow_run(Name=workflow_name)
    
   

    print (bucket, key, table_name)
    return {
        'statusCode': 200
    }


