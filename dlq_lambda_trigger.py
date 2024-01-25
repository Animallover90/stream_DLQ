import json
import boto3
import datetime
import logging
from datetime import date

logger = logging.getLogger()
logger.setLevel("INFO")

def lambda_handler(event, context):
    logger.info(event)
    logger.info(context)
    for record in event['Records']:
        main(record)
    
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


s3 = boto3.client('s3')
max_retries = 5
s3_prefix_stage='dlq'
s3_bucket="your bucket name"
today = date.today()
now_date = today.strftime("%Y-%m-%d")

def main(data):
    logger.info(data)
    new_MessageAttributes = {'retryCount' : {'DataType': 'Number','StringValue': '0'}}
    retryCount = 1
    current_retry_count=0
    
    if 'messageAttributes' in data:
        new_MessageAttributes = data['messageAttributes']
        if 'retryCount' in data['messageAttributes'] :
            retryCount = int(data['messageAttributes']['retryCount']['stringValue']) + 1
            current_retry_count=int(data['messageAttributes']['retryCount']['stringValue'])

        new_MessageAttributes['retryCount'] = {'StringValue': str(retryCount),'DataType': 'Number'}

    if current_retry_count < max_retries :
        sendQueue(sqs_source_queue,data['body'],new_MessageAttributes)
    else:
        current_timestamp = datetime.datetime.now().timestamp()
        logger.info("current_retry_count = " + str(current_retry_count))
        s3_key = s3_prefix_stage+"/"+now_date+"/"+str(current_timestamp)+"_"+data['messageId']+".json"
        message_data=json.dumps({"Body":data['body'],"Attribute":new_MessageAttributes})
        s3.put_object(Body=message_data,Bucket=s3_bucket,Key=s3_key)


sqs_send = boto3.client('sqs')
sqs_source_queue="https://sqs.ap-northeast-2.amazonaws.com/your_sqs_endpoint_url"
def sendQueue(sendQueueUrl,messageBody,messageAttributes) :
    ret = sqs_send.send_message( QueueUrl=sendQueueUrl, 
                            MessageBody=messageBody,
                            MessageAttributes=messageAttributes) 
    return ret