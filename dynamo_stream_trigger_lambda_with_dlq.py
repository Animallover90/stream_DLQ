import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel("INFO")

def lambda_handler(event, context):
    logging.info("==================================== lambda run ====================================")
    logging.info(event)
    
    MessageAttributes = {}
    if 'messageAttributes' in event['Records'][0]:
        if 'retryCount' in event['Records'][0]['messageAttributes']:
            retryCount = event['Records'][0]['messageAttributes']['retryCount']['stringValue']
            MessageAttributes = {'retryCount' : {'DataType': 'Number','StringValue': str(retryCount)}}
        else:
            MessageAttributes = {'retryCount' : {'DataType': 'Number','StringValue': '0'}}
    else:
        MessageAttributes = {'retryCount' : {'DataType': 'Number','StringValue': '0'}}
    
    event_source = event['Records'][0].get('eventSource')
    if event_source == 'aws:dynamodb':
        # 하나의 Array에 많은 dict가 들어가 있으니 500개씩 작은 Array로 잘라서 Array로 감쌈
        # ex) [[{id : '1', title : 'aaa'},{id : '2', title : 'aaa'}][{id : '3', title : 'aaa'},{id : '4', title : 'aaa'}]]
        result_array = split_json_array(event['Records'], 500)
        for record in result_array:
            try:
                main(record) # record 타입은 Array
            except Exception as e:
                logging.error(e)
                response = sendQueue(dlqQueueUrl,json.dumps(record),MessageAttributes)
    elif event_source == 'aws:sqs':
        for record in event['Records']:
            try:
                main(json.loads(record['body']))
            except Exception as e:
                logging.error(e)
                response = sendQueue(dlqQueueUrl,record['body'],MessageAttributes)
    else:
        response = sendQueue(dlqQueueUrl,json.dumps(event['Records']),MessageAttributes)
        
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


def main(record):
    ## your main code
    ## record type is array
    logging.info(record)
    
    ## 아래 코드는 강제 에러 발생을 위한 코드
    a = []
    a[0]


def split_json_array(json_array, chunk_size):
    result = [json_array[i:i + chunk_size] for i in range(0, len(json_array), chunk_size)]
    return result


dlqQueueUrl="https://sqs.ap-northeast-2.amazonaws.com/your_dlq_sqs_endpoint_url"
sqs_send = boto3.client('sqs')
def sendQueue(sendQueueUrl,messageBody,messageAttributes) :
    ret = sqs_send.send_message( QueueUrl=sendQueueUrl, 
                            MessageBody=messageBody,
                            MessageAttributes=messageAttributes) 
    return ret