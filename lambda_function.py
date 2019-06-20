import json
import boto3
import os


def lambda_handler(event, context):
    fetch_object_keys()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

group_name = '/aws/lambda/' + os.environ['LAMBDA_FUNCTION_NAME']
log_stream_name_prefix = '2019/06'
print(log_stream_name_prefix)
# 適宜変更する
filter_pattern = 'ERROR'
client = boto3.client('logs')


def call_filter_log_events(log_stream_names, next_token=''):
    print('called call_filter_log_events')
    if not next_token:
        response = client.filter_log_events(
            logGroupName=group_name,
            logStreamNames=log_stream_names,
            limit=2000,
            filterPattern=filter_pattern,
        )
    else:
        response = client.filter_log_events(
            logGroupName=group_name,
            logStreamNames=log_stream_names,
            limit=2000,
            nextToken=next_token,
            filterPattern=filter_pattern,
        )

    print(len(response['events']))
    for event in response['events']:
        print('log: ' + event['message'])
    if 'nextToken' in response:
        call_filter_log_events(log_stream_names, response['nextToken'])


def fetch_object_keys(next_token=''):
    print('called fetch_object_keys')
    if not next_token:
        response = client.describe_log_streams(
            logGroupName=group_name,
            logStreamNamePrefix=log_stream_name_prefix,
            orderBy='LogStreamName',
            descending=True,
        )
    else:
        response = client.describe_log_streams(
            logGroupName=group_name,
            logStreamNamePrefix=log_stream_name_prefix,
            orderBy='LogStreamName',
            descending=True,
            nextToken=next_token,
        )
    print(len(response['logStreams']))
    for stream in response['logStreams']:
        print(stream['logStreamName'])
        call_filter_log_events([stream['logStreamName']])

    if 'nextToken' in response:
        fetch_object_keys(response['nextToken'])
    else:
        print('end')
