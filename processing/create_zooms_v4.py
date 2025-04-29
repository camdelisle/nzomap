from PIL import Image
import os
import io
import math
import boto3
import concurrent.futures
import requests
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ProcessingAreasV2')


def main(area_name):
    # Get all tiles for area from DynamoDB
    client = boto3.client('dynamodb')
    paginator = client.get_paginator('query')
    operation_parameters = {
        'TableName': 'ProcessingAreasV2',
        'KeyConditionExpression': 'lidar_dataset = :area_name',
        'ExpressionAttributeValues': {
            ':area_name': {'S': area_name}
        }
    }

    page_iterator = paginator.paginate(**operation_parameters)
    counter = 1

    for page in page_iterator:
        for item in page['Items']:
            if item['processed']['BOOL'] and not item['tiled']['BOOL']:
                xmin = item['xmin']['N']
                ymin = item['ymin']['N']
                area_uuid = item['uuid']['S']
                print(f'Queuing area {area_name} with UUID {area_uuid}, {counter}')
                counter += 1

                # Call the main processing lambda function
                payload = {
                    'xmin': int(xmin),
                    'ymin': int(ymin),
                    'area_name': area_name,
                    'uuid': area_uuid
                }

                lambda_client = boto3.client('lambda')
                lambda_client.invoke(
                    FunctionName='arn:aws:lambda:us-east-2:664418968878:function:nzomapCreateZooms',
                    InvocationType='Event',
                    Payload=json.dumps(payload).encode('utf-8')
                )


if __name__ == "__main__":
    area_name = 'Central_South'

    main(area_name)
