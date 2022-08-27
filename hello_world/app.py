import json
import boto3
import os
import urllib.parse

print('Loading function')

s3 = boto3.client('s3')
textract = boto3.client('textract')

def getTextractData(bucketName, documentKey):
    print('Loading getTextractData')
    # Call Amazon Textract
    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucketName,
                'Name': documentKey
            }
        })

    detectedText = ''

    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            detectedText += item['Text'] + '\n'

    return detectedText



def analyse(bucketName, documentKey):
    response = textract.analyze_document(
        Document={
            'Bytes': b'bytes',
            'S3Object': {
                'Bucket': bucketName,
                'Name': documentKey
            }
        },
        FeatureTypes=[
            'TABLES','FORMS'
        ]
    )
    return response


def writeTextractToS3File(textractData, bucketName, createdS3Document):
    print('Loading writeTextractToS3File')
    generateFilePath = os.path.splitext(createdS3Document)[0] + '.txt'
    s3.put_object(Body=textractData, Bucket=bucketName, Key=generateFilePath)
    print('Generated ' + generateFilePath)



def analyze_expense(bucket, document):
    response = textract.analyze_expense(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}})
    return response


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    resultExpense = ""
    resultAnalyse = ""

    try:
        print(">>> starting analyze_expense")
        resultExpense = analyze_expense(bucket, key)
        print(resultExpense)
        print(">>> finished analyze_expense")
    except Exception as e:
        print(e)

    try:
        print(">>> starting analyse")
        resultAnalyse = analyse(bucket, key)
        print(resultAnalyse)
        print(">>> finished analyse")
    except Exception as e:
        print(e)

    try:
        print(">>> starting getTextractData")
        detectedText = getTextractData(bucket, key)
        writeTextractToS3File(detectedText, bucket, key)
        print(">>> finished getTextractData")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Process finished. resultExpense = {resultExpense}, resultAnalyse = {resultAnalyse}, detectTExt = {detectedText} ",
            }),
        }

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
