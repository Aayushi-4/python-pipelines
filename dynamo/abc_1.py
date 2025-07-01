import boto3
dynamodb = boto3.client('dynamodb')
 
response = dynamodb.list_tables()
print("Your DynamoDB tables:", response.get('TableNames', []))