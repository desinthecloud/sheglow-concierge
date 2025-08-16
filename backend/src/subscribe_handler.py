import os
import json
import re
import boto3
from botocore.exceptions import ClientError
from common.db import USERS_TABLE, get_user_id_from_event

sns = boto3.client('sns')
TOPIC_ARN = os.environ['REMINDER_TOPIC_ARN']

def response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

def is_valid_email(email):
    """Basic email validation"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def handler(event, context):
    try:
        # Get user ID
        user_id = get_user_id_from_event(event)
        if not user_id:
            return response(401, {"message": "Unauthorized"})

        # Parse request body
        try:
            body = json.loads(event.get('body') or '{}')
        except json.JSONDecodeError:
            return response(400, {"message": "Invalid JSON in request body"})

        email = body.get('email')
        if not email:
            return response(400, {"message": "Email is required"})

        # Validate email format
        if not is_valid_email(email):
            return response(400, {"message": "Invalid email format"})

        # Update user profile with email
        try:
            USERS_TABLE.update_item(
                Key={'userId': user_id},
                UpdateExpression='SET #e = :e',
                ExpressionAttributeNames={'#e': 'email'},
                ExpressionAttributeValues={':e': email},
                # Ensure the user exists
                ConditionExpression='attribute_exists(userId)'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return response(404, {"message": "User not found"})
            else:
                # Log the error for debugging but don't expose internal details
                print(f"DynamoDB error: {e}")
                return response(500, {"message": "Failed to update user profile"})

        # Subscribe to SNS topic
        try:
            resp = sns.subscribe(
                TopicArn=TOPIC_ARN,
                Protocol='email',
                Endpoint=email,
                Attributes={
                    'FilterPolicy': json.dumps({'userId': [user_id]})
                }
            )
            
            # Optional: Store subscription ARN for future management
            subscription_arn = resp.get('SubscriptionArn')
            if subscription_arn and subscription_arn != 'pending confirmation':
                # You might want to store this in the database for later unsubscribe functionality
                pass
                
        except ClientError as e:
            print(f"SNS error: {e}")
            error_code = e.response['Error']['Code']
            
            if error_code == 'InvalidParameter':
                return response(400, {"message": "Invalid email address"})
            elif error_code == 'SubscriptionLimitExceeded':
                return response(429, {"message": "Subscription limit exceeded"})
            else:
                return response(500, {"message": "Failed to create subscription"})

        return response(200, {
            "message": "Subscription created successfully. Please check your email to confirm the subscription."
        })

    except Exception as e:
        # Catch-all for unexpected errors
        print(f"Unexpected error: {e}")
        return response(500, {"message": "Internal server error"})