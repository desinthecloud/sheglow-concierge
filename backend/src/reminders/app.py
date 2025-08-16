import json
import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

def lambda_handler(event, context):
    try:
        # This function is triggered by CloudWatch Events (daily cron)
        logger.info("Daily reminder function triggered")
        
        # Get all active routines (simplified - in production you'd paginate)
        routines_table = dynamodb.Table(os.environ['TABLE_ROUTINES'])
        
        # Scan for recent routines (last 30 days)
        from datetime import timedelta
        cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
        
        response = routines_table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('created_at').gt(cutoff_date),
            Limit=100  # Limit for demo
        )
        
        reminder_count = 0
        topic_arn = os.environ['REMINDER_TOPIC_ARN']
        
        for item in response['Items']:
            user_id = item.get('user_id')
            routine_id = item.get('routine_id')
            
            # Send reminder (customize message as needed)
            message = f"""
            🌟 SheGlow Reminder 🌟
            
            Don't forget your personalized skincare routine!
            
            User: {user_id}
            Routine ID: {routine_id}
            
            Consistency is key to healthy, glowing skin! ✨
            """
            
            sns.publish(
                TopicArn=topic_arn,
                Message=message,
                Subject="Your Daily SheGlow Skincare Reminder"
            )
            
            reminder_count += 1
        
        logger.info(f"Sent {reminder_count} reminders")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully sent {reminder_count} reminders',
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }