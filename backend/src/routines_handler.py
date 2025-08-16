import os, json, uuid, re
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from common.db import ROUTINES_TABLE, USERS_TABLE, get_user_id_from_event

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
scheduler = boto3.client('scheduler')

# Environment variables
REMINDER_TOPIC_ARN = os.environ['REMINDER_TOPIC_ARN']
SCHEDULER_ROLE_ARN = os.environ['SCHEDULER_ROLE_ARN']

# Constants for validation
VALID_WEEKDAYS = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
MAX_TITLE_LENGTH = 100
MAX_STEPS = 20
MAX_STEP_LENGTH = 200

def response(status, body, additional_headers=None):
    """Create a standardized HTTP response with CORS headers."""
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",  # Restrict this in production
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }
    
    if additional_headers:
        headers.update(additional_headers)
    
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body)
    }

def validate_time_format(time_str):
    """Validate time format (HH:MM) and return parsed hours/minutes."""
    if not isinstance(time_str, str):
        raise ValueError("Time must be a string")
    
    pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    match = re.match(pattern, time_str)
    
    if not match:
        raise ValueError("Time must be in HH:MM format (00:00-23:59)")
    
    return int(match.group(1)), int(match.group(2))

def validate_routine_data(data, is_update=False):
    """Validate routine data and return cleaned data with validation errors."""
    errors = []
    cleaned_data = {}
    
    # Validate title
    title = data.get('title')
    if not is_update or title is not None:
        if not title or not isinstance(title, str) or len(title.strip()) == 0:
            if not is_update:
                errors.append("title is required and must be a non-empty string")
        elif len(title.strip()) > MAX_TITLE_LENGTH:
            errors.append(f"title must be {MAX_TITLE_LENGTH} characters or less")
        else:
            cleaned_data['title'] = title.strip()
    
    # Validate steps
    steps = data.get('steps')
    if not is_update or steps is not None:
        if steps is None:
            if not is_update:
                cleaned_data['steps'] = []
        elif not isinstance(steps, list):
            errors.append("steps must be an array")
        elif len(steps) > MAX_STEPS:
            errors.append(f"Maximum {MAX_STEPS} steps allowed")
        else:
            cleaned_steps = []
            for i, step in enumerate(steps):
                if not isinstance(step, str):
                    errors.append(f"Step {i+1} must be a string")
                elif len(step.strip()) == 0:
                    errors.append(f"Step {i+1} cannot be empty")
                elif len(step.strip()) > MAX_STEP_LENGTH:
                    errors.append(f"Step {i+1} must be {MAX_STEP_LENGTH} characters or less")
                else:
                    cleaned_steps.append(step.strip())
            
            if not errors:
                cleaned_data['steps'] = cleaned_steps
    
    # Validate timezone
    timezone = data.get('timezone')
    if timezone is not None:
        if not isinstance(timezone, str) or len(timezone.strip()) == 0:
            errors.append("timezone must be a non-empty string")
        else:
            cleaned_data['timezone'] = timezone.strip()
    
    # Validate when (schedule)
    when = data.get('when')
    if not is_update or when is not None:
        if when is None:
            if not is_update:
                cleaned_data['when'] = {"type": "daily", "time": "07:00"}
        elif not isinstance(when, dict):
            errors.append("when must be an object")
        else:
            schedule_type = when.get('type')
            if schedule_type not in ['daily', 'weekly', 'cron']:
                errors.append("when.type must be 'daily', 'weekly', or 'cron'")
            else:
                cleaned_when = {'type': schedule_type}
                
                if schedule_type in ['daily', 'weekly']:
                    time_str = when.get('time', '07:00')
                    try:
                        validate_time_format(time_str)
                        cleaned_when['time'] = time_str
                    except ValueError as e:
                        errors.append(f"when.time: {str(e)}")
                
                if schedule_type == 'weekly':
                    days = when.get('days', ['MON'])
                    if not isinstance(days, list) or not days:
                        errors.append("when.days must be a non-empty array for weekly schedules")
                    else:
                        invalid_days = [d for d in days if d not in VALID_WEEKDAYS]
                        if invalid_days:
                            errors.append(f"Invalid days: {', '.join(invalid_days)}. Valid options: {', '.join(VALID_WEEKDAYS)}")
                        else:
                            cleaned_when['days'] = days
                
                if schedule_type == 'cron':
                    expression = when.get('expression')
                    if not expression or not isinstance(expression, str):
                        errors.append("when.expression is required for cron schedules")
                    else:
                        cleaned_when['expression'] = expression.strip()
                
                if not errors:
                    cleaned_data['when'] = cleaned_when
    
    return cleaned_data, errors

def create_schedule(user_id, routine):
    """Create EventBridge schedule for routine."""
    try:
        tz = routine.get('timezone', 'America/New_York')
        name = f"sheglow-{user_id[:8]}-{routine['routineId'][:8]}"
        when = routine.get('when', {})
        schedule_expression = None

        if when.get('type') == 'daily':
            hh, mm = validate_time_format(when.get('time', '07:00'))
            schedule_expression = f"cron({mm} {hh} * * ? *)"
        
        elif when.get('type') == 'weekly':
            hh, mm = validate_time_format(when.get('time', '07:00'))
            days = ','.join(when.get('days', ['MON']))
            schedule_expression = f"cron({mm} {hh} ? * {days} *)"
        
        elif when.get('type') == 'cron':
            schedule_expression = when.get('expression')

        if not schedule_expression:
            raise ValueError('Unsupported or invalid schedule configuration')

        input_body = json.dumps({
            "type": "routine.reminder",
            "userId": user_id,
            "routineId": routine['routineId'],
            "title": routine.get('title'),
            "steps": routine.get('steps', [])
        })

        scheduler.create_schedule(
            Name=name,
            ScheduleExpression=schedule_expression,
            ScheduleExpressionTimezone=tz,
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": REMINDER_TOPIC_ARN,
                "RoleArn": SCHEDULER_ROLE_ARN,
                "Input": input_body,
                "MessageAttributes": {
                  "userId": {"DataType": "String", "StringValue": user_id}
    }
            },
            Description=f"SheGlow reminder for {user_id} – {routine.get('title', 'Untitled')}"
        )
        
        logger.info(f"Created schedule {name} for user {user_id}")
        return name
        
    except ClientError as e:
        logger.error(f"Failed to create schedule for user {user_id}: {e}")
        raise Exception(f"Failed to create reminder schedule: {e}")
    except Exception as e:
        logger.error(f"Error creating schedule: {e}")
        raise

def delete_schedule(name):
    """Delete EventBridge schedule."""
    try:
        scheduler.delete_schedule(Name=name)
        logger.info(f"Deleted schedule {name}")
    except scheduler.exceptions.ResourceNotFoundException:
        logger.warning(f"Schedule {name} not found for deletion")
    except ClientError as e:
        logger.error(f"Failed to delete schedule {name}: {e}")
        # Don't raise here - we want to continue with other operations

def handler(event, context):
    """Main Lambda handler for routine operations."""
    try:
        # Extract method and user ID
        method = event.get('requestContext', {}).get('http', {}).get('method')
        user_id = get_user_id_from_event(event)
        
        if not user_id:
            logger.warning("Unauthorized access attempt")
            return response(401, {"error": "Unauthorized", "message": "Valid authentication required"})

        logger.info(f"Processing {method} request for user {user_id}")

        # Handle OPTIONS for CORS preflight
        if method == 'OPTIONS':
            return response(200, {})

        # Handle GET - list user routines
        if method == 'GET':
            try:
                resp = ROUTINES_TABLE.query(
                    KeyConditionExpression=Key('userId').eq(user_id)
                )
                routines = resp.get('Items', [])
                logger.info(f"Retrieved {len(routines)} routines for user {user_id}")
                return response(200, routines)
                
            except ClientError as e:
                logger.error(f"Error querying routines for user {user_id}: {e}")
                return response(500, {"error": "Database error", "message": "Failed to retrieve routines"})

        # Parse request body for POST/PUT/DELETE
        try:
            body = json.loads(event.get('body') or '{}')
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in request body: {e}")
            return response(400, {"error": "Invalid JSON", "message": "Request body must be valid JSON"})

        # Handle POST - create new routine
        if method == 'POST':
            try:
                # Validate input
                cleaned_data, validation_errors = validate_routine_data(body)
                if validation_errors:
                    logger.warning(f"Validation errors for new routine: {validation_errors}")
                    return response(400, {
                        "error": "Validation failed",
                        "message": "Invalid input data",
                        "details": validation_errors
                    })

                # Create routine object
                routine = {
                    'userId': user_id,
                    'routineId': str(uuid.uuid4()),
                    'title': cleaned_data.get('title', 'My Routine'),
                    'steps': cleaned_data.get('steps', []),
                    'timezone': cleaned_data.get('timezone', 'America/New_York'),
                    'when': cleaned_data.get('when', {"type": "daily", "time": "07:00"}),
                    'createdAt': datetime.utcnow().isoformat()
                }

                # Create schedule first
                schedule_name = create_schedule(user_id, routine)
                routine['scheduleName'] = schedule_name

                # Save to database
                ROUTINES_TABLE.put_item(Item=routine)
                
                logger.info(f"Created routine {routine['routineId']} for user {user_id}")
                return response(201, routine)

            except Exception as e:
                logger.error(f"Error creating routine for user {user_id}: {e}")
                # Cleanup schedule if it was created
                if 'schedule_name' in locals():
                    delete_schedule(schedule_name)
                return response(500, {"error": "Internal server error", "message": "Failed to create routine"})

        # Handle PUT - update existing routine
        if method == 'PUT':
            try:
                # Get routine ID from path
                path_params = event.get('pathParameters', {})
                routine_id = path_params.get('routineId')
                
                if not routine_id:
                    return response(400, {"error": "Missing parameter", "message": "routineId is required"})

                # Validate input
                cleaned_data, validation_errors = validate_routine_data(body, is_update=True)
                if validation_errors:
                    logger.warning(f"Validation errors for routine update: {validation_errors}")
                    return response(400, {
                        "error": "Validation failed",
                        "message": "Invalid input data",
                        "details": validation_errors
                    })

                # Fetch current routine
                current = ROUTINES_TABLE.get_item(
                    Key={'userId': user_id, 'routineId': routine_id}
                ).get('Item')
                
                if not current:
                    return response(404, {"error": "Not found", "message": "Routine not found"})

                # Update fields
                for field in ['title', 'steps', 'timezone']:
                    if field in cleaned_data:
                        current[field] = cleaned_data[field]

                # Handle schedule updates
                if 'when' in cleaned_data:
                    # Delete old schedule
                    if current.get('scheduleName'):
                        delete_schedule(current['scheduleName'])
                    
                    # Create new schedule
                    current['when'] = cleaned_data['when']
                    current['scheduleName'] = create_schedule(user_id, current)

                # Save updated routine
                ROUTINES_TABLE.put_item(Item=current)
                
                logger.info(f"Updated routine {routine_id} for user {user_id}")
                return response(200, current)

            except Exception as e:
                logger.error(f"Error updating routine {routine_id} for user {user_id}: {e}")
                return response(500, {"error": "Internal server error", "message": "Failed to update routine"})

        # Handle DELETE - delete routine
        if method == 'DELETE':
            try:
                # Get routine ID from path
                path_params = event.get('pathParameters', {})
                routine_id = path_params.get('routineId')
                
                if not routine_id:
                    return response(400, {"error": "Missing parameter", "message": "routineId is required"})

                # Get routine to find schedule name
                item = ROUTINES_TABLE.get_item(
                    Key={'userId': user_id, 'routineId': routine_id}
                ).get('Item')
                
                # Delete schedule if exists
                if item and item.get('scheduleName'):
                    delete_schedule(item['scheduleName'])

                # Delete from database
                ROUTINES_TABLE.delete_item(
                    Key={'userId': user_id, 'routineId': routine_id}
                )
                
                logger.info(f"Deleted routine {routine_id} for user {user_id}")
                return response(204, {})

            except Exception as e:
                logger.error(f"Error deleting routine {routine_id} for user {user_id}: {e}")
                return response(500, {"error": "Internal server error", "message": "Failed to delete routine"})

        # Handle unsupported methods
        else:
            logger.warning(f"Method {method} not allowed")
            return response(405, {"error": "Method not allowed", "message": f"Method {method} is not supported"})

    except Exception as e:
        logger.error(f"Unexpected error in handler: {e}")
        return response(500, {"error": "Internal server error", "message": "An unexpected error occurred"})