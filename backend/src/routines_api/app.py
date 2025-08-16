import os
import json
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Environment variables
TABLE_ROUTINES = os.getenv("TABLE_ROUTINES")

# AWS clients with retry configuration
dynamodb = boto3.resource("dynamodb", config=Config(
    retries={'max_attempts': 3, 'mode': 'adaptive'}
))
table = dynamodb.Table(TABLE_ROUTINES)

def _validate_user_id(user_id: str) -> tuple[bool, str]:
    """Validate user_id parameter"""
    if not user_id or not isinstance(user_id, str):
        return False, "user_id is required and must be a string"
    
    if len(user_id) > 100:
        return False, "user_id must be less than 100 characters"
    
    return True, ""

def _create_cors_headers() -> dict:
    """Create standard CORS headers"""
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

def _handle_health_check() -> dict:
    """Handle health check endpoint"""
    return {
        "statusCode": 200,
        "headers": _create_cors_headers(),
        "body": json.dumps({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "SheGlow Routines API",
            "version": "1.0"
        })
    }

def _handle_get_routines(query_params: dict) -> dict:
    """Handle GET /routines"""
    user_id = query_params.get("user_id")
    
    # Validate user_id
    is_valid, error_msg = _validate_user_id(user_id)
    if not is_valid:
        return {
            "statusCode": 400,
            "headers": _create_cors_headers(),
            "body": json.dumps({"error": error_msg})
        }
    
    try:
        # Query routines for user
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id),
            ScanIndexForward=False,  # Latest first
            Limit=20  # Reasonable limit
        )
        
        # Format response
        routines = []
        for item in response['Items']:
            routine = {
                "routine_id": item.get("routine_id"),
                "created_at": item.get("created_at"),
                "skin_type": item.get("skin_type"),
                "concerns": item.get("concerns", []),
                "inventory": item.get("inventory", []),
                "summary": item.get("summary", ""),
                "steps": item.get("steps", []),
                "reminders": item.get("reminders", [])
            }
            routines.append(routine)
        
        logger.info(json.dumps({
            "event": "get_routines_success",
            "user_id": user_id,
            "count": len(routines)
        }))
        
        return {
            "statusCode": 200,
            "headers": _create_cors_headers(),
            "body": json.dumps({
                "user_id": user_id,
                "routines": routines,
                "count": len(routines)
            })
        }
        
    except ClientError as e:
        logger.error(f"DynamoDB error: {e}")
        return {
            "statusCode": 503,
            "headers": _create_cors_headers(),
            "body": json.dumps({"error": "Unable to fetch routines, please try again"})
        }

def _handle_delete_routine(path: str, query_params: dict) -> dict:
    """Handle DELETE /routines/{routine_id}"""
    user_id = query_params.get("user_id")
    routine_id = path.split('/')[-1] if '/' in path else None
    
    # Validate inputs
    is_valid, error_msg = _validate_user_id(user_id)
    if not is_valid:
        return {
            "statusCode": 400,
            "headers": _create_cors_headers(),
            "body": json.dumps({"error": error_msg})
        }
    
    if not routine_id or len(routine_id) < 10:
        return {
            "statusCode": 400,
            "headers": _create_cors_headers(),
            "body": json.dumps({"error": "Valid routine_id is required"})
        }
    
    try:
        # Delete routine
        table.delete_item(
            Key={
                "user_id": user_id,
                "routine_id": routine_id
            },
            ConditionExpression="attribute_exists(user_id)"  # Ensure item exists
        )
        
        logger.info(json.dumps({
            "event": "routine_deleted",
            "user_id": user_id,
            "routine_id": routine_id
        }))
        
        return {
            "statusCode": 200,
            "headers": _create_cors_headers(),
            "body": json.dumps({
                "message": "Routine deleted successfully",
                "routine_id": routine_id
            })
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                "statusCode": 404,
                "headers": _create_cors_headers(),
                "body": json.dumps({"error": "Routine not found"})
            }
        else:
            logger.error(f"DynamoDB error: {e}")
            return {
                "statusCode": 503,
                "headers": _create_cors_headers(),
                "body": json.dumps({"error": "Unable to delete routine, please try again"})
            }

def lambda_handler(event, context):
    """Enhanced lambda handler for routines API"""
    try:
        # Log request
        logger.info(json.dumps({
            "event": "routines_api_request",
            "request_id": context.aws_request_id if context else "local"
        }))
        
        # Extract request details
        http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
        path = event.get('path') or event.get('rawPath', '')
        query_params = event.get('queryStringParameters') or {}
        
        # Handle CORS preflight
        if http_method == 'OPTIONS':
            return {
                "statusCode": 200,
                "headers": _create_cors_headers(),
                "body": ""
            }
        
        # Route requests
        if path == '/health' and http_method == 'GET':
            return _handle_health_check()
        
        elif path == '/routines' and http_method == 'GET':
            return _handle_get_routines(query_params)
        
        elif '/routines/' in path and http_method == 'DELETE':
            return _handle_delete_routine(path, query_params)
        
        else:
            return {
                "statusCode": 404,
                "headers": _create_cors_headers(),
                "body": json.dumps({"error": "Endpoint not found"})
            }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": _create_cors_headers(),
            "body": json.dumps({"error": "Internal server error"})
        }