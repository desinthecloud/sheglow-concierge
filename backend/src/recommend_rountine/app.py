import os
import json
import uuid
import boto3
import datetime
import logging
import time
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Environment variables
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID")
TABLE_ROUTINES = os.getenv("TABLE_ROUTINES")

# AWS clients with retry configuration
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_ROUTINES)
bedrock = boto3.client("bedrock-runtime", config=Config(
    retries={'max_attempts': 3, 'mode': 'adaptive'},
    read_timeout=60
))

def _validate_input(body: dict) -> tuple[bool, str]:
    """Enhanced input validation"""
    user_id = body.get("user_id")
    if not user_id or not isinstance(user_id, str):
        return False, "user_id is required and must be a string"

    if len(user_id) > 100:
        return False, "user_id must be less than 100 characters"

    concerns = body.get("concerns", [])
    if not isinstance(concerns, list):
        return False, "concerns must be an array"

    if len(concerns) > 10:
        return False, "too many concerns (max 10)"

    inventory = body.get("inventory", [])
    if not isinstance(inventory, list):
        return False, "inventory must be an array"

    if len(inventory) > 20:
        return False, "too many inventory items (max 20)"

    skin_type = body.get("skin_type", "combination")
    valid_skin_types = ["oily", "dry", "combination", "sensitive", "normal"]
    if skin_type not in valid_skin_types:
        return False, f"skin_type must be one of: {', '.join(valid_skin_types)}"

    return True, ""

def _create_prompt(skin_type: str, concerns: list, inventory: list) -> str:
    """Create structured prompt for Bedrock"""
    return f"""
You are an expert skincare consultant. Create a personalized skincare routine based on:

- Skin Type: {skin_type}
- Primary Concerns: {', '.join(concerns) if concerns else 'general maintenance'}
- Available Products: {', '.join(inventory) if inventory else 'recommendations needed'}

Return a JSON response with this exact structure:
{{
  "routine_id": "unique_id",
  "summary": "Brief routine description (max 200 chars)",
  "steps": [
    {{
      "time_of_day": "AM" or "PM",
      "step_name": "Step name",
      "product": "Product name or recommendation",
      "instructions": "Detailed instructions"
    }}
  ],
  "reminders": ["08:00 AM", "08:00 PM"]
}}

Keep the routine simple with 3-6 steps total. Focus on effectiveness and user compliance.
"""

def _invoke_bedrock(prompt: str) -> dict:
    """Enhanced Bedrock invocation with better error handling"""
    try:
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "temperature": 0.3,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ]
        }

        logger.info(json.dumps({
            "event": "bedrock_invoke_start",
            "model_id": BEDROCK_MODEL_ID
        }))

        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(body)
        )

        payload = json.loads(response["body"].read())
        text = payload["content"][0]["text"]

        # Try to parse as JSON
        try:
            data = json.loads(text)
            # Ensure required fields exist
            if not data.get("routine_id"):
                data["routine_id"] = str(uuid.uuid4())
            if not data.get("summary"):
                data["summary"] = "Custom skincare routine"
            if not data.get("steps"):
                data["steps"] = []
            if not data.get("reminders"):
                data["reminders"] = ["08:00 AM", "08:00 PM"]

            logger.info(json.dumps({
                "event": "bedrock_invoke_success",
                "routine_id": data["routine_id"]
            }))
            return data

        except json.JSONDecodeError as e:
            logger.error(f"Bedrock returned invalid JSON: {e}")
            # Fallback response
            return {
                "routine_id": str(uuid.uuid4()),
                "summary": "Basic skincare routine - please consult text response",
                "steps": [
                    {
                        "time_of_day": "AM",
                        "step_name": "Cleanse",
                        "product": "Gentle cleanser",
                        "instructions": text[:200] if text else "Follow basic cleansing routine"
                    }
                ],
                "reminders": ["08:00 AM", "08:00 PM"]
            }

    except ClientError as e:
        logger.error(f"Bedrock API error: {e}")
        error_code = e.response['Error']['Code']
        if error_code == 'ThrottlingException':
            raise Exception("Service temporarily overloaded, please try again")
        elif error_code == 'ValidationException':
            raise Exception("Invalid request format")
        else:
            raise Exception("AI service unavailable")
    except Exception as e:
        logger.error(f"Unexpected Bedrock error: {e}")
        raise Exception("AI service error")

def lambda_handler(event, context):
    """Enhanced lambda handler with comprehensive error handling"""
    try:
        # Log request
        logger.info(json.dumps({
            "event": "recommend_request_start",
            "request_id": context.aws_request_id if context else "local"
        }))

        # Parse and validate input
        try:
            body = json.loads(event.get("body") or "{}")
        except json.JSONDecodeError:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Invalid JSON in request body"})
            }

        # Validate required fields
        is_valid, error_msg = _validate_input(body)
        if not is_valid:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": error_msg})
            }

        # Extract validated inputs
        user_id = body["user_id"]
        skin_type = body.get("skin_type", "combination")
        concerns = body.get("concerns", [])
        inventory = body.get("inventory", [])

        # Generate recommendation
        prompt = _create_prompt(skin_type, concerns, inventory)
        recommendation = _invoke_bedrock(prompt)
        routine_id = recommendation["routine_id"]

        # Prepare DynamoDB item with TTL (90 days)
        ttl_timestamp = int(time.time()) + (90 * 24 * 60 * 60)

        item = {
            "user_id": user_id,
            "routine_id": routine_id,
            "created_at": datetime.datetime.utcnow().isoformat(),
            "skin_type": skin_type,
            "concerns": concerns,
            "inventory": inventory,
            "summary": recommendation.get("summary", ""),
            "steps": recommendation.get("steps", []),
            "reminders": recommendation.get("reminders", ["08:00 AM", "08:00 PM"]),
            "ttl": ttl_timestamp
        }

        # Save to DynamoDB
        try:
            table.put_item(Item=item)
            logger.info(json.dumps({
                "event": "routine_saved",
                "user_id": user_id,
                "routine_id": routine_id
            }))
        except ClientError as e:
            logger.error(f"DynamoDB error: {e}")
            return {
                "statusCode": 503,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Unable to save routine, please try again"})
            }

        # Return success response
        return {
            "statusCode": 201,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization"
            },
            "body": json.dumps(item)
        }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal server error"})
        }
