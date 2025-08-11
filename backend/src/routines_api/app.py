import os
import json
import boto3
import logging
import datetime
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

TABLE_ROUTINES = os.getenv("TABLE_ROUTINES")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_ROUTINES)

def _response(status_code: int, body: dict, content_type: str = "application/json") -> dict:
    """Standardized response with CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": content_type,
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Api-Key"
        },
        "body": json.dumps(body) if isinstance(body, (dict, list)) else body
    }

def lambda_handler(event, context):
    """Enhanced API handler with proper routing and error handling"""
    try:
        # Extract request details
        route = event.get("requestContext", {}).get("http", {}).get("path", "")
        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
        params = event.get("queryStringParameters") or {}

        logger.info(json.dumps({
            "event": "api_request",
            "method": method,
            "path": route,
            "request_id": context.aws_request_id if context else "local"
        }))

        # Handle CORS preflight
        if method == "OPTIONS":
            return _response(200, {})

        # Health check endpoint
        if method == "GET" and route.endswith("/health"):
            return _response(200, {
                "status": "healthy",
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "version": "1.0"
            })

        # GET /routines - List user's routines
        if method == "GET" and route.endswith("/routines"):
            user_id = params.get("user_id")
            if not user_id:
                return _response(400, {"error": "user_id query parameter is required"})

            try:
                # Query with GSI for better performance if needed
                response = table.query(
                    KeyConditionExpression=Key("user_id").eq(user_id),
                    ScanIndexForward=False,  # Most recent first
                    Limit=50  # Reasonable limit
                )
                routines = response.get("Items", [])

                logger.info(json.dumps({
                    "event": "routines_retrieved",
                    "user_id": user_id,
                    "count": len(routines)
                }))

                return _response(200, {
                    "routines": routines,
                    "count": len(routines)
                })

            except ClientError as e:
                logger.error(f"DynamoDB error in GET /routines: {e}")
                return _response(503, {"error": "Unable to retrieve routines"})

        # POST /routines - Create/update routine
        if method == "POST" and route.endswith("/routines"):
            try:
                body = json.loads(event.get("body") or "{}")
            except json.JSONDecodeError:
                return _response(400, {"error": "Invalid JSON"})

            user_id = body.get("user_id")
            routine_id = body.get("routine_id")

            if not user_id or not routine_id:
                return _response(400, {"error": "user_id and routine_id are required"})

            try:
                table.put_item(Item=body)
                logger.info(json.dumps({
                    "event": "routine_created",
                    "user_id": user_id,
                    "routine_id": routine_id
                }))
                return _response(201, body)

            except ClientError as e:
                logger.error(f"DynamoDB error in POST /routines: {e}")
                return _response(503, {"error": "Unable to save routine"})

        # DELETE /routines/{routine_id}
        if method == "DELETE" and "/routines/" in route:
            user_id = params.get("user_id")
            routine_id = route.split("/routines/")[-1]

            if not user_id or not routine_id:
                return _response(400, {"error": "user_id parameter and routine_id in path are required"})

            try:
                table.delete_item(
                    Key={"user_id": user_id, "routine_id": routine_id}
                )
                logger.info(json.dumps({
                    "event": "routine_deleted",
                    "user_id": user_id,
                    "routine_id": routine_id
                }))
                return _response(204, {})

            except ClientError as e:
                logger.error(f"DynamoDB error in DELETE /routines: {e}")
                return _response(503, {"error": "Unable to delete routine"})

        # Route not found
        return _response(404, {"error": "Endpoint not found"})

    except Exception as e:
        logger.error(f"Unexpected API error: {str(e)}")
        return _response(500, {"error": "Internal server error"})
