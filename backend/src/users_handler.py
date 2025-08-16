import json
import logging
from botocore.exceptions import ClientError
from common.db import USERS_TABLE, get_user_id_from_event

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Valid skin types and concerns for validation
VALID_SKIN_TYPES = ['dry', 'oily', 'combination', 'normal', 'sensitive']
VALID_CONCERNS = ['acne', 'hyperpigmentation', 'wrinkles', 'dark_spots', 'dryness', 'oiliness', 'sensitivity']

def response(status, body, additional_headers=None):
    """Create a standardized HTTP response with CORS headers."""
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",  # Restrict this in production
        "Access-Control-Allow-Methods": "GET, PUT, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }
    
    if additional_headers:
        headers.update(additional_headers)
    
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body)
    }

def validate_profile_data(data):
    """Validate profile data and return cleaned data with validation errors."""
    errors = []
    cleaned_data = {}
    
    # Validate displayName (optional but must be string if provided)
    display_name = data.get('displayName')
    if display_name is not None:
        if not isinstance(display_name, str) or len(display_name.strip()) == 0:
            errors.append("displayName must be a non-empty string")
        elif len(display_name.strip()) > 100:
            errors.append("displayName must be 100 characters or less")
        else:
            cleaned_data['displayName'] = display_name.strip()
    
    # Validate skinType (optional)
    skin_type = data.get('skinType')
    if skin_type is not None:
        if skin_type not in VALID_SKIN_TYPES:
            errors.append(f"skinType must be one of: {', '.join(VALID_SKIN_TYPES)}")
        else:
            cleaned_data['skinType'] = skin_type
    
    # Validate concerns (optional, must be array)
    concerns = data.get('concerns')
    if concerns is not None:
        if not isinstance(concerns, list):
            errors.append("concerns must be an array")
        else:
            invalid_concerns = [c for c in concerns if c not in VALID_CONCERNS]
            if invalid_concerns:
                errors.append(f"Invalid concerns: {', '.join(invalid_concerns)}. Valid options: {', '.join(VALID_CONCERNS)}")
            else:
                cleaned_data['concerns'] = list(set(concerns))  # Remove duplicates
    
    # Validate timezone (optional)
    timezone = data.get('timezone')
    if timezone is not None:
        if not isinstance(timezone, str) or len(timezone.strip()) == 0:
            errors.append("timezone must be a non-empty string")
        else:
            cleaned_data['timezone'] = timezone.strip()
    
    # Validate email (optional)
    email = data.get('email')
    if email is not None:
        if not isinstance(email, str) or '@' not in email:
            errors.append("email must be a valid email address")
        elif len(email) > 255:
            errors.append("email must be 255 characters or less")
        else:
            cleaned_data['email'] = email.strip().lower()
    
    return cleaned_data, errors

def get_user_profile(user_id):
    """Get user profile from database with error handling."""
    try:
        response = USERS_TABLE.get_item(Key={'userId': user_id})
        item = response.get('Item')
        
        if not item:
            # Return default profile structure if user doesn't exist
            return {
                'userId': user_id,
                'displayName': None,
                'skinType': None,
                'concerns': [],
                'timezone': 'America/New_York',
                'email': None
            }
        
        return item
        
    except ClientError as e:
        logger.error(f"DynamoDB error getting user {user_id}: {e}")
        raise Exception("Database error occurred")

def update_user_profile(user_id, profile_data):
    """Update user profile with merge logic."""
    try:
        # Get existing profile first
        existing_profile = get_user_profile(user_id)
        
        # Merge new data with existing (only update provided fields)
        updated_profile = existing_profile.copy()
        updated_profile.update(profile_data)
        updated_profile['userId'] = user_id  # Ensure userId is always set
        
        # Save to database
        USERS_TABLE.put_item(Item=updated_profile)
        
        logger.info(f"Updated profile for user {user_id}")
        return updated_profile
        
    except ClientError as e:
        logger.error(f"DynamoDB error updating user {user_id}: {e}")
        raise Exception("Database error occurred")

def handler(event, context):
    """Main Lambda handler for user profile operations."""
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
        
        # Handle GET - retrieve user profile
        if method == 'GET':
            try:
                profile = get_user_profile(user_id)
                return response(200, profile)
                
            except Exception as e:
                logger.error(f"Error getting profile for user {user_id}: {e}")
                return response(500, {"error": "Internal server error", "message": "Failed to retrieve profile"})
        
        # Handle PUT - update user profile
        elif method == 'PUT':
            try:
                # Parse request body
                body_str = event.get('body') or '{}'
                try:
                    body = json.loads(body_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in request body: {e}")
                    return response(400, {"error": "Invalid JSON", "message": "Request body must be valid JSON"})
                
                # Validate input data
                cleaned_data, validation_errors = validate_profile_data(body)
                
                if validation_errors:
                    logger.warning(f"Validation errors for user {user_id}: {validation_errors}")
                    return response(400, {
                        "error": "Validation failed",
                        "message": "Invalid input data",
                        "details": validation_errors
                    })
                
                # Update profile
                updated_profile = update_user_profile(user_id, cleaned_data)
                return response(200, updated_profile)
                
            except Exception as e:
                logger.error(f"Error updating profile for user {user_id}: {e}")
                return response(500, {"error": "Internal server error", "message": "Failed to update profile"})
        
        # Handle unsupported methods
        else:
            logger.warning(f"Method {method} not allowed")
            return response(405, {"error": "Method not allowed", "message": f"Method {method} is not supported"})
    
    except Exception as e:
        logger.error(f"Unexpected error in handler: {e}")
        return response(500, {"error": "Internal server error", "message": "An unexpected error occurred"})