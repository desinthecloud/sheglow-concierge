import os
import boto3
from boto3.dynamodb.conditions import Key
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resources
ddb = boto3.resource('dynamodb')
USERS_TABLE = ddb.Table(os.environ['USERS_TABLE'])
ROUTINES_TABLE = ddb.Table(os.environ['ROUTINES_TABLE'])

def get_user_id_from_event(event):
    """
    Extract user ID from Cognito JWT claims in the event.
    
    Args:
        event: AWS Lambda event object
        
    Returns:
        str: User ID (sub claim) or None if not found
        
    Raises:
        ValueError: If event structure is invalid
    """
    try:
        # Navigate through the event structure safely
        request_context = event.get('requestContext', {})
        authorizer = request_context.get('authorizer', {})
        jwt = authorizer.get('jwt', {})
        claims = jwt.get('claims', {})
        
        # Get the 'sub' claim which is the stable user identifier
        user_id = claims.get('sub')
        
        if not user_id:
            logger.warning("No 'sub' claim found in JWT")
            return None
            
        logger.info(f"Extracted user ID: {user_id}")
        return user_id
        
    except Exception as e:
        logger.error(f"Error extracting user ID from event: {str(e)}")
        raise ValueError(f"Invalid event structure: {str(e)}")

def validate_user_exists(user_id):
    """
    Check if a user exists in the database.
    
    Args:
        user_id: The user ID to check
        
    Returns:
        bool: True if user exists, False otherwise
    """
    if not user_id:
        return False
        
    try:
        response = USERS_TABLE.get_item(Key={'userId': user_id})
        return 'Item' in response
    except Exception as e:
        logger.error(f"Error checking if user exists: {str(e)}")
        return False

def get_authenticated_user_id(event):
    """
    Get and validate user ID from event.
    
    Args:
        event: AWS Lambda event object
        
    Returns:
        str: Validated user ID
        
    Raises:
        ValueError: If user ID is missing or invalid
        Exception: If user doesn't exist in database
    """
    user_id = get_user_id_from_event(event)
    
    if not user_id:
        raise ValueError("User ID not found in authentication context")
    
    # Optionally validate user exists (remove if not needed for performance)
    if not validate_user_exists(user_id):
        logger.warning(f"User {user_id} not found in database")
        # You might want to create the user here or just continue
        # raise Exception(f"User {user_id} not found")
    
    return user_id

# Alternative: More defensive version with fallbacks
def get_user_id_from_event_defensive(event):
    """
    More defensive version that tries multiple claim sources.
    """
    try:
        claims = event.get('requestContext', {}).get('authorizer', {}).get('jwt', {}).get('claims', {})
        
        # Try 'sub' first (recommended)
        user_id = claims.get('sub')
        if user_id:
            return user_id
            
        # Fallback to 'username' if sub not available
        user_id = claims.get('username')
        if user_id:
            logger.warning("Using 'username' claim as fallback for user ID")
            return user_id
            
        # Fallback to 'email' if others not available
        user_id = claims.get('email')
        if user_id:
            logger.warning("Using 'email' claim as fallback for user ID")
            return user_id
            
        return None
        
    except Exception as e:
        logger.error(f"Error in defensive user ID extraction: {str(e)}")
        return None