import json
import logging
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from common.db import ROUTINES_TABLE, get_user_id_from_event

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Weekday mapping from input format to ICS format
WEEKDAY_MAPPING = {
    'MON': 'MO',
    'TUE': 'TU', 
    'WED': 'WE',
    'THU': 'TH',
    'FRI': 'FR',
    'SAT': 'SA',
    'SUN': 'SU'
}

def escape_ics_text(text):
    """Escape text for ICS format according to RFC 5545."""
    if not text:
        return ""
    
    # Convert to string if not already
    text = str(text)
    
    # Escape special characters
    text = text.replace('\\', '\\\\')  # Backslash must be first
    text = text.replace(',', '\\,')     # Comma
    text = text.replace(';', '\\;')     # Semicolon
    text = text.replace('\n', '\\n')    # Newline
    text = text.replace('\r', '')       # Remove carriage returns
    
    # Limit length to prevent issues
    if len(text) > 500:
        text = text[:497] + "..."
    
    return text

def validate_time_format(time_str):
    """Validate and parse time format (HH:MM)."""
    if not isinstance(time_str, str):
        raise ValueError("Time must be a string")
    
    pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    match = re.match(pattern, time_str)
    
    if not match:
        raise ValueError("Time must be in HH:MM format")
    
    return int(match.group(1)), int(match.group(2))

def get_base_date():
    """Get a base date for recurring events (using epoch + a few days to avoid timezone issues)."""
    return "19700105"  # January 5, 1970 (Monday)

def create_ics_event(routine):
    """Create an ICS event from a routine object."""
    try:
        # Extract basic info
        title = escape_ics_text(routine.get('title', 'Routine'))
        routine_id = routine.get('routineId', 'unknown')
        when = routine.get('when', {})
        tz = routine.get('timezone', 'America/New_York')
        steps = routine.get('steps', [])
        
        # Create description
        if steps:
            description = f"SheGlow routine steps: {'; '.join(steps)}"
        else:
            description = "SheGlow routine"
        description = escape_ics_text(description)
        
        # Generate timestamp
        now = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        
        # Handle different schedule types
        schedule_type = when.get('type')
        
        if schedule_type == 'daily':
            time_str = when.get('time', '07:00')
            try:
                hh, mm = validate_time_format(time_str)
                dtstart = f"{get_base_date()}T{hh:02d}{mm:02d}00"
                
                return [
                    'BEGIN:VEVENT',
                    f'UID:{routine_id}@sheglow.app',
                    f'DTSTAMP:{now}',
                    f'SUMMARY:{title}',
                    f'DESCRIPTION:{description}',
                    f'DTSTART;TZID={tz}:{dtstart}',
                    'RRULE:FREQ=DAILY',
                    'END:VEVENT'
                ]
            except ValueError as e:
                logger.warning(f"Invalid time format for routine {routine_id}: {e}")
                return None
                
        elif schedule_type == 'weekly':
            time_str = when.get('time', '07:00')
            days = when.get('days', ['MON'])
            
            try:
                hh, mm = validate_time_format(time_str)
                
                # Convert weekdays to ICS format
                ics_days = []
                for day in days:
                    if day in WEEKDAY_MAPPING:
                        ics_days.append(WEEKDAY_MAPPING[day])
                    else:
                        logger.warning(f"Invalid weekday '{day}' for routine {routine_id}")
                
                if not ics_days:
                    logger.warning(f"No valid weekdays for routine {routine_id}")
                    return None
                
                byday = ','.join(ics_days)
                dtstart = f"{get_base_date()}T{hh:02d}{mm:02d}00"
                
                return [
                    'BEGIN:VEVENT',
                    f'UID:{routine_id}@sheglow.app',
                    f'DTSTAMP:{now}',
                    f'SUMMARY:{title}',
                    f'DESCRIPTION:{description}',
                    f'DTSTART;TZID={tz}:{dtstart}',
                    f'RRULE:FREQ=WEEKLY;BYDAY={byday}',
                    'END:VEVENT'
                ]
            except ValueError as e:
                logger.warning(f"Invalid time format for routine {routine_id}: {e}")
                return None
                
        elif schedule_type == 'cron':
            # For cron expressions, we'll create a simpler event without recurrence
            # since ICS doesn't support complex cron patterns
            logger.info(f"Cron schedule for routine {routine_id} - creating non-recurring event")
            dtstart = f"{get_base_date()}T070000"  # Default to 7 AM
            
            return [
                'BEGIN:VEVENT',
                f'UID:{routine_id}@sheglow.app',
                f'DTSTAMP:{now}',
                f'SUMMARY:{title} (Custom Schedule)',
                f'DESCRIPTION:{description} - Note: Custom cron schedule not fully supported in calendar',
                f'DTSTART;TZID={tz}:{dtstart}',
                'END:VEVENT'
            ]
        
        else:
            logger.warning(f"Unsupported schedule type '{schedule_type}' for routine {routine_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error creating ICS event for routine {routine.get('routineId', 'unknown')}: {e}")
        return None

def to_ics(routines, user_id=None):
    """Convert routines to ICS calendar format."""
    try:
        # ICS header
        lines = [
            'BEGIN:VCALENDAR',
            'VERSION:2.0',
            'PRODID:-//SheGlow//Concierge Calendar//EN',
            'CALSCALE:GREGORIAN',
            'METHOD:PUBLISH'
        ]
        
        # Add timezone definition for common timezone
        # Note: For production, you might want to add proper VTIMEZONE blocks
        
        event_count = 0
        
        # Process each routine
        for routine in routines:
            event_lines = create_ics_event(routine)
            if event_lines:
                lines.extend(event_lines)
                event_count += 1
            else:
                logger.warning(f"Skipped routine {routine.get('routineId', 'unknown')} - could not create event")
        
        # ICS footer
        lines.append('END:VCALENDAR')
        
        logger.info(f"Generated ICS calendar with {event_count} events for user {user_id}")
        
        # Join with proper line endings for ICS format
        return "\r\n".join(lines)
        
    except Exception as e:
        logger.error(f"Error generating ICS calendar: {e}")
        raise Exception("Failed to generate calendar")

def response(status, body, content_type='application/json', filename=None):
    """Create HTTP response with proper headers."""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }
    
    if content_type == 'text/calendar':
        headers['Content-Type'] = 'text/calendar; charset=utf-8'
        if filename:
            headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return {
            "statusCode": status,
            "headers": headers,
            "body": body
        }
    else:
        headers['Content-Type'] = 'application/json'
        return {
            "statusCode": status,
            "headers": headers,
            "body": json.dumps(body)
        }

def handler(event, context):
    """Main Lambda handler for ICS calendar generation."""
    try:
        # Handle OPTIONS for CORS preflight
        method = event.get('requestContext', {}).get('http', {}).get('method')
        if method == 'OPTIONS':
            return response(200, {})
        
        # Get and validate user
        user_id = get_user_id_from_event(event)
        if not user_id:
            logger.warning("Unauthorized calendar access attempt")
            return response(401, {"error": "Unauthorized", "message": "Valid authentication required"})
        
        logger.info(f"Generating calendar for user {user_id}")
        
        # Query user routines
        try:
            resp = ROUTINES_TABLE.query(
                KeyConditionExpression=Key('userId').eq(user_id)
            )
            routines = resp.get('Items', [])
            
            logger.info(f"Found {len(routines)} routines for user {user_id}")
            
        except ClientError as e:
            logger.error(f"DynamoDB error querying routines for user {user_id}: {e}")
            return response(500, {"error": "Database error", "message": "Failed to retrieve routines"})
        
        # Generate ICS calendar
        try:
            ics_content = to_ics(routines, user_id)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d')
            filename = f"sheglow-routines-{timestamp}.ics"
            
            return response(200, ics_content, 'text/calendar', filename)
            
        except Exception as e:
            logger.error(f"Error generating ICS for user {user_id}: {e}")
            return response(500, {"error": "Calendar generation failed", "message": "Failed to generate calendar"})
    
    except Exception as e:
        logger.error(f"Unexpected error in ICS handler: {e}")
        return response(500, {"error": "Internal server error", "message": "An unexpected error occurred"})