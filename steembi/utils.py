from datetime import datetime, timezone
from nectar.utils import addTzInfo

def ensure_timezone_aware(dt):
    """
    Ensures a datetime object has timezone information (UTC).
    If it's already timezone-aware, returns it unchanged.
    If it's timezone-naive or not a datetime, adds UTC timezone.
    
    Args:
        dt: A datetime object or string representation of a datetime
        
    Returns:
        datetime: A timezone-aware datetime object (UTC)
    """
    if dt is None:
        return None
    if not isinstance(dt, datetime) or dt.tzinfo is None:
        return addTzInfo(dt)
    return dt
