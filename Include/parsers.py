from datetime import date, datetime
from typing import Optional


def parse_int(val):
    """
    Safely parse an int from val. Returns None if val is empty or invalid.
    """
    try:
        if val == '' or val is None:
            return None
        return int(val)
    except (ValueError, TypeError):
        return None
    
def parse_float(val):
    """
    Safely parse a float from val. Handles ',' as a decimal separator.
    Returns None if val is empty or invalid.
    """
    try:
        if val == '' or val is None:
            return None
        if isinstance(val, str):
            val = val.replace(',', '.')  # Replace ',' with '.' for decimal points
        return float(val)
    except (ValueError, TypeError):
        return None
    
def parse_date(date_input) -> Optional[datetime]:
    """
    Parse a date string in 'YYYY-MM-DD' format or a date object to a datetime object.
    Returns None if the input is None or invalid.
    """
    from datetime import datetime, date
    try:
        if isinstance(date_input, str):
            return datetime.strptime(date_input, "%Y-%m-%d")
        elif isinstance(date_input, date) and not isinstance(date_input, datetime):
            return datetime.combine(date_input, datetime.min.time())
        elif isinstance(date_input, datetime):
            return date_input
        return None
    except Exception:
        return None
