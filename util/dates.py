import logging
from datetime import datetime
from typing import Optional


def duration_to_seconds(duration_str: str) -> Optional[int]:
    """
    >>> duration_to_seconds('00:42:44')
    2564
    """
    try:
        h, m, s = [int(i) for i in duration_str.split(':')]
        return 3600 * h + 60 * m + s
    except:
        logging.warning(f"Failed to parse duration string: {duration_str}", exc_info=True)
        return None


def ymd_to_date(ymd_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ymd_str, '%Y-%m-%d')
    except:
        logging.warning(f"Failed to parse ymd string: {ymd_str}", exc_info=True)
        return None


if __name__ == "__main__":
    import doctest

    doctest.testmod()
