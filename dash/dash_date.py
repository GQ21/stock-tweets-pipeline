from datetime import datetime, timedelta
import pytz

def get_yesterday() -> tuple:
    """Gets yesterdays morning and night date time."""
    today = datetime.now(pytz.timezone("America/New_York")).replace(tzinfo=None)
    yesterday_morning = (today - timedelta(days=1)).replace(
        tzinfo=None, hour=0, minute=0, second=0, microsecond=0
    )
    yesterday_night = (today - timedelta(days=1)).replace(
        tzinfo=None, hour=23, minute=59, second=59, microsecond=0
    )

    return yesterday_morning, yesterday_night