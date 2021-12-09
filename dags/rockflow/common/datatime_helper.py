from datetime import datetime, timedelta

GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'


class GmtDatetimeCheck:
    def __init__(
            self,
            gmt_str,
            days=0,
            seconds=0,
            microseconds=0,
            milliseconds=0,
            minutes=0,
            hours=0,
            weeks=0
    ):
        self.now = datetime.now()
        self.future = datetime.strptime(gmt_str, GMT_FORMAT) + timedelta(
            days=days,
            seconds=seconds,
            microseconds=microseconds,
            milliseconds=milliseconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks
        )

    @property
    def check(self):
        return self.now > self.future
