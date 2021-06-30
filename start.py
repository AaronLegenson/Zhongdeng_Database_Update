# -*- coding: utf-8 -*-

import schedule
from hour_job import hour_job
from tools import now_time_string
from notice import notice_hours, notice_minutes


def clock_send():
    print(now_time_string(), "[ ok ] Starting ...")
    for hour in notice_hours:
        for minute in notice_minutes:
            schedule.every().day.at("%02d:%02d" % (hour, minute)).do(hour_job)
    print(now_time_string(), "[ ok ] Triggers at {0} : {1} every day".format(
        str(["%02d" % item for item in notice_hours]).replace("'", ""),
        str(["%02d" % item for item in notice_minutes]).replace("'", "")))
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    clock_send()
