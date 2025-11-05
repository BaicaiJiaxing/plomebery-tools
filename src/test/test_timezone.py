from datetime import datetime

from apscheduler.triggers.cron import CronTrigger
from tzlocal import get_localzone

# 测试时区
def test_timezone():
    zone = get_localzone()
    print(zone)
    cron = CronTrigger.from_crontab("30 10 28-31 * *",timezone='Asia/Shanghai')
    print(cron)
    print(cron.timezone)
    print(cron.get_next_fire_time(previous_fire_time='2025-10-30 10:30:00'),now=datetime.now())