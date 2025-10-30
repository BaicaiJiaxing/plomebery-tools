import os
from datetime import datetime
from random import randint

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from plombery import task, get_logger, Trigger, register_pipeline,get_app
from starlette.staticfiles import StaticFiles

from run.src.check_config import check_config_job
from run.src.fetch_account_data import fetch_account_data_job
from run.src.fetch_plan_data import fetch_plan_data_job


app = get_app()
register_pipeline(
    id="check",
    description="远传出账配置检查",
    tasks = [check_config_job],
    triggers = [
        Trigger(
            id="daily",
            name="每日",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 10 28-31 * *"),
        ),
    ],
)
register_pipeline(
    id="fetch",
    description="月初统计远传数据",
    tasks = [fetch_plan_data_job,fetch_account_data_job],
    triggers = [
        Trigger(
            id="daily",
            name="每天",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 10 1-10  * *"),
        ),
    ],
)
# 本地调试
if __name__ == "__main__":
   import uvicorn
   uvicorn.run("plombery:get_app", reload=True, factory=True)
