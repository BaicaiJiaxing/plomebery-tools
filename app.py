from apscheduler.triggers.cron import CronTrigger
from plombery import Trigger, register_pipeline, get_app

from src.check_config import check_config_job
from src.check_xxljob_config import check_xxl_job
from src.fetch_account_data import fetch_account_data_job
from src.fetch_plan_data import fetch_plan_data_job

app = get_app()
register_pipeline(
    id="check_config",
    description="远传出账配置检查",
    tasks = [
             check_config_job
    ],
    triggers = [
        Trigger(
            id="daily",
            name="每日",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 10 28-31 * *",timezone='Asia/Shanghai'),
        ),
    ],
)

register_pipeline(
    id="check_XXL_JOB",
    description="远传出账定时任务配置检查",
    tasks = [
             check_xxl_job
    ],
    triggers = [
        Trigger(
            id="daily",
            name="每日",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 10 28-31 * *",timezone='Asia/Shanghai'),
        ),
    ],
)
register_pipeline(
    id="fetch_account",
    description="月初统计远传数据",
    tasks = [fetch_account_data_job],
    triggers = [
        Trigger(
            id="daily",
            name="1-10号",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 10 1-10 * *",timezone='Asia/Shanghai'),
        ),
    ],
)
register_pipeline(
    id="fetch",
    description="月初统计查表计划数据",
    tasks = [fetch_plan_data_job],
    triggers = [
        Trigger(
            id="daily",
            name="每个月凌晨1:30",
            description="每天运行",
            schedule=CronTrigger.from_crontab("30 1 1 * *",timezone='Asia/Shanghai'),
        ),
    ],
)

if __name__ == "__main__":
   import uvicorn
   uvicorn.run("plombery:get_app", reload=True, factory=True)
