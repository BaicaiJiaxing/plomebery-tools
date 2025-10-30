import datetime
import logging

from plombery import task, get_logger

from run.src.utils import logger
from run.src.rules.CompanyEnum import CompanyNameEnum
from run.src.sms.sms_client import SMSClient
from run.src.utils import ConfigLoader, dbutils

config = ConfigLoader.ConfigLoader()
sms_client = SMSClient(base_url=config.get_sms_api())
job_name = "fetch_account_data"
job_config = config.get_config_by_job(job_name)
logger = logging.getLogger(__name__)


def fill_sql(job_config):
    """填充config中的sql参数并返回最终可执行的sql"""
    sql_template = job_config.get("sql")

    # 获取当前日期
    today = datetime.date.today()
    current_month = today.strftime("%Y%m")  # 202509 -> mr_month 格式

    # 上一年同月
    last_year_date = today.replace(year=today.year - 1)
    last_year_month = last_year_date.strftime("%Y-%m")

    # 上一个月
    first_day_of_this_month = today.replace(day=1)
    previous_month_date = first_day_of_this_month - datetime.timedelta(days=1)
    previous_month = previous_month_date.strftime("%Y-%m")

    # 填充 SQL 模板
    sql_filled = sql_template.format(
        previous=previous_month_date.year,
        previous_month=previous_month,
        last_year=last_year_date.year,
        last_year_month=last_year_month
    )
    return sql_filled

def fetch_data_by_company(company: str):
    """按分公司获取数据"""
    company_config =  config.get_database(company)
    db = dbutils.DBUtils(company_config)
    db.connect()

    # 获取公司对应数据库配置（如果有不同公司）
    sql = job_config.get('sql')
    users = db.query(sql)
    db.close()
    # 返回格式 [{'count': 139930, 'seq': 1}, {'count': 0, 'seq': 2}, {'count': 134194, 'seq': 3}]
    # 1-本周期 2-上周期 3-去年同期
    data = {
        '本周期户表已出账(支)': users[0]['count'] if len(users) > 0 else -1,
        '本周期户表应出账(支)': users[1]['count'] if len(users) > 1 else -1,
        '本周期大路表已出账(支)': users[2]['count'] if len(users) > 0 else -1,
        '本周期大路表应出账(支)': users[3]['count'] if len(users) > 1 else -1,
    }

    res = {
        'company': company,
        'data': data
    }

    logger.info("处理后数据：%s", res)
    return res
def build_excel():
    '''查询出来的结果写入到excel'''



def build_sms_message():
    # 从config.yaml中获取该定时任务需要执行的分公司名称，注意获取数据并组装成短信内容
    companies = job_config.get("companies")
    res = []
    message = "【生产环境】截至目前本月远传出账情况："
    # 获取所有分公司数据
    for company in companies:
        data = fetch_data_by_company(company)
        res.append(data)
        message += '\n'
        message += CompanyNameEnum.get_name(data['company'].upper()) + ':' + str(data['data']).replace('{', '').replace('}',
                                                                                                             '').replace(
            '\'', '')

    logger.debug(message)
    return message

# 注册到plombery
# @task
def fetch_account_data_job():
    """定时任务：发送生产环境远传出账生成情况"""
    logger = get_logger()
    message = build_sms_message()
    sms_client.send_sms(phones=job_config['phones'],content=message,logger=logger)


if __name__ == "__main__":
    # 手动触发测试
    fetch_account_data_job()

    # fill_sql(job_config)