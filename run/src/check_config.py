from datetime import datetime, timedelta # 导入了 datetime 类
from dateutil.relativedelta import relativedelta
import logging

from plombery import task, get_logger

from run.src.utils import logger
from run.src.rules.CompanyEnum import CompanyNameEnum
from run.src.sms.sms_client import SMSClient
from run.src.utils import ConfigLoader, dbutils

config = ConfigLoader.ConfigLoader()
sms_client = SMSClient(base_url=config.get_sms_api())
job_name = "check_config_job"
job_config = config.get_config_by_job(job_name)
logger = logging.getLogger(__name__)


# def build_sms_message():
#     # 从config.yaml中获取该定时任务需要执行的分公司名称，注意获取数据并组装成短信内容
#     companies = job_config.get("companies")
#     res = []
#     message = "【生产环境】查表计划当前生成情况："
#     # 获取所有分公司数据
#     for company in companies:
#         data = fetch_data_by_company(company)
#         res.append(data)
#         message += '\n'
#         message += CompanyNameEnum.get_name(data['company'].upper()) + ':' + str(data['data']).replace('{', '').replace('}',
#                                                                                                              '').replace(
#             '\'', '')
#
#     logger.debug(message)
#     return message



"""
    每月出账前检查所有参数，并发送短信；
    每日轮询参数值，如果和预期不一致即报警。
"""


def check_system_parameter() -> bool:
    """
    检查系统参数 0125 和 0126 的值。
    只有当 0125='2' 且 0126='3' 且 param_state='Y' 时才返回 True。
    """
    # 假设 dbutils 和 config 已经在作用域内
    db = dbutils.DBUtils(config=config.get_database('ds_common'))
    sql = '''
          SELECT param_code, param_state, param_value
          FROM water_revenue.sys_param
          WHERE param_code IN ('0125', '0126') \
          '''

    try:
        db.connect()
        res = db.query(sql)

        # 1. 检查查询结果是否有效
        if not res or len(res) < 2:
            return False

        # 2. 将结果转换为 {param_code: param_object} 字典，方便查找和检查重复
        param_map = {item['param_code']: item for item in res}

        # 3. 检查是否有且仅有我们关心的两个参数
        if set(param_map.keys()) != {'0125', '0126'}:
            if len(param_map) != 2:
                return False

        # 4. 检查 0125 的条件
        param_0125 = param_map.get('0125')
        is_0125_ok = (
                param_0125 is not None and
                param_0125.get('param_state') == 'Y' and
                param_0125.get('param_value') == '2'
        )
        # 5. 检查 0126 的条件
        param_0126 = param_map.get('0126')
        is_0126_ok = (
                param_0126 is not None and
                param_0126.get('param_state') == 'Y' and
                param_0126.get('param_value') == '3'
        )

        # 6. 只有两个条件都满足才返回 True
        return is_0125_ok and is_0126_ok

    except Exception as e:
        # 最好在这里记录日志
        # self.logger.error(f"数据库操作失败: {e}")
        return False
    finally:
        db.close()

def check_business_parameter():
    """业务参数-自动审核"""
    pass


logger = logging.getLogger(__name__)


def check_work_day_jq():
    """检查日历配置的有效性。
    逻辑：
    1. cal_day 的日期部分必须等于 work_day_seq。
    2. 如果日期是 02-06，则 is_make_day 必须是 'Y'。
    3. 如果日期不是 02-06，则 is_make_day 必须是 'N'。
    """

    db = dbutils.DBUtils(config=config.get_database('ds_common'))

    # --- 日期计算逻辑 (保留) ---
    now = datetime.now()
    target_date = now
    if now.day >= 25:
        target_date = now + relativedelta(months=1)
    query_prefix = target_date.strftime('%Y-%m')

    # --- SQL 查询 (保留) ---
    sql = f'''
    select cal_day,is_work_day,work_day_seq,is_make_day from water_revenue.calendar_date where cal_day like '{query_prefix}%' order by cal_day ;
    '''

    # --- 检查逻辑所需的常量 ---
    MAKE_DAY_DATES = {'02', '03', '04', '05', '06'}

    try:
        db.connect()
        res = db.query(sql,params=None)

        if not res:
            logger.warning(f"查询月份 {query_prefix} 结果集为空，跳过检查。")
            return True  # 保持您的逻辑：查询为空返回 True

        for item in res:
            # 安全获取字段值
            cal_day_date_str = item.get('cal_day')[-2:]
            work_day_seq = item.get('work_day_seq')
            is_make_day = item.get('is_make_day')
            is_work_day = item.get('is_work_day') # 如果不需要 is_work_day 检查，可以不定义

            # --- 约束 1: cal_day 的日期部分必须等于 work_day_seq ---
            if cal_day_date_str != work_day_seq or is_work_day != '0':
                logger.error(f"约束1失败: 日期 '{cal_day_date_str}' 不等于 work_day_seq '{work_day_seq}'")
                return False

            # --- 约束 2 & 3: 检查 is_make_day 的逻辑 ---
            if cal_day_date_str in MAKE_DAY_DATES:
                # 逻辑：日期是 02-06，要求 is_make_day 必须是 'Y'
                if is_make_day != 'Y':
                    logger.error(f"约束2失败: 日期 '{cal_day_date_str}' 要求 is_make_day 为 'Y'，实际为 '{is_make_day}'")
                    return False
            else:
                # 逻辑：日期不是 02-06 (即其他所有日期)，要求 is_make_day 必须是 'N'
                if is_make_day != 'N':
                    logger.error(f"约束3失败: 日期 '{cal_day_date_str}' 要求 is_make_day 为 'N'，实际为 '{is_make_day}'")
                    return False

        # 如果所有项都通过了循环中的检查
        logger.info(f"成功通过日历配置检查：月份 {query_prefix} 的所有记录都满足约束。")
        return True

    except Exception as e:
        logger.error(f"检查日历配置时发生异常: {e}", exc_info=True)
        return False

    finally:
        db.close()

def check_fee_price():
    """检查全量水价"""
    pass
@task
def check_config_job():
    logger = get_logger()
    message = '\n当月远传配置情况\n'
    if check_system_parameter():
        message += '【系统参数0125/0126】远传出账取数时间范围正确√'+'\n'
        logger.info('远传出账取数时间范围正确√')
    else :
        message += '【系统参数0125/0126】远传出账取数时间范围，需要调整'+'\n'
        logger.error('远传出账取数时间范围，需要调整')
    if check_work_day_jq():
        message +='【计划管理】工作日与户表开账日配置正确√'+'\n'
        logger.info('工作日与户表开账日配置正确√')
    else :
        message += '【计划管理】工作日与户表开账日配置错误，需要调整' + '\n'
        logger.error('工作日与户表开账日配置错误，需要调整')
    # 组装成短信，并发送
    sms_client.send_sms(phones=job_config['phones'],content=message,logger=logger)

if __name__ == '__main__':
    check_config_job()
