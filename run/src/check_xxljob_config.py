import logging
from logging import getLogger

import requests
import json

from plombery import task,get_logger

from run.src.fetch_account_data import sms_client
from run.src.sms.sms_client import SMSClient
from run.src.utils import ConfigLoader

config = ConfigLoader.ConfigLoader()
LOGIN_URL = config.get('XXL_LOGIN_URL')
PAGE_URL = config.get('XXL_PAGE_URL')

sms_client = SMSClient(base_url=config.get_sms_api())
logger = logging.getLogger(__name__)

def get_xxl_session(url, login_data):
    """
    使用requests.Session来自动管理Cookie。
    登录成功后，返回这个session对象。
    """
    logger.info("正在尝试登录 XXL-Job...")
    session = requests.Session()
    try:
        response = session.post(url, data=login_data, timeout=10)
        response.raise_for_status()
        if response.json().get('code') == 200:
            logger.info("登录成功！")
            return session
        else:
            logger.error(f"登录失败: {response.json().get('msg', '未知错误')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"登录请求失败: {e}")
        return None


def get_xxl_page(session, url, page_payload):
    """
    使用上一步获取的session对象来调用pagelist接口，获取定时任务数据。
    """
    if not session:
        return None
    logger.info("正在获取任务列表...")
    try:
        response = session.post(url, data=page_payload, timeout=10)
        response.raise_for_status()
        page_data = response.json()
        logger.info(f"成功获取到 {len(page_data.get('data', []))} 条任务数据。")
        return page_data
    except requests.exceptions.RequestException as e:
        logger.error(f"获取任务列表失败: {e}")
        return None
    except json.JSONDecodeError:
        logger.error("解析任务列表响应失败，返回的不是有效的JSON格式。")
        return None


def check_job_configs_dlb(logger) -> str:
    """
    检查大路表远传表出账定时任务参数
    """

    logger.info("=      开始执行 XXL-Job 配置巡检任务      =")
    # --- 1. 定义所有需要校验的任务配置 ---
    jobs_to_check = [
        {
            "job_id": 1079,
            "job_desc": "清北大路表远传表出账",  # 描述，用于日志输出
            "expected_cron": "0 0 7 1-5 * ?",
            "expected_param": '{"2":["02150201","02150205"]}',
            "company":['ds_qb']
        },
        {
            "job_id": 1078,
            "job_desc": "怀柔、檀州大路表远传表出账",
            "expected_cron": "0 0 7 1 * ?",
            "expected_param": '{0:["02180201","02170201"]}'
            ,
            "company": ['ds_hr','ds_jy']
        },
        {
            "job_id": 1074,
            "job_desc": "通州大路表远传表出账",
            "expected_cron": "0 0 7 * * ?",
            "expected_param": '{"1":["02130201"]}'
            ,
            "company": ['ds_tz']
        },
        {
            "job_id": 1073,
            "job_desc": "良泉大路表远传表出账",
            "expected_cron": "0 0 7 1-16 * ?",
            "expected_param": '{"2":["02110201","02110202","02110203","02110204","02110205","02110207","02110208","02110209"]}',
            "company":['ds_lq']

        },
        {
            "job_id": 1071,
            "job_desc": "门头沟、缙阳大路表远传表出账",
            "expected_cron": "0 0 7 * * ?",
            "expected_param": '{"0":["02160201"],"1":["02190202","02190201"]}',
            "company":['ds_mtg','ds_jy']
        },
        {
            "job_id": 1070,
            "job_desc": "大兴大路表远传表出账",
            "expected_cron": "0 0 7 1-8 * ?",
            "expected_param": '{"1":["02120201"],"2":["02120202"]}',
            "company":['ds_dx']
        },
        {
            "job_id": 1069,
            "job_desc": "石景山大路表远传表出账",
            "expected_cron": "0 0 7 1-3 * ?",
            "expected_param": '{"0":["02200201"],"1":["02200207"],"2":["02200208"]}',
            "company":['ds_sjs']
        }
    ]

    # --- 2. 登录和查询的公共配置 ---
    login_payload = {'userName': 'admin', 'password': '123456'}
    page_payload = {
        'jobGroup': 38,  # 任务所在的执行器ID
        'triggerStatus': -1,
        'start': 0,
        'length': 200
    }

    message = '[大路表远传出账定时任务检查]\n'
    session = get_xxl_session(LOGIN_URL, login_payload)
    if not session:
        logger.error("\n巡检任务因登录失败而中止。")
        message += "xxljob登录失败，无法检查"
        return message

    page_data = get_xxl_page(session, PAGE_URL, page_payload)
    if not page_data or 'data' not in page_data:
        logger.error("\n巡检任务因获取任务列表失败而中止。")
        message += "获取任务列表失败而中止。"
        return message

    # --- 4. 遍历并校验每个预定义的任务 ---
    all_jobs_data = {job['id']: job for job in page_data['data']}  # 将列表转为字典，方便通过ID查找

    logger.info("\n--- 开始逐一校验任务配置 ---")
    overall_success = True
    message = '[大路表远传出账定时任务检查]\n'
    for check_item in jobs_to_check:
        job_id = check_item["job_id"]
        job_desc = check_item["job_desc"]
        expected_cron = check_item["expected_cron"]
        expected_param = check_item["expected_param"]

        logger.info(f"\n--- 校验任务: {job_desc} (ID: {job_id}) ---")

        if job_id not in all_jobs_data:
            logger.error(f"校验失败：在任务列表中未找到 ID 为 {job_id} 的任务。")
            message += f'{job_desc}在任务列表中未找到!\n'
            overall_success = False
            continue

        target_job = all_jobs_data[job_id]
        actual_cron = target_job.get('scheduleConf')
        actual_param = target_job.get('executorParam')
        actual_status = target_job.get('triggerStatus')

        # 标志当前任务是否校验成功
        current_job_success = True

        # 校验Cron
        if actual_cron.replace(" ", "").replace("\n", "") != expected_cron.replace(" ", "").replace("\n", ""):
            # 处理不匹配的情况
            logger.error(f"  -{job_desc}Cron不匹配! 期望: '{expected_cron}', 实际: '{actual_cron}'")
            message += f'{job_desc}Cron不匹配!\n'

            current_job_success = False
            overall_success = False

        # 校验参数
        if actual_param.replace(" ", "").replace("\n", "") != expected_param.replace(" ", "").replace("\n", ""):
            # 处理不匹配的情况
            logger.error(f"  -{job_desc}参数不匹配! 期望: '{expected_param}', 实际: '{actual_param}'")
            message += f'{job_desc}参数不匹配!\n'
            current_job_success = False
            overall_success = False

        # 校验定时任务状态
        if actual_status != 1:
            logger.error(f"  -{job_desc}当前定时任务状态异常，未开启")
            message += f'{job_desc}任务状态异常，未开启!\n'
            current_job_success = False
            overall_success = False

        if current_job_success:
            logger.info("  - ✅ 配置完全正确！")
            message += f'{job_desc}配置完全正确!\n'

    if overall_success:
        logger.info("所有预定义的任务配置均校验通过！")
        message += "所有预定义的任务配置均校验通过！"
    else:
        logger.info("注意！部分任务配置存在不匹配项，请检查以上日志！")
        message += f'部分任务配置存在不匹配项，请检查以上日志！'

    # sms_client.send_sms(phones=job_config['phones'],content=message,logger=logger)
    return message

def check_job_configs_hb(logger) -> str:
    """
    检查大路表户表出账定时任务参数
    """

    logger.info("=      开始执行 XXL-Job 配置巡检任务      =")

    # --- 1. 定义所有需要校验的任务配置 ---
    jobs_to_check = [
        {
            "job_id": 1023,
            "job_desc": "户表远传表出账(非良泉\石景山)",  # 描述，用于日志输出
            "expected_cron": "0 0 6 3-6 * ?",
            "expected_param": '{"0":["02160201","02130201","02150201","02150205"],"1":["02120201","02190202","02190201"],"2":["02120202","02180201","02170201"]}'
            # "expected_param": '{"0":["02200208","02160201","02130201","02150201","02150205"],"1":["02200207","02120201","02190202","02190201"],"2":["02200201","02120202","02180201","02170201"]}'
        },
        {
            "job_id": 1084,
            "job_desc": "户表远传表出账(石景山)",  # 描述，用于日志输出
            "expected_cron": "0 0 6 10-13 * ?",
            "expected_param": '{"0":["02200208"],"1":["02200207"],"2":["02200201"]}'
        },
        {
            "job_id": 1072,
            "job_desc": "户表远传表出账(良泉)",
            "expected_cron": "0 0 6 2-6 * ?",
            "expected_param": '{"0":["02110201","02110202","02110203"],"1":["02110204","02110205"],"2":["02110207","02110208","02110209"]}'
        }
    ]

    # --- 2. 登录和查询的公共配置 ---
    login_payload = {'userName': 'admin', 'password': '123456'}
    page_payload = {
        'jobGroup': 35,  # 任务所在的执行器ID
        'triggerStatus': -1,
        'jobDesc':'出账',
        'start': 0,
        'length': 200
    }

    # --- 3. 执行登录和查询 ---
    message = '[户表远传出账定时任务检查]\n'
    session = get_xxl_session(LOGIN_URL, login_payload)
    if not session:
        logger.error("\n巡检任务因登录失败而中止。")
        message += "xxljob登录失败，无法检查"
        return message

    page_data = get_xxl_page(session, PAGE_URL, page_payload)
    if not page_data or 'data' not in page_data:
        logger.error("\n巡检任务因获取任务列表失败而中止。")
        message += "获取任务列表失败而中止。"
        return message

    # --- 4. 遍历并校验每个预定义的任务 ---
    all_jobs_data = {job['id']: job for job in page_data['data']}  # 将列表转为字典，方便通过ID查找

    logger.info("\n--- 开始逐一校验任务配置 ---")
    overall_success = True
    for check_item in jobs_to_check:
        job_id = check_item["job_id"]
        job_desc = check_item["job_desc"]
        expected_cron = check_item["expected_cron"]
        expected_param = check_item["expected_param"]

        logger.info(f"\n校验任务: {job_desc} (ID: {job_id}) ---")

        if job_id not in all_jobs_data:
            logger.error(f"校验失败：在任务列表中未找到 ID 为 {job_id} 的任务。")
            message += f'{job_desc}在任务列表中未找到!\n'
            overall_success = False
            continue

        target_job = all_jobs_data[job_id]
        actual_cron = target_job.get('scheduleConf')
        actual_param = target_job.get('executorParam')
        actual_status = target_job.get('triggerStatus')

        # 标志当前任务是否校验成功
        current_job_success = True

        # 校验Cron
        if actual_cron.replace(" ", "").replace("\n", "") != expected_cron.replace(" ", "").replace("\n", ""):
            # 处理不匹配的情况
            logger.error(f"  -{job_desc}Cron不匹配! 期望: '{expected_cron}', 实际: '{actual_cron}'")
            message+=f'{job_desc}Cron不匹配!\n'

            current_job_success = False
            overall_success = False

        # 校验参数
        if actual_param.replace(" ", "").replace("\n", "") != expected_param.replace(" ", "").replace("\n", ""):
            # 处理不匹配的情况
            logger.error(f"  -{job_desc}参数不匹配! 期望: '{expected_param}', 实际: '{actual_param}'")
            message += f'{job_desc}参数不匹配!\n'
            current_job_success = False
            overall_success = False

        # 校验定时任务状态
        if actual_status != 1:
            logger.error(f"  -{job_desc}当前定时任务状态异常，未开启")
            message += f'{job_desc}任务状态异常，未开启!\n'
            current_job_success = False
            overall_success = False

        if current_job_success:
            logger.info("  - 配置完全正确！")
            message += f'{job_desc}配置完全正确!\n'

    # --- 5. 打印最终总结 ---
    if overall_success:
        logger.info("所有预定义的任务配置均校验通过！")
        message += "所有预定义的任务配置均校验通过！"
    else:
        logger.info("注意！部分任务配置存在不匹配项，请检查以上日志！")
        message += f'部分任务配置存在不匹配项，请检查以上日志！'
    return message
    # 发送短信
    # sms_client.send_sms(phones=job_config['phones'],content=message,logger=logger)
@task
def check_xxl_job():
    logger = get_logger()
    job_name = "check_job_configs"
    job_config = config.get_config_by_job(job_name)
    message = check_job_configs_dlb(logger)
    sms_client.send_sms(phones=job_config['phones'], content=message, logger=logger)
    message = check_job_configs_hb(logger)
    sms_client.send_sms(phones=job_config['phones'], content=message, logger=logger)

if __name__ == "__main__":
    logger = getLogger(__name__)
    job_name = "check_job_configs"
    job_config = config.get_config_by_job(job_name)
    message = check_job_configs_dlb(logger)
    logger.info(message)
    # sms_client.send_sms(phones=job_config['phones'], content=message, logger=logger)
    message = check_job_configs_hb(logger)
    logger.info(message)
    # sms_client.send_sms(phones=job_config['phones'], content=message, logger=logger)