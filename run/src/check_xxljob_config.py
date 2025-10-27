import requests
import json

LOGIN_URL = 'http://10.10.102.252:11005/xxljobadmin/login'
PAGE_URL = 'http://10.10.102.252:11005/xxljobadmin/jobinfo/pageList'


def get_xxl_session(url, login_data):
    """
    使用requests.Session来自动管理Cookie。
    登录成功后，返回这个session对象。
    """
    print("正在尝试登录 XXL-Job...")
    session = requests.Session()
    try:
        response = session.post(url, data=login_data, timeout=10)
        response.raise_for_status()
        if response.json().get('code') == 200:
            print("✔ 登录成功！")
            return session
        else:
            print(f"❌ 登录失败: {response.json().get('msg', '未知错误')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 登录请求失败: {e}")
        return None


def get_xxl_page(session, url, page_payload):
    """
    使用上一步获取的session对象来调用pagelist接口，获取定时任务数据。
    """
    if not session:
        return None
    print("正在获取任务列表...")
    try:
        response = session.post(url, data=page_payload, timeout=10)
        response.raise_for_status()
        page_data = response.json()
        print(f"✔ 成功获取到 {len(page_data.get('data', []))} 条任务数据。")
        return page_data
    except requests.exceptions.RequestException as e:
        print(f"❌ 获取任务列表失败: {e}")
        return None
    except json.JSONDecodeError:
        print("❌ 解析任务列表响应失败，返回的不是有效的JSON格式。")
        return None


def check_job_configs_dlb() -> str:
    """
    一个自包含的函数，用于检查所有预定义的XXL-Job任务配置。
    所有配置信息都硬编码在此函数内部。
    """
    print("=============================================")
    print("=      开始执行 XXL-Job 配置巡检任务      =")
    print("=============================================\n")

    # --- 1. 定义所有需要校验的任务配置 ---
    jobs_to_check = [
        {
            "job_id": 1079,
            "job_desc": "清北大路表远传表出账",  # 描述，用于日志输出
            "expected_cron": "0 0 7 1-5 * ?",
            "expected_param": '{"2":["02150201","02150205"]}'
        },
        {
            "job_id": 1078,
            "job_desc": "怀柔、檀州大路表远传表出账",
            "expected_cron": "0 0 7 1 * ?",
            "expected_param": '{0:["02180201","02170201"]}'
        },
        {
            "job_id": 1074,
            "job_desc": "通州大路表远传表出账",
            "expected_cron": "0 0 7 * * ?",
            "expected_param": '{"1":["02130201"]}'
        },
        {
            "job_id": 1073,
            "job_desc": "良泉大路表远传表出账",
            "expected_cron": "0 0 7 1-16 * ?",
            "expected_param": '{"2":["02110201","02110202","02110203","02110204","02110205","02110207","02110208","02110209"]}'
        },
        {
            "job_id": 1071,
            "job_desc": "门头沟、缙阳大路表远传表出账",
            "expected_cron": "0 0 7 * * ?",
            "expected_param": '{"0":["02160201"],"1":["02190202","02190201"]}'
        },
        {
            "job_id": 1070,
            "job_desc": "大兴大路表远传表出账",
            "expected_cron": "0 0 7 1-8 * ?",
            "expected_param": '{"1":["02120201"],"2":["02120202"]}'
        },
        {
            "job_id": 1069,
            "job_desc": "石景山大路表远传表出账",
            "expected_cron": "0 0 7 1-3 * ?",
            "expected_param": '{"0":["02200201"],"1":["02200207"],"2":["02200208"]}'
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

    # --- 3. 执行登录和查询 ---
    session = get_xxl_session(LOGIN_URL, login_payload)
    if not session:
        print("\n巡检任务因登录失败而中止。")
        return

    page_data = get_xxl_page(session, PAGE_URL, page_payload)
    if not page_data or 'data' not in page_data:
        print("\n巡检任务因获取任务列表失败而中止。")
        return

    # --- 4. 遍历并校验每个预定义的任务 ---
    all_jobs_data = {job['id']: job for job in page_data['data']}  # 将列表转为字典，方便通过ID查找

    print("\n--- 开始逐一校验任务配置 ---")
    overall_success = True
    message = '[大路表远传出账定时任务检查]\n'
    for check_item in jobs_to_check:
        job_id = check_item["job_id"]
        job_desc = check_item["job_desc"]
        expected_cron = check_item["expected_cron"]
        expected_param = check_item["expected_param"]

        print(f"\n--- 校验任务: {job_desc} (ID: {job_id}) ---")

        if job_id not in all_jobs_data:
            print(f"❌ 校验失败：在任务列表中未找到 ID 为 {job_id} 的任务。")
            overall_success = False
            continue

        target_job = all_jobs_data[job_id]
        actual_cron = target_job.get('scheduleConf')
        actual_param = target_job.get('executorParam')
        actual_status = target_job.get('triggerStatus')

        # 标志当前任务是否校验成功
        current_job_success = True

        # 校验Cron
        if actual_cron != expected_cron:
            print(f"  -{job_desc}Cron不匹配! 期望: '{expected_cron}', 实际: '{actual_cron}'")
            message += '{job_desc}Cron不匹配!\n'

            current_job_success = False
            overall_success = False

        # 校验参数
        if actual_param != expected_param:
            print(f"  -{job_desc}参数不匹配! 期望: '{expected_param}', 实际: '{actual_param}'")
            message += '{job_desc}参数不匹配!\n'
            current_job_success = False
            overall_success = False

        # 校验定时任务状态
        if actual_status != 1:
            print(f"  -{job_desc}当前定时任务状态异常，未开启")
            message += '{job_desc}任务状态异常，未开启!\n'
            current_job_success = False
            overall_success = False

        if current_job_success:
            print("  - ✅ 配置完全正确！")
            message += '{job_desc}配置完全正确!\n'

    # --- 5. 打印最终总结 ---
    print("\n=============================================")
    if overall_success:
        print("🎉 恭喜！所有预定义的任务配置均校验通过！")
    else:
        print("⚠️ 注意！部分任务配置存在不匹配项，请检查以上日志！")
    print("=============================================")


def check_job_configs_hb() -> str:
    """
    一个自包含的函数，用于检查所有预定义的XXL-Job任务配置。
    所有配置信息都硬编码在此函数内部。
    """
    print("=============================================")
    print("=      开始执行 XXL-Job 配置巡检任务      =")
    print("=============================================\n")

    # --- 1. 定义所有需要校验的任务配置 ---
    jobs_to_check = [
        {
            "job_id": 1023,
            "job_desc": "户表远传表出账(非良泉)",  # 描述，用于日志输出
            "expected_cron": "0 0 6 3-6 * ?",
            "expected_param": '{"0": ["02200208","02160201","02130201","02150201","02150205"],"1": ["02200207","02120201","02190202","02190201"],"2": ["02200201","02120202","02180201","02170201"]}'
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
    session = get_xxl_session(LOGIN_URL, login_payload)
    if not session:
        print("\n巡检任务因登录失败而中止。")
        return

    page_data = get_xxl_page(session, PAGE_URL, page_payload)
    if not page_data or 'data' not in page_data:
        print("\n巡检任务因获取任务列表失败而中止。")
        return

    # --- 4. 遍历并校验每个预定义的任务 ---
    all_jobs_data = {job['id']: job for job in page_data['data']}  # 将列表转为字典，方便通过ID查找

    print("\n--- 开始逐一校验任务配置 ---")
    overall_success = True
    message = '[户表远传出账定时任务检查]\n'
    for check_item in jobs_to_check:
        job_id = check_item["job_id"]
        job_desc = check_item["job_desc"]
        expected_cron = check_item["expected_cron"]
        expected_param = check_item["expected_param"]

        print(f"\n--- 校验任务: {job_desc} (ID: {job_id}) ---")

        if job_id not in all_jobs_data:
            print(f"❌ 校验失败：在任务列表中未找到 ID 为 {job_id} 的任务。")
            overall_success = False
            continue

        target_job = all_jobs_data[job_id]
        actual_cron = target_job.get('scheduleConf')
        actual_param = target_job.get('executorParam')
        actual_status = target_job.get('triggerStatus')

        # 标志当前任务是否校验成功
        current_job_success = True

        # 校验Cron
        if actual_cron != expected_cron:
            print(f"  -{job_desc}Cron不匹配! 期望: '{expected_cron}', 实际: '{actual_cron}'")
            message+='{job_desc}Cron不匹配!\n'

            current_job_success = False
            overall_success = False

        # 校验参数
        if actual_param != expected_param:
            print(f"  -{job_desc}参数不匹配! 期望: '{expected_param}', 实际: '{actual_param}'")
            message += '{job_desc}参数不匹配!\n'
            current_job_success = False
            overall_success = False

        # 校验定时任务状态
        if actual_status != 1:
            print(f"  -{job_desc}当前定时任务状态异常，未开启")
            message += '{job_desc}任务状态异常，未开启!\n'
            current_job_success = False
            overall_success = False

        if current_job_success:
            print("  - ✅ 配置完全正确！")
            message += '{job_desc}配置完全正确!\n'

    # --- 5. 打印最终总结 ---
    print("\n=============================================")
    if overall_success:
        print("🎉 恭喜！所有预定义的任务配置均校验通过！")
    else:
        print("⚠️ 注意！部分任务配置存在不匹配项，请检查以上日志！")
    print("=============================================")
if __name__ == "__main__":
    # 调用这个唯一的、自包含的检查函数
    print(check_job_configs_dlb())
    print(check_job_configs_hb())