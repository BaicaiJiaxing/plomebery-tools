# core/sms_client.py
import json
import logging

from plombery import get_logger

import requests

class SMSClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.logger = logging.getLogger("SMSClient")

    def send_sms(self, phones: str, content: str,logger=None):
        """北水短信服务发送格式"""
        if logger is None:
            logger = self.logger
        paramList = []
        postData = {
            "phones": phones,
            "content": content
        }
        paramList.append(postData)
        logger.info(f"post:{self.base_url},参数:{json.dumps(paramList, ensure_ascii=False)}")
        #TODO:发内网环境之前需要解除注释
        resp = requests.post(self.base_url, json=paramList)
        if resp == "success":
            logger.info(f"【短信发送成功】手机号: {phones}, 内容: {content}")
        else:
            logger.error(f"【短信发送失败】手机号: {phones}, 内容: {content}")
        return True
