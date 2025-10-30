# utils/config.py
import logging

import yaml
from pathlib import Path

class ConfigLoader:
    def __init__(self, config_file: str = "config.yaml"):
        # 根目录路径
        self.root_dir = Path(__file__).parent.parent.resolve()
        # 始终去根目录找 config 文件
        self.config_file = self._find_config(config_file)
        self._config = self._load_config()
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)


    def _find_config(self, filename: str) -> Path:
        """
        在根目录查找配置文件
        """
        candidate = self.root_dir / filename
        if candidate.exists():
            return candidate
        else:
            raise FileNotFoundError(f"配置文件 {filename} 在根目录 {self.root_dir} 未找到")

    def _load_config(self) -> dict:
        if not self.config_file.exists():
            raise FileNotFoundError(f"配置文件 {self.config_file} 不存在")
        with open(self.config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_database(self,company) -> dict:
        """获取指定分公司名称对应的数据库配置"""
        return self._config["database"][company]

    def get_sms_api(self) -> dict:
        """获取短信发送的api"""
        return self._config["sms_api"]

    def get_config_by_job(self,job_name) -> dict:
        config = self._config["jobs"]
        return config[job_name]

    def get(self, key: str, default=None):
        """支持通过 a.b.c 获取配置"""
        keys = key.split(".")
        value = self._config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
