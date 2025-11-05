# utils/dbutils.py
import pymysql
import psycopg2
import logging

from src.utils.ConfigLoader import ConfigLoader


class DBUtils:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)

    def connect(self):
        db_type = self.config.get("type")
        host = self.config.get("host")
        port = self.config.get("port")
        user = self.config.get("user")
        password = self.config.get("password")
        dbname = self.config.get("name")

        if db_type == "mysql":
            self.conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=dbname,
                charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor
            )
        elif db_type in ["kingbase", "postgres"] or db_type is None:
            self.conn = psycopg2.connect(
                host=host, port=port, user=user,
                password=password, dbname=dbname
            )
        else :
            raise ValueError(f"不支持的数据库类型: {db_type}")

    def query(self, sql: str, params=None):
        if self.conn is None:
            self.connect()
        with self.conn.cursor() as cursor:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            self.logger.info(f"当前执行sql:{sql}")
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            # 组装成字典列表
            result = [dict(zip(columns, row)) for row in rows]
            self.logger.info(f"SQL执行结果:{result}")
            return result

    def execute(self, sql: str, params=None):
        if self.conn is None:
            self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params or ())
            self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
