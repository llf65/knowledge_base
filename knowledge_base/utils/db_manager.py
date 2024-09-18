# knowledge_base/utils/db_manager.py

import pymysql
from pymysql.cursors import DictCursor
from knowledge_base.config.mysql_config import db_config


class DatabaseManager:
    _instance = None

    def __init__(self):
        self.connection = pymysql.connect(
            host=db_config['db_host'],
            user=db_config['db_user'],
            password=db_config['db_password'],
            database=db_config['db_name'],
            cursorclass=DictCursor,
            autocommit=False  # We'll manage transactions manually
        )

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = DatabaseManager()
        return cls._instance

    def get_connection(self):
        return self.connection

    def insert_into_table(self, table_name, data):
        """
        插入数据到指定的表中.

        :param table_name: 要插入数据的表的名称.
        :param data: 包含列-值.
        """
        columns = ', '.join(f"`{col}`" for col in data.keys())
        placeholders = ', '.join('%s' for _ in data)
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            return True
        except Exception as e:
            self.connection.rollback()
            # Log the error as needed
            return False

    def check_item_exists(self, table_name, condition):
        """
        检查指定表中是否存在符合条件的项。

        :param table_name: 要查询的表的名称。
        :param condition: 表示 WHERE 子句的列。
        :return: 如果项存在返回 True，否则返回 False。
        """

        where_clause = ' AND '.join(f"`{key}` = %s" for key in condition.keys())
        sql = f"SELECT 1 FROM `{table_name}` WHERE {where_clause} LIMIT 1"
        # print(sql)

        with self.connection.cursor() as cursor:
            cursor.execute(sql, tuple(condition.values()))
            result = cursor.fetchone()
            return result is not None

    # def insert_failure_record(self, item, reason):
    #     """
    #     将失败记录插入t_download_failed表中.
    #     """
    #     failure_data = {
    #         'title': item.get('report_title', ''),
    #         'source': 'CSRC',
    #         'type': 1,  # Assuming type 1 for guidance information
    #         'reason': reason,
    #         'create_time': 'CURRENT_TIMESTAMP',  # or appropriate timestamp
    #         'regi_state': '001',
    #     }
    #     self.insert_into_table('t_download_failed', failure_data)

    def close(self):
        self.connection.close()

    def close(self):
        self.connection.close()
