# -*- coding: utf-8 -*-

import pymysql
import pymysql.cursors
import platform
import sys
import pandas as pd
from tools import now_time_string, read_log_files, print_df
from strings import *
from parameters import *


class MySQLConnection:
    def __init__(self):
        self.connection_params = {
            "host": "172.16.24.74",
            "port": 3306,
            "user": "xuenze",
            "passwd": "FU%yXr0asa*B3Exo",
            "db": "sam_bi_analysis"
        }
        self.connection = None  # pymysql.connections.Connection()
        self.cursor = None  # pymysql.cursors.Cursor()

    def insert_user_log(self, df: pd.DataFrame, method="prd"):
        data_list = [(
            row.get(STRING_COL_ORG_NAME), row.get(STRING_COL_QUERY_TIME),
            row.get(STRING_COL_KEYWORD_COUNT), row.get(STRING_COL_USER_ID)) for index, row in df.iterrows() if row.get(STRING_COL_ORG_NAME)]
        # print(data_list)
        self.cursor.execute("truncate table user_log_{0};".format(method))
        self.cursor.executemany(
            "insert into user_log_{0}(`enterprise_name`, `query_time`, `keyword_count`, `user_id`) "
            "values (%s, %s, %s, %s);".format(method), data_list)

    def insert_business_info(self, df: pd.DataFrame, method="prd"):
        data_list = [(
            row.get(STRING_COL_BUSINESS_NAME), row.get(STRING_COL_TEAM), row.get(STRING_COL_TEAM_CONTACT)) for index, row in df.iterrows()]
        self.cursor.execute("truncate table business_info_{0};".format(method))
        self.cursor.executemany(
            "insert into business_info_{0}(`business_name`, `team`, `team_contact`) "
            "values (%s, %s, %s);".format(method),
            data_list)

    def connect(self):
        self.connection = pymysql.connect(**self.connection_params)
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()


def hour_job(method="prd"):
    print(now_time_string(), "[ ok ] Launching...")
    try:
        mc = MySQLConnection()
        mc.connect()

        try:
            # Step 1: business_info
            print(now_time_string(), "[ ok ] Step 1/2 Executing insert_business_info: ", end="")
            df_business_info = pd.read_excel(PARAMS_BUSINESS_INFO_PATH, engine="openpyxl")
            length = len(df_business_info)
            mc.insert_business_info(df_business_info, method)
            print("{0} Done.".format(length))
        except Exception as e:
            print("Error in business_info: ", e)

        try:
            # Step 2: user_log
            print(now_time_string(), "[ ok ] Step 2/2 Executing insert_user_log: ", end="")
            if platform.system().lower() == "linux":
                path = PARAMS_USER_LOG_PATH
                # if method == "dev":
                #     print("path:", path)
            else:
                path = "logs"
                # if method == "dev":
                #     print("path:", path)
            df_user_log = read_log_files(path)
            length = len(df_user_log)
            mc.insert_user_log(df_user_log, method)
            print("{0} Done.".format(length))
        except Exception as e:
            print("Error in user_log: ", e)

        mc.close()
    except Exception as e:
        print("Error in connecting:", e)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "dev":
        print("dev model")
        hour_job("dev")
    else:
        print("prd model")
        hour_job()
