# -*- coding: utf-8 -*-

import re
import os
import json
import time
import urllib3
import hashlib
import requests
import datetime
import pandas as pd
import numpy as np
from strings import *


def print_df(df):
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)
    print(df)


def get_business_names(filename="normal/business_info.xlsx"):
    df = pd.read_excel(filename, engine="openpyxl")
    df.index = df.index + 1
    return df


def get_user(filename="normal/user_info.xlsx"):
    df = pd.read_excel(filename, engine="openpyxl")
    df.index = df.index + 1
    return df


def sha256(filename):
    with open(filename, "rb") as f:
        data = f.read()
    file_md5 = hashlib.sha256(data).hexdigest()
    return file_md5


def read_log_file(filename):
    try:
        try:
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    lines = f.readlines()
            except Exception as e:
                print("[Error] File %s was not encoded by \"utf-8\", try \"gbk\" instead. Warning:" % filename, e)
                with open(filename, "r", encoding="gbk") as f:
                    lines = f.readlines()
        except Exception as e:
            print("[Error] File %s was not encoded by \"utf-8\" or \"gbk\", try \"binary\" instead. Warning:" % filename, e)
            lines_bin = open(filename, "rb")
            lines = []
            for i, one_line in enumerate(lines_bin):
                try:
                    line_clear = one_line.decode()
                    lines.append(line_clear)
                except Exception as e:
                    print("[Error] Skip line %d. Warning:" % (i + 1), e)
        lines = [line.replace("\n", "") for line in lines if len(line) > 5]
        log_res = []
        # print(filename)
        for i, line in enumerate(lines):
            parts = re.findall("INFO: {.*?}", line)
            if len(parts) > 0 and len(parts[0]) > 5:
                data = parts[0][6:]
                data = data.replace("'\"", "\"").replace("\"'", "\"").replace("'", "\"")
                query_time = " ".join(line.split()[:2])[:19]
                # print(json.loads(data))
                try:
                    log_res.append([query_time, json.loads(data)])
                except Exception as e:
                    print("Error in reading file %s row %d:" % (filename, i), e)
        # print(log_res)
        # log_res = [item for item in log_res if item[1].get("orgName")]
        query_time = [item[0] for item in log_res]
        # org_name = [item[1].get("orgName") for item in log_res]
        org_name = []
        for item in log_res:
            if item[1].get("orgNameList") and len(item[1].get("orgNameList")) > 0:
                org_name.append("{0} 等{1}家企业".format(item[1].get("orgNameList")[0],  len(item[1].get("orgNameList"))))
            else:
                org_name.append(item[1].get("orgName"))
        user_id = [item[1].get("userId") for item in log_res]
        keywords_count = [len_double_list(item[1].get("keyWordList1")) + len_list(item[1].get("keyWordList2")) for item in log_res]
        df = pd.DataFrame({
            STRING_COL_ORG_NAME: org_name,
            STRING_COL_QUERY_TIME: query_time,
            STRING_COL_KEYWORD_COUNT: keywords_count,
            STRING_COL_USER_ID: user_id
        })
        return df
    except Exception as e:
        print("[Error] File %s still fails. Skip this file. Warning:" % filename, e)
        df = pd.DataFrame({
            STRING_COL_ORG_NAME: [],
            STRING_COL_QUERY_TIME: [],
            STRING_COL_KEYWORD_COUNT: [],
            STRING_COL_USER_ID: []
        })
        return df


def read_log_files(path):
    log_paths = get_log_paths(path)
    df = pd.DataFrame(columns=[STRING_COL_ORG_NAME, STRING_COL_QUERY_TIME, STRING_COL_KEYWORD_COUNT, STRING_COL_USER_ID])
    for file in log_paths:
        df_tmp = read_log_file(file)
        df = pd.concat([df, df_tmp])
    df.sort_values(by=[STRING_COL_QUERY_TIME], ascending=[False], inplace=True)
    df = df.reset_index(drop=True)
    return df


def get_log_paths(path):
    files = os.listdir(path)
    files = ["{0}{1}{2}".format(path, os.sep, file) for file in files if "user_query" in file]
    return files


def len_double_list(double_list):
    count = 0
    if double_list is None:
        return 0
    for item in double_list:
        count += len(item)
    return count


def len_list(single_list):
    if single_list is None:
        return 0
    return len(single_list)


def now_time_string(string_format=STRING_FORMAT_FULL_SECOND):
    return stamp_to_string(time.time(), string_format)


class Log:
    def __init__(self):
        self.logs = []
        self.flag = False
        self.log = STRING_EMPTY
    
    def print(self, string, log_end=None):
        if self.flag:
            full_log = string
        else:
            full_log = STRING_FORMAT_3.format(now_time_string(), STRING_SPACE, string)
        if log_end is not None:
            print(full_log, end=STRING_EMPTY)
            self.logs.append(full_log)
            self.flag = True
        else:
            print(full_log)
            full_log += STRING_PLAIN_ENTER
            self.logs.append(full_log)
            self.flag = False
        self.log += full_log


def stamp_to_string(stamp, string_format=STRING_FORMAT_FULL_SECOND):
    return time.strftime(string_format, time.localtime(stamp))


def string_to_stamp(string, string_format=STRING_FORMAT_FULL_SECOND):
    return time.mktime(time.strptime(string, string_format))


def build_tab(df, business_name):
    df_tab = df[(df[STRING_COL_BUSINESS_NAME] == business_name)].copy(deep=True)
    # print_df(df_tab)
    df_tab[STRING_COL_QUERY_COUNT] = [1] * len(df_tab)
    df_group = df_tab[[
        STRING_COL_ORG_NAME, STRING_COL_KEYWORD_COUNT]].groupby(
        [STRING_COL_ORG_NAME], as_index=False).sum()
    df_group[STRING_COL_QUERY_COUNT] = df_tab[[
        STRING_COL_ORG_NAME, STRING_COL_KEYWORD_COUNT]].groupby(
        [STRING_COL_ORG_NAME], as_index=False).count()[STRING_COL_KEYWORD_COUNT]
    # df_group[STRING_COL_REGISTRATION_COUNT] = df_group[STRING_COL_FILE_COUNT] = df_group[STRING_COL_PAGE_COUNT] = [np.nan] * len(df_group)
    registration_count, file_count, page_count = [], [], []
    session = requests.Session()
    for one_org_name in df_group[STRING_COL_ORG_NAME]:
        tab_detail = get_tab_detail(session, one_org_name)
        registration_count.append(tab_detail[0])
        file_count.append(tab_detail[1])
        page_count.append(tab_detail[2])
    session.close()
    df_group[STRING_COL_REGISTRATION_COUNT] = registration_count
    df_group[STRING_COL_FILE_COUNT] = file_count
    df_group[STRING_COL_PAGE_COUNT] = page_count
    df_group = df_group[[STRING_COL_ORG_NAME, STRING_COL_QUERY_COUNT, STRING_COL_KEYWORD_COUNT, STRING_COL_REGISTRATION_COUNT, STRING_COL_FILE_COUNT, STRING_COL_PAGE_COUNT]]
    df_group.index = df_group.index + 1
    # print_df(df_group)
    return df_group


def build_instructions():
    df = pd.DataFrame()
    df["使用说明"] = [
        "1.在“开通机构信息”页面点击“开通机构”超链接转到机构查询明细界面；",
        "2.在机构查询明细页面点击“返回主页”回到“开通机构信息”页面；",
        "3.试用账户有效期默认为一个月，体验账户暂无到期日；",
        "4.全量详情为所有机构用户的查询明细。"
    ]
    return df

        
def http_post(session, url, org_name):
    urllib3.disable_warnings()
    headers = {
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
    }
    data = {
        "checkInDateEnd": None,
        "checkInDateStart": None,
        "checkInNo": None,
        "checkInType": None,
        "endDate": None,
        "fileConfidenceEnd": None,
        "fileConfidenceStart": None,
        "fileId": None,
        "guarantorName": None,
        "idList": None,
        "index": 0,
        "keyWord": None,
        "keyWordList1": None,
        "keyWordList2": None,
        "orderBy": None,
        "orgName": org_name,
        "orgNameList": None,
        "pageSize": 15,
        "sort": "checkInDateStart",
        "startDate": None,
        "txBussType": None,
        "txBussTypeList": None,
        "userId": 888
    }
    try:
        response = session.post(url, headers=headers, data=json.dumps(data))
        response.encoding = "utf-8"
        if response.status_code == 200:
            return response.text
        return None
    except requests.exceptions.RequestException:
        return None


def get_tab_detail(session, org_name):
    try:
        data = http_post(session, "http://10.30.4.101:9001/elasticsearch", org_name)
        # print(json.dumps(json.loads(data), indent=4, ensure_ascii=False))
        data_json = json.loads(data)
        tab_detail = [
            data_json.get("data").get("allCheckInCount"),
            data_json.get("data").get("allFileCount"),
            data_json.get("data").get("allPageCount")
        ]
        return tab_detail
    except Exception as e:
        print("Error in fetching tab_detail from %s:" % org_name, e)
        return [np.nan, np.nan, np.nan]


def get_week_day(string, string_format=STRING_FORMAT_FULL_SECOND):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(string, string_format))).isoweekday()


def previous_period(legal_list):
    if len(legal_list) == 0:
        print("Error: illegal weekday list!")
        return time.time()
    today = datetime.date.fromtimestamp(time.time())  # - datetime.timedelta(days=5)
    today_stamp = time.mktime(today.timetuple())
    previous = today - datetime.timedelta(days=1)
    while previous.isoweekday() not in legal_list:
        previous -= datetime.timedelta(days=1)
    previous_stamp = time.mktime(previous.timetuple())
    return previous_stamp, today_stamp


if __name__ == "__main__":
    res = read_log_files(r"./logs")
    print_df(res)
    # res.to_excel("11.xlsx")
    # df_test = read_log_files("logs")
    # print_df(df_test)
    # print(string_to_stamp("2021-05-08 18:11:47,488"[:19], STRING_FORMAT_FULL_SECOND))
    # test = previous_period([4, 7])
    # print(stamp_to_string(test))
    # print("1".isdigit())
    # print("s".isdigit())
    # print("我".isdigit())
    # print("1".isalpha())
    # print("s".isalpha())
    # print("我".isalpha())
    # print(ord("1"))
    # print(ord("s"))
    # print(ord("我"))
    # session = requests.Session()
    # res = get_tab_detail(session, "杭州杭富轨道交通有限公司")
    # print(res)
    # set_excel_style("saves/中登网开通用户信息清单_20210428_152248.xlsx", ["远东国际融资租赁有限公司", "民生银行上海分行"])
    # res = pd.read_excel("saves/中登网开通用户信息清单_20210428_151002.xlsx", sheet_name=STRING_SHEET_DETAILS)
    # build_tab(res, "远东国际融资租赁有限公司")
    # res = get_log_paths(r"D:\Workspace\zhongdeng_daily\code\logs")
    # print(res)
    # res = read_log_file("logs/user_query.log.2021-04-26")
    # print_df(res)
