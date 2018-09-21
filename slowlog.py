#!/usr/bin/env python
#coding=utf-8

from __future__ import division
import os
import json
import sys
import time
import csv
import codecs
import smtplib
import urllib2
import datetime
import pandas as pd
from math import floor, ceil
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
#from datetime import timedelta, datetime
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
reload(sys)
sys.setdefaultencoding('utf8')


CURR_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIGFILE = CURR_DIR + os.sep + "config.ini"
SLOWLOGDIR = CURR_DIR + os.sep + "SLOW_LOG"

def csv_to_xlsx_pd(csv_file, excel_file):
    csv = pd.read_csv(csv_file, encoding='utf-8')
    csv.to_excel(excel_file, sheet_name='slowlog')
    writer.sheets['slowlog'].column_dimensions['执行开始时间'].width = 15


def localtrfutc(local_time):
    utc_time = local_time - datetime.timedelta(hours=8)
    #return utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return utc_time.strftime("%Y-%m-%dT%H:%MZ")

def utctrflocal(utc_time):
    UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    utc_st = datetime.datetime.strptime(utc_time, UTC_FORMAT)
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    local_time = local_st.strftime('%Y-%m-%d %H:%M:%S')
    return local_time

def sendmail(sender,password,receivers,smtpServer,subject,content,file_list):
    sender = sender
    password = password
    receivers = receivers
    smtp_server = smtpServer
    message = MIMEMultipart()
    message['From'] = sender
    message['To'] = ','.join(receivers)
    message['Subject'] = subject
    message.attach(MIMEText(content, 'plain', 'utf-8'))
    for f in file_list:
        file_name = os.path.basename(f)
        att = MIMEText(open(f, 'rb').read(), 'base64', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'
        att["Content-Disposition"] = 'attachment; filename=%s' %file_name
        message.attach(att)
    server = smtplib.SMTP(smtp_server,25)
    #server.set_debuglevel(1)
    server.login(sender,password)
    server.sendmail(sender,receivers,message.as_string())
    server.quit()

def GetSlowLogRecords(DBInstanceId, SecretId, SecretKey, RegionId, Description, StartTime, EndTime):

    client = AcsClient(SecretId, SecretKey, RegionId)

    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('rds.aliyuncs.com')
    request.set_method('POST')
    request.set_version('2014-08-15')
    request.set_action_name('DescribeSlowLogRecords')

    request.add_query_param('StartTime', StartTime)
    request.add_query_param('EndTime', EndTime)
    #request.add_query_param('EndTime', '2018-09-11T19:00Z')
    #request.add_query_param('DBInstanceId', 'rm-2zegda82nh2cw88rm')
    request.add_query_param('DBInstanceId', DBInstanceId)
    request.add_query_param('PageSize', '100')
    request.add_query_param('PageNumber', '1')


    response = client.do_action_with_exception(request)
    #TotalRecordCount = response['TotalRecordCount']
    #print TotalRecordCount
    response_py = json.loads(response)
    total_count = response_py['TotalRecordCount']
    page_count = int(ceil(total_count/100))
    #print total_count
    #print page_count
    res_items = []
    for i in range(1, page_count + 1 ):
        request.add_query_param('PageNumber', i)
        res = client.do_action_with_exception(request)
        res_items = res_items + response_py['Items']['SQLSlowRecord']
        #res_items
    #return json.dumps(res_items, indent=4, sort_keys=False)
    return res_items

if __name__ == '__main__':
    #StartTime = '2018-09-11T00:00Z'
    #EndTime = '2018-09-11T23:59Z'
    current_time = datetime.datetime.now()
    StartTime = localtrfutc(current_time - datetime.timedelta(days = 1))
    EndTime = localtrfutc(current_time)
    cfg = ConfigParser()
    cfg.read(CONFIGFILE)
    file_list = []
    for db in cfg.sections():
        DBInstanceId = str(cfg.get(db,"DBInstanceId"))
        SecretId = str(cfg.get(db,"SecretId"))
        SecretKey = str(cfg.get(db,"SecretKey"))
        RegionId = str(cfg.get(db,"RegionId"))
        Description = str(cfg.get(db,"Description"))
        #slow_log = SLOWLOGDIR + '/' + Description + '-' + DBInstanceId + '.log'
        slow_pre = SLOWLOGDIR + '/' + Description + '-' + DBInstanceId
        slow_excel = slow_pre + '.xlsx'

        file_list.append(slow_excel)
        ExecutionStartTime = []
        SQLText = []
        HostAddress = []
        DBName = []
        QueryTimes = []
        LockTimes = []
        ParseRowCounts = []
        ReturnRowCounts = []
        try:
            res = GetSlowLogRecords(DBInstanceId, SecretId, SecretKey, RegionId, Description, StartTime, EndTime)
            for r in res:
                slow_list = []
                ExeTime = utctrflocal(r['ExecutionStartTime'])
                ExecutionStartTime.append(ExeTime)
                SQLText.append(r['SQLText'].encode('utf-8'))
                HostAddress.append(r['HostAddress'])
                DBName.append(r['DBName'])
                QueryTimes.append(r['QueryTimes'])
                LockTimes.append(r['LockTimes'])
                ParseRowCounts.append(r['ParseRowCounts'])
                ReturnRowCounts.append(r['ReturnRowCounts'])
        except Exception, e:
            print e
            pass
        df = pd.DataFrame({'执行开始时间':    ExecutionStartTime,
                   'SQL语句': SQLText,
                   '客户端IP': HostAddress,
                   '数据库名': DBName,
                   '执行时长(秒)': QueryTimes,
                   '锁定时长(秒)': LockTimes,
                   '解析行数': ParseRowCounts,
                   '返回行数': ReturnRowCounts,
        })
        columns = ['执行开始时间', '客户端IP', '数据库名', '执行时长(秒)', '锁定时长(秒)', '解析行数', '返回行数', 'SQL语句']
        writer = pd.ExcelWriter(slow_excel, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', columns=columns)
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']
        #format1 = workbook.add_format({'num_format': '#,##0.00'})
        #format1 = workbook.add_format({'num_format': '#,##0'})
        format1 = workbook.add_format({'num_format': '0'})
        #format1 = workbook.add_format({'num_format': '0%'})
        #format2 = workbook.add_format({'num_format': '0%'})
        worksheet.set_column('B:B', 20, format1)
        #worksheet.set_column('C:C', None, format2)
        worksheet.set_column('C:C', 46, format1)
        worksheet.set_column('D:D', 20, format1)
        worksheet.set_column('E:E', 16, format1)
        worksheet.set_column('F:F', 16, format1)
        worksheet.set_column('G:G', 16, format1)
        worksheet.set_column('H:H', 16, format1)
        worksheet.set_column('I:I', 100, format1)
        writer.save()
    sender = 'pms@test.cn'
    receivers = ['luwen@test.cn', 'zhaomi@test.cn']
    receivers = ['luwen@test.cn']
    password = 'ooooxxxx'
    smtpServer = 'smtp.ooxx.com'
    subject = '阿里云rds慢日志'
    content = '阿里云rds慢日志汇总，请查看附件'
    sendmail(sender,password,receivers,smtpServer,subject,content,file_list)
