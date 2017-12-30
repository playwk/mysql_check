# -*- coding: utf-8 -*-
# @author ZhengZhong,Jiang
# @time 2017/12/12 19:54

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import datetime
from subprocess import Popen, PIPE

import psutil
import pymysql
import configparser
import xlrd, xlwt
from xlutils.copy import copy

#nowtime = time.strftime('%Y-%m', time.localtime(time.time()))
nowtime = datetime.datetime.now().strftime('%Y%m%d')
logtime = datetime.datetime.now().strftime('%Y-%m')
baktime = datetime.datetime.now().strftime('%Y-%m-%d')


font = xlwt.Font()
font.name = 'Verdana'
font.size = 11
style = xlwt.XFStyle()
style.font = font
alignment = xlwt.Alignment()
alignment.wrap = 1
alignment.horz = xlwt.Alignment.HORZ_LEFT
alignment.vert = xlwt.Alignment.VERT_CENTER
style.alignment = alignment


class Check:
    def __init__(self, role):
        self.role = role
        config = configparser.ConfigParser()
        config.read('/home/db/mysql_check/check.conf')

        try:
            self.host = config.get(self.role, 'host')
            self.port = int(config.get(self.role, 'port'))
            self.user = config.get(self.role, 'user')
            self.password = config.get(self.role, 'password')
            self.data_vol = config.get(self.role, 'data_vol')
            self.check_dbs = config.get(self.role, 'check_dbs')
            self.errlog_path = config.get(self.role, 'errlog_path')
            self.errlog_num = int(config.get(self.role, 'errlog_num'))

            if self.role == 'master':
                self.bak_vol = config.get(self.role, 'bak_vol')
        except ValueError:
            print('获取状态值失败!')

    def syncstatus(self):
        if self.role == 'slave':
            status = u'不正常'
            try:
                conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password)
                cur = conn.cursor()
                cur.execute('show slave status;')
                for n in cur.fetchall():
                    if n[10] == n[11] == 'Yes':
                        status = u'正常'
            except pymysql.Error as e:
                print("pymysql Error!", e)
            return status
        else:
            return u'-'

    def data_vol_used(self):
        data_vol_used = psutil.disk_usage(self.data_vol).percent
        return "%s %s%%" % (self.data_vol, data_vol_used)

    def status(self):
        res = Popen("mysql -h%s -P%s -u%s -p%s -e'status'" % (self.host,
            str(self.port), self.user, self.password), shell=True, stdout=PIPE)
        return res.stdout.readlines()

    def uptime(self):
        res = self.status()[-5].strip("\n")
        return res.split(':')[1].strip("\t")

    def basestatus(self):
        res = self.status()
        return res[-3].strip("\n")

    def data_size(self):
        try:
            conn = pymysql.connect(host=self.host, port=self.port,
                                   user=self.user, password=self.password)
            cur = conn.cursor()
            cur.execute("SELECT concat(truncate((sum(DATA_LENGTH)+sum(INDEX_LENGTH))/1024/1024, 2), 'MB') as data_size FROM information_schema.TABLES where TABLE_SCHEMA='%s'" % self.check_dbs)
            return cur.fetchall()[0][0]
        except pymysql.Error as e:
            return "pymysql Error! %s" % e

    def conn(self):
        try:
            conn_list = []
            conn = pymysql.connect(host=self.host, port=self.port,
                                   user=self.user, password=self.password
                                   )
            cur = conn.cursor()
            cur.execute("show processlist;")
            current_conn = len(cur.fetchall())
            conn_list.append(current_conn)
            cur.execute("show status like 'Max_used_connections';")
            max_used_conn = cur.fetchall()[0][1]
            conn_list.append(max_used_conn)
            cur.close()
            conn.close()
            return conn_list 
        except pymysql.Error as e:
            print("pymysql Error!", e)
            return False

    def log_alarm(self):
        if self.role == 'slave':
            log_filter = Popen("grep '^%s' %s" % (logtime, self.errlog_path), shell=True,
                              stdout=PIPE)
            log_alarm = Popen("grep -iw '\[error\]'", shell=True, stdin=log_filter.stdout, stdout=PIPE)
            log_alarm = ''.join(log_alarm.stdout.readlines()[-self.errlog_num:])
        elif self.role == 'master':
            import paramiko
            hostname=self.host
            port=22
            username='root'
            pkey='/root/.ssh/id_rsa'
            key=paramiko.RSAKey.from_private_key_file(pkey)
            s=paramiko.SSHClient()
            s.load_system_host_keys()
            s.connect(hostname, port, username, key)
            cmd = "grep %s %s|grep -iw '\[error\]'" % (logtime, self.errlog_path)
            stdin,stdout,stderr = s.exec_command("grep %s %s|grep -iw '\[error\]'|tail -%s" \
						% (logtime, self.errlog_path, self.errlog_num))
            log_alarm = stdout.read()
            tmp = log_alarm
        if not log_alarm:
            return u'无告警'
        else:
            return "%s %s" % (u'有告警', log_alarm)

    def bakup_status(self):
	if self.role == 'master':
            try:
                os.path.getsize("%s/mmcfull_%s.tar.gz" % (self.bak_vol, baktime))
                bakup_res = u'正常'
            except:
                bakup_res = u'不正常'
            bak_vol_used = psutil.disk_usage(self.bak_vol).percent
            return  "%s %s %s%%" % (bakup_res, self.bak_vol, bak_vol_used)
        else:
            return u'-'

    def mycat_status(self):
	if self.role == 'master':
            flag = 1
            for pid in psutil.pids():
                p = psutil.Process(pid)
                if "wrapper-linux-x86-64" in p.name():
                    flag = 0
                    return u'正常'
            if flag:        
                return u'不正常'
        else:
            return u'-'

    def copy_new_sheet(self):
        rb = xlrd.open_workbook('/home/db/mysql_check/mmc_db_check.xls', formatting_info=True, encoding_override='utf-8')
        wb = copy(rb)
        rs = rb.sheet_by_index(0)
        wb.add_sheet(nowtime)
        ws = wb.get_sheet(-1)
        for r in range(rs.nrows):
            for c in range(rs.ncols):
                cwidth = ws.col(c).width
                if len(str(rs.cell_value(r, c))) * 40 > cwidth:
                    ws.col(c).width = len(str(rs.cell_value(r, c))) * 40
                ws.write(r, c, rs.cell_value(r, c), style)
        wb.save('/home/db/mysql_check/mmc_db_check.xls')

    def put_new_value(self):
        rb = xlrd.open_workbook('/home/db/mysql_check/mmc_db_check.xls', formatting_info=True, encoding_override='utf-8')
        wb = copy(rb)
        ws = wb.get_sheet(-1)
        if self.role == 'master':
            ws.write(1, 1, self.data_vol_used(), style)
            ws.write(2, 1, self.basestatus(), style)
            ws.write(3, 1, self.uptime(), style)
            ws.write(4, 1, self.data_size(), style)
            ws.write(5, 1, self.conn()[0], style)
            ws.write(6, 1, self.conn()[1], style)
            ws.write(8, 1, self.log_alarm(), style)
            ws.write(9, 1, self.bakup_status(), style)
            ws.write(10, 1, self.mycat_status(), style)
        elif self.role == 'slave':
            ws.write(1, 2, self.data_vol_used(), style)
            ws.write(2, 2, self.basestatus(), style)
            ws.write(3, 2, self.uptime(), style)
            ws.write(4, 2, self.data_size(), style)
            ws.write(5, 2, self.conn()[0], style)
            ws.write(6, 2, self.conn()[1], style)
            ws.write(7, 2, self.syncstatus(), style)
            ws.write(8, 2, self.log_alarm(), style)
        wb.save('/home/db/mysql_check/mmc_db_check.xls')

if __name__ == '__main__':
    master_check = Check('master')
    master_check.copy_new_sheet()
    master_check.put_new_value()
    slave_check = Check('slave')
    slave_check.put_new_value()
