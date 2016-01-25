
import requests, re
from datetime import datetime
from bs4 import BeautifulSoup

class NameNodeLog(object):

    MAXSIZE = 2147480000
    TYPE_NORMAL = "normal"
    TYPE_AUDIT = "audit"
    time_format = "%b %d, %Y %I:%M:%S %p"
    normal_ptn = r"hadoop-hdfs-namenode-cms-nn00\.rcac\.purdue\.edu\.log.*"
    audit_ptn = r"hdfs-audit\.log.*"

    def __init__(self, fname, link, size, modify_time, t):
        self.ori_name = fname
        self.link = link
        self.size = size
        self.modify_time = datetime.strptime(modify_time, NameNodeLog.time_format)
        self.type = t

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return " ".join(["NameNodeLog", self.ori_name, str(self.size), self.modify_time.strftime(self.time_format), self.type])

    def log_ready(self):
        return self.size >= NameNodeLog.MAXSIZE and self.ori_name.split(".")[-1] != "log"


def getNameLogList(text):

    soup = BeautifulSoup(text, "html.parser")

    re_normal = re.compile(NameNodeLog.normal_ptn)
    re_audit = re.compile(NameNodeLog.audit_ptn)

    normal_list = []
    audit_list = []

    for l in soup.text.split('\n'):
        #print(l)
        fields = l.replace(u'\xa0', " ").split()
        #print(fields)

        if len(fields) < 3:
            continue

        name = fields[0]
        size = int(fields[1])
        mtime = " ".join(fields[3:])
        link = name

        if re_normal.match(name):
            print(name + " normal")
            normal_list.append(NameNodeLog(name, link, size, mtime, NameNodeLog.TYPE_NORMAL))
        elif re_audit.match(name):
            print(name + " audit")
            audit_list.append(NameNodeLog(name, link, size, mtime, NameNodeLog.TYPE_AUDIT))
        else:
            print("wrong type")
            continue

    #print(normal_list)
    return normal_list, audit_list









nlog = NameNodeLog("hadoop-hdfs-namenode-cms-nn00.rcac.purdue.edu.log.3", None, 2147483681, "Dec 22, 2015 9:54:21 AM", NameNodeLog.TYPE_NORMAL)

print(nlog)
print(nlog.log_ready())
hdfs = "http://cms-nn00.rcac.purdue.edu:50070/dfshealth.jsp"
namenode_log_link = "http://cms-nn00.rcac.purdue.edu:50070/logs/"
namenode_log_page = requests.get(namenode_log_link)

#print(namenode_log_page.text)
normal_list, audit_list = getNameLogList(namenode_log_page.text)

testlog = normal_list[0]
testlog_link = namenode_log_link + testlog.link
print(testlog_link)


