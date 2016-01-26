
import requests
import re
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from usermask import LogMasker


class Log(object):

    STATUS_INIT = 0
    STATUS_DOWNLOAD = 1
    STATUS_MASKED = 2

    def __init__(self, name, size, acctime, link):
        self.name = name
        self.size = size
        self.acctime = acctime
        self.link = link
        self.maskedLog = None
        self.status = Log.STATUS_INIT

    def download(self, folder, counter):

        print('downloading', self.name, self.acctime)

        #f = requests.get(self.link)
        with open(os.path.join(folder, self.name + "_" + str(counter)), 'w') as dfile:
            #dfile.write(f.text)
            print('saved to', os.path.join(folder, self.name + "_" + str(counter)))

        self.status = Log.STATUS_DOWNLOAD

    def mask(self):
        masker = LogMasker(self.name)
        self.maskedLog = masker.mask_file()
        self.status = Log.STATUS_MASKED


class LogPageParser(object):

    DN_LOGPATTERN = r"hadoop-hdfs-datanode-cms-\w\d{3}\.rcac\.purdue\.edu\.log\.\d"
    NN_LOGPATTERN = r"hdfs-audit\.log\.\d"
    TIME_FORMAT = "%b %d, %Y %I:%M:%S %p"

    def __init__(self, link, name_pattern):
        self.pagelink = link
        self.pattern = re.compile(name_pattern)
        self.pendingPattern = re.compile(name_pattern[:-4])
        print(self.pendingPattern)

    def getLogList(self, nodeLink):

        try:
            page = requests.get(self.pagelink)
        except:
            print("connection to", self.pagelink, " TIMEOUT")
            return [], None

        soup = BeautifulSoup(page.text, 'html.parser')

        logs = soup.find_all('tr')

        logList = []
        pendingLog = None

        for log in logs:
            cols = log.find_all('td')
            if self.pattern.match(cols[0].string) is not None:
                name = cols[0].string.strip()
                if name.split('.')[-1] == 'gz':
                    continue
                size = cols[1].string.split(' ')[0]
                date_ = datetime.strptime(cols[2].string, LogPageParser.TIME_FORMAT)
                logList.append(Log(name, size, date_, nodeLink + name))
                #print(name, size, date_, nodeLink + name)
            elif self.pendingPattern.match(cols[0].string) is not None:
                name = cols[0].string.strip()
                if name.split('.')[-1] == 'gz':
                    continue
                size = cols[1].string.split(' ')[0]
                date_ = datetime.strptime(cols[2].string, LogPageParser.TIME_FORMAT)
                pendingLog = Log(name, size, date_, nodeLink + name)
                #print("pending log", name, size, date_, nodeLink + name)

        return logList, pendingLog


class DataNode(object):

    STAT_DECOM = "Decommissioned"
    STAT_INSERVICE = "In Service"
    PORT = 50075

    def __init__(self, name, status):
        self.name = name
        self.link = "http://" + name + '.rcac.purdue.edu:' + str(DataNode.PORT)
        self.loglink = self.link + "/logs/"
        self.status = status
        self.pendingLog = None
        self.log_counter = 0
        self.live = False

        try:
            os.mkdir(name)
        except FileExistsError as e:
            pass

    def goLive(self):
        self.live = True

    def getLogList(self):

        print(self.name, "fetching log list")

        parser = LogPageParser(self.loglink, LogPageParser.DN_LOGPATTERN)
        self.loglist, pendingLog = parser.getLogList(self.loglink)

        if self.pendingLog is None or int(self.pendingLog.size) <= int(pendingLog.size):
            self.newLog = False
        elif int(self.pendingLog.size) > int(pendingLog.size):
            print(self.name, "new log found")
            self.newLog = True

        self.pendingLog = pendingLog

    def downloadAllLog(self):
        if self.log_counter == 0:
            for l in self.loglist:
                l.download(self.name, self.log_counter)
                self.log_counter += 1
        elif self.newLog:
            for l in self.loglist:
                if l.name[-2:] == '.1':
                    l.download(self.name, self.log_counter)
                    self.log_counter += 1

    def getNodeLink(self):
        return self.link

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return self.name + " " + self.link + " " + self.status

    def __repr__(self):
        return self.__str__()


class HDFSsite(object):

    """
    namenode of a hdfs site
    """

    def __init__(self, url, port):

        self.url = url
        self.port = port
        self.liveDataNodes = []

    def loop(self, interval_min):
        # dead loop
        self.liveDataNodes = self.getLiveDataNodes()
        while True:
            self.checkDNlive()
            for dn in self.liveDataNodes:
                if dn.name == 'cms-e000':
                    dn.getLogList()
                    dn.downloadAllLog()
            time.sleep(interval_min * 60)

    def checkDNlive(self):

        newLiveList = self.getLiveDataNodes()

        for olddn in self.liveDataNodes:
            if olddn not in newLiveList:
                print(olddn.name, "not live")
                self.liveDataNodes.remove(olddn)
            else:
                #print(olddn.name, "still live")
                newLiveList.remove(olddn)

        if len(newLiveList) > 0:
            print('new live nodes', newLiveList)
            self.liveDataNodes += newLiveList

    def getLiveDataNodes(self):

        DNlist_url = self.url + ":" + str(self.port) + '/dfsnodelist.jsp?whatNodes=LIVE'
        #print(DNlist_url)
        page = requests.get(DNlist_url)
        #print(page.encoding)
        soup = BeautifulSoup(page.text, 'lxml')  # has to use lxml lib to parse, html.parser doesnt work

        #soup = BeautifulSoup(open("dnlist.html"), 'lxml')
        nodeTable = soup.body.find('table', 'nodes')
        nodelist = nodeTable.find_all('tr')[1:]

        liveDataNodes = []

        for node in nodelist:
            #print(node)
            cols = node.find('td', class_="name")
            if cols is not None:
                name = cols.string
                status = node.find('td', class_="adminstate").string

                if status == DataNode.STAT_INSERVICE:
                    #newdn = DataNode()
                    liveDataNodes.append(DataNode(name, status))
        return liveDataNodes
        #self.printLiveDN()


    def getDataNode(self, index):
        if index < len(self.liveDataNodes):
            return self.liveDataNodes[index]
        else:
            return None

    def printLiveDN(self):
        if len(self.liveDataNodes) > 0:
            for node in self.liveDataNodes:
                print(node)



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

"""
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
"""
if __name__ == "__main__":

    cms = HDFSsite("http://cms-nn00.rcac.purdue.edu", 50070)
    cms.loop(1)







