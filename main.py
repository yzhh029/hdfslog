
import requests
import re
import os
import time
import argparse

from datetime import datetime
from bs4 import BeautifulSoup
from usermask import LogMasker


class Log(object):

    STATUS_INIT = 0
    STATUS_DOWNLOAD = 1
    STATUS_MASKED = 2

    FAKE_DOWNLOAD = False

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
        if Log.FAKE_DOWNLOAD:
            with open(os.path.join(folder, self.name + "_" + str(counter)), 'w') as dfile:
            #dfile.write(f.text)
                print('saved to', os.path.join(folder, self.name + "_" + str(counter)))
        else:
            f = requests.get(self.link, stream=True)

            with open(os.path.join(folder, self.name + "_" + str(counter)), 'wb') as dfile:
                size = 0
                start = 0
                end = 0
                start = time.perf_counter()
                for block in f.iter_content(4* 1024 * 1024):
                    dfile.write(block)
                    end = time.perf_counter()
                    size += 4
                    print("\r\t" + str(size) + "MB speed: " + str(round(4096 / (end - start), 2)) + " KB/sec", end="")
                    start = time.perf_counter()
                print()

        self.status = Log.STATUS_DOWNLOAD

    def mask(self):
        masker = LogMasker(self.name)
        self.maskedLog = masker.mask_file()
        self.status = Log.STATUS_MASKED

    def __str__(self):
        return self.name + " " + self.size

    def __repr__(self):
        return self.__str__()

    """
    def __eq__(self, other):
        return self.acctime == other.acctime
    """

class LogPageParser(object):

    DN_LOGPATTERN = r"hadoop-hdfs-datanode-cms-\w\d{3}\.rcac\.purdue\.edu\.log\.\d"
    NN_LOGPATTERN = r"hdfs-audit\.log\.\d"
    TIME_FORMAT = "%b %d, %Y %I:%M:%S %p"

    def __init__(self, link, name_pattern):
        self.pagelink = link
        self.pattern = re.compile(name_pattern)
        self.pendingPattern = re.compile(name_pattern[:-4])
        #print(self.pendingPattern)

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
                l = Log(name, size, date_, nodeLink + name)
                logList.append(l)
                if name.split(".")[-1] == "1":
                    pendingLog = l
                #print(name, size, date_, nodeLink + name)


            """
            elif self.pendingPattern.match(cols[0].string) is not None:
                name = cols[0].string.strip()
                if name.split('.')[-1] == 'gz':
                    continue
                size = cols[1].string.split(' ')[0]
                date_ = datetime.strptime(cols[2].string, LogPageParser.TIME_FORMAT)
                pendingLog = Log(name, size, date_, nodeLink + name)
                #print("pending log", name, size, date_, nodeLink + name)
            """

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
        self.loglist = None

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

        if self.pendingLog is not None:
            for l in self.loglist:
                # find old .1 log in new list
                if l.acctime == self.pendingLog.acctime:
                    # if old .1 log is the .1 log in new list
                    # no newlog
                    if l.name == self.pendingLog.name:
                        print(self.name, "no new log")
                        self.newLog = None
                        break
                    # if old .1 log is not .1 log in new list
                    else:
                        print(self.name, "found new log")
                        #logname = l.name[:-2]
                        new_lognum = int(l.name.split('.')[-1])
                        self.newLog = []
                        for i in range(1, new_lognum):
                            for nl in self.loglist:
                                if int(nl.name.split(".")[-1]) < new_lognum:
                                    print(self.name, "new log", nl.name)
                                    self.newLog.append(nl)
                        break

        self.pendingLog = pendingLog

        """
        if self.pendingLog is None or int(self.pendingLog.size) <= int(pendingLog.size):
            self.newLog = False
        elif int(self.pendingLog.size) > int(pendingLog.size):
            print(self.name, "new log found")
            self.newLog = True

        self.pendingLog = pendingLog
        """

    def downloadAllLog(self):
        if self.log_counter == 0:
            for l in self.loglist:
                l.download(self.name, self.log_counter)
                self.log_counter += 1
        elif self.newLog is not None:
            for l in self.newLog:

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
        self.loglink = url + ":" + str(port) + '/logs/'
        self.loglist = None
        self.pendingLog = None
        self.liveDataNodes = []
        self.log_counter = 0

        if os.path.exists('namenode') is False:
            os.mkdir('namenode')

    def getLogList(self):

        print('namenode fetch log list')

        parser = LogPageParser(self.loglink, LogPageParser.NN_LOGPATTERN)
        self.loglist, pendingLog = parser.getLogList(self.loglink)

        if self.pendingLog is None or int(self.pendingLog.size) <= int(pendingLog.size):
            self.newLog = False
        elif int(self.pendingLog.size) > int(pendingLog.size):
            print('namenode', "new log found")
            self.newLog = True

        self.pendingLog = pendingLog

    def downloadAllLog(self):

        if self.log_counter == 0:

            for l in self.loglist:
                l.download('namenode', self.log_counter)
                self.log_counter += 1
            """
            self.loglist[1].download('namenode', self.log_counter)
            self.log_counter += 1
            """
        elif self.newLog:
            for l in self.loglist:
                if l.name[-2:] == '.1':
                    l.download('namenode', self.log_counter)
                    self.log_counter += 1


    def loop(self, interval_min):

        # dead loop
        self.liveDataNodes = self.getLiveDataNodes()
        while True:
            self.checkDNlive()
            #self.getLogList()
            #self.downloadAllLog()

            for dn in self.liveDataNodes:
                dn.getLogList()
                dn.downloadAllLog()

            time.sleep(interval_min * 60)

        #self.getLogList()

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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Log downloader for collecting HDFS audit logs")
    parser.add_argument('-nn', '--namenode', required=True, help='Namenode URL')
    parser.add_argument('--fake_dl', choices=['0', '1'], default='0', help='whether to download the log')
    parser.add_argument('-p', '--period', default= 5, help="time interval (mins) for checking new logs")

    args = parser.parse_args()

    print("namenode link\t\t\t", args.namenode)
    print("enable fake download\t", bool(int(args.fake_dl)))
    print("checking interval\t\t", args.period, "mins")

    Log.FAKE_DOWNLOAD = bool(int(args.fake_dl))
    #print(Log.FAKE_DOWNLOAD)

    cms = HDFSsite(args.namenode, 50070)
    cms.loop(int(args.period))







