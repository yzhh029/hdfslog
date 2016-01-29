# HDFS log collector

Retrieve the log list from web ui

    namenode NNURL:50070/logs/
    datanode DNURL:50075/logs/

Retrieve live nodes from

    NNURL:50070/dfsnodelist.jsp?whatNodes=LIVE


# Install

## install required python package
Assume in ubuntu with python3

install lxml

    # sudo apt-get install python3-lxml
Other dependency

    # sudo pip3 install -f requirements.txt
    
# Usage

	python3 hdfsDownloader.py -nn NAMENODE_URL --fake_dl [0, 1] -p PERIOD
	
## parameters

	-nn NAMENODE_URL the url of namenode.
				 	 e.g. "http://192.168.1.1"
	--fake_dl [0, 1] whether to enable fake download
					 fake download only create dummy logs instead of download them from server
					 1 = enable
					 0 = disable
	-p PERIOS		 time interval (in minutes) in between
					 two log check. 
					 default is 5 mins
## Examples

### test with fake download 
    python3 hdfsLog.py -nn "http://192.168.1.1" --fake_dl 1 -p 5
    
### real run
	python3 hdfsLog.py -nn "http://192.168.1.1" --fake_dl 0 -p 5


