#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/log4cxx/lib:/usr/local/lib

echo "newIndex Restart..."
cd /opt/retrieval-ad/retrieval/
                                                                                                       
if [ -f /opt/retrieval-ad/retrieval/search_server ];                                                                                
then                   
	mpid=$(cat /opt/retrieval-ad/retrieval/master.pid)
	kill -9 $mpid
    nohup /opt/retrieval-ad/retrieval/search_server > /dev/null 2>&1 &                                                              
    echo $! > /opt/retrieval-ad/retrieval/master.pid                                                                               
	npid=$(cat /opt/retrieval-ad/retrieval/master.pid)
	#r_pid=`ps -p $npid|awk '{print \$1}'`
	
	if [ -d /proc/$npid ]
	then
		echo "Restart success!"
	else
		echo "Restart Failed..."
		echo "Restart again..."
		sleep 5
		kill -9 $npid
		nohup /opt/retrieval-ad/retrieval/search_server > /dev/null 2>&1 &
		echo $! > /opt/retrieval-ad/retrieval/master.pid
	fi
else                                                                                                   
    echo "Can't find newIndex sev file"                                                                   
fi 
