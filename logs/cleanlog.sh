#!/bin/bash

SEARCHFILE=search.log.
INDEXFILE=index.log.

TIME=`date -d "2 days ago" +%Y-%m-%d`

rm -rf /opt/retrieval-ad/logs/$SEARCHFILE$TIME
rm -rf /opt/retrieval-ad/logs/$INDEXFILE$TIME
