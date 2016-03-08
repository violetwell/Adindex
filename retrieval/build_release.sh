#!/bin/bash

make -f Makefile.srch all
echo "Please enter thrift service port:"
read PORT
echo $PORT
echo $PORT > config/conf.txt
echo "run search_server to start."

