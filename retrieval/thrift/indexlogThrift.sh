#!/bin/bash

rm -rf indexlogThrift
mkdir indexlogThrift
thrift -out indexlogThrift --gen cpp indexlog.thrift
