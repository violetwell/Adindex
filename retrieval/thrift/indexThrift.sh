#!/bin/bash

rm -r indexThrift
mkdir indexThrift
thrift -out indexThrift --gen cpp index.thrift
