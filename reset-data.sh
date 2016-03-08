#!/bin/bash

echo "retrieval data clean"

rm -rf data
cp -rf data-clean data

echo "Please enter incrid:"
read INCRID
echo $INCRID
echo $INCRID > data/index/incrid.txt
echo "retrieval data clean finish."
