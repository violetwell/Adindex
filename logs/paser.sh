#!/bin/bash

cat $1 | awk -F"[ ]" '{sum+=$18} END {print "Average time =  ", sum/NR}'
cat $1 | wc -l
cat $1 | awk -F"[ ]" '{if($18 < 10){sum++}} END {print "sum [0, 10)  = ", sum}'
cat $1 | awk -F"[ ]" '{if(9 < $18 && $18 < 20){sum++}} END {print "sum [10, 20)  = ", sum}'
cat $1 | awk -F"[ ]" '{if(19 < $18 && $18 < 30){sum++}} END {print "sum [20, 30)  = ", sum}'

