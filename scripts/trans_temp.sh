#!/bin/bash
while read line
do
  res=$(echo "scale=2;((9/5) * $line) + 32" | bc )
  echo "$res"
done