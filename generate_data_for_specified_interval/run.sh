#!/usr/bin/env bash
startDate=$(date -d "$1")

endDate=$(date -d "$2")
while [ "$startDate" != "$endDate" ]; do
  echo "$startDate"
  sh prepare_data.sh "$startDate"
  startDate=$(date -d "$startDate + 1 hour")
done