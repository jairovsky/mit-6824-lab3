#!/bin/bash

declare -i count
while [[ $? -eq 0 ]]; do
  count+=1
  echo testing... $count
  go test -race -run TestUnreliableChurn2C > ~/Downloads/tmp/log.log
done

notify-send "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" "Algum teste falhou"