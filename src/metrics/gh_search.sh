#!/bin/bash 
delay=300
while true; 
  do 
    wget -nc --limit-rate=20k https://api.github.com/search/repositories\?q\=created:{2016..2020}-{01..04}-{01..31}
    
    let delay+=delay
    sleep $delay
  done