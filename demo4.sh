#!/bin/bash
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat1&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "1000+1000*s(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat2&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "1000+1000*c(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
