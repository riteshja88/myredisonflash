#!/bin/bash
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat3&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "1000+1000*s(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat3_one_month_ago&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "900+900*s(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat4&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "1000+1000*c(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=stat4_one_month_ago&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "900+900*c(((4*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
