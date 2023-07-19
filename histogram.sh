#!/bin/bash
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub1&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "1000+1000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub2&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "2000+2000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub3&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "3000+3000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub4&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "4000+4000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub5&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "5000+5000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub6&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "6000+6000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
for((i=0;i<$((24*7*2));i++));do curl -vv -d"stat=pub7&ts=$((1679295600 +$i*(60*60)))&value=`printf %.0f $(echo "7000+7000*s(((2*3.14)/(24*7*2))*$i)"|bc -l)`" 'http://localhost:3333/submitstat';done
