all:
	go run myredisonflash.go

run: build
	./myredisonflash


build:
	go build myredisonflash.go

clean:
	rm -f myredisonflash


submitstats:
	curl -vv -d'stat=stat1&value=18446744073709551615' 'http://localhost:3333/submitstat'


perftest:
	echo "date +"%T.%N" > /tmp/time1" > a.sh;for((i=0;i<10000;i++)); do echo "curl -vv -d\"stat=stat${i}&value=1111\" 'http://10.172.141.84:3333/submitstat' &";done >> a.sh;echo "wait;date +"%T.%N" >> /tmp/time1;cat /tmp/time1" >> a.sh
	source a.sh

demo:
	echo "" > /tmp/data1;mkdir -p /tmp/keys
	rm -f /tmp/keys/*
	curl -vv -d'stat=stat1&ts=1679295600&value=1111' 'http://localhost:3333/submitstat'
	curl -vv -d'stat=stat1&ts=1679900400&value=1234' 'http://localhost:3333/submitstat'
	curl -vv -d'stat=stat2&ts=1679295600&value=9999' 'http://localhost:3333/submitstat'
	curl -vv -d'stat=stat2&ts=1679900400&value=4321' 'http://localhost:3333/submitstat'
	curl -vv 'http://localhost:3333/backup'
	cat /tmp/data1|jq -c
	cat /tmp/keys/stat1|jq -c
	cat /tmp/keys/stat2|jq -c


demo2:
	curl -vv -d'stat=stat1&start=1679295600&end=1679900400' 'http://localhost:3333/getstat'|jq -c
demo3:
	curl -vv -d'stat=stat2&start=1679295600&end=1679900400' 'http://localhost:3333/getstat'|jq -c

demo4:
	./demo4.sh

demo5:
	./histogram.sh
