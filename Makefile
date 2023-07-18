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
