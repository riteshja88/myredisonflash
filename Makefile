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
