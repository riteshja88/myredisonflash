all:
	go run myredisonflash.go


run: build
	./myredisonflash


build:
	go build myredisonflash.go

clean:
	rm -f myredisonflash
