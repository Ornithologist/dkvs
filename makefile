CC=gcc
CFLAGS=-O0 -fPIC -fno-builtin -lm
GO=go build

all: check

default: check

clean:
	rm -rf *.o size size.dat size.png

proxy: proxy.go
	$(GO) proxy.go

%.o: %.c
	$(CC) $(CFLAGS) $< -c -o $@

check: proxy
	./proxy

dist:
	dir=`basename $$PWD`; cd ..; tar cvf $$dir.tar ./$$dir; gzip $$dir.tar 