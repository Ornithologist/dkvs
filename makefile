CC=gcc
CFLAGS=-O0 -fPIC -fno-builtin -lm
GO=go build

all: check

default: check

clean:
	rm -rf *.o proxy

proxy: proxy.go
	$(GO) proxy.go

%.o: %.c
	$(CC) $(CFLAGS) $< -c -o $@

check: proxy
	./proxy &
	./run_servers.sh &

dist:
	dir=`basename $$PWD`; cd ..; tar cvf $$dir.tar --exclude ./$$dir/.git ./$$dir; gzip $$dir.tar 