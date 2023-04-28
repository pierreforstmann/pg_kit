IDIR=$(shell pg_config --includedir)
INCLUDES=-I$(IDIR)
LDIR=$(shell pg_config --libdir)
LINKLIBS=-L$(LDIR)
CC=gcc

all:	pg_so	

pg_so:	pg_so.o 
	$(CC) -o pg_so pg_so.o $(LINKLIBS) -lpq
pg_so.o: pg_so.c 
	$(CC) -c pg_so.c $(INCLUDES)

clean:
	rm -f pg_so *.o
