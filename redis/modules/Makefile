# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	macVer := $(shell sh -c 'sw_vers -productVersion')
	SHOBJ_LDFLAGS ?= -arch x86_64 -macosx_version_min $(macVer) -bundle -undefined dynamic_lookup
endif

.SUFFIXES: .c .so .xo .o

all: integ.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

integ.xo: ../redismodule.h

integ.so: integ.xo
	$(LD) -o $@ $< $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so
