.PHONY: all clean
all:

.PHONY: clean
clean:
	rm -rf obj

obj/libs3_proto.so: src/s3_proto.cpp src/s3_proto.h
	mkdir -p obj
	c++ -O2 -shared -fPIC src/s3_proto.cpp -laws-cpp-sdk-s3 -laws-crt-cpp -laws-cpp-sdk-core -lcurl -o obj/libs3_proto.so

all: obj/libs3_proto.so

/usr/local/lib/libs3_proto.so: obj/libs3_proto.so
	cp obj/libs3_proto.so /usr/local/lib

/usr/local/include/s3_proto.h: src/s3_proto.h
	cp src/s3_proto.h /usr/local/include

install: /usr/local/lib/libs3_proto.so /usr/local/include/s3_proto.h
	ldconfig

uninstall:
	rm -f /usr/local/lib/libs3_proto.so
	rm -f /usr/local/include/s3_proto.h
