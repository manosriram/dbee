build_run:
	gcc -g -o db db.c && ./db

build:
	gcc -g -o db db.c

run:
	./db

clean:
	rm source tree db

format:
	clang-format -i *.c

test:
	chmod +x test_db.sh
	./test_db.sh

source:
ifneq ("$(wildcard source)","")
		hexdump -C ./source
else
		touch source && hexdump -C ./source
endif

watch_source:
ifneq ("$(wildcard source)","")
		watch hexdump -C ./source
else
		touch source && watch hexdump -C ./source
endif
