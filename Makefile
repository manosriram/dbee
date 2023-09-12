build_run:
	gcc ./db.c && ./a.out

build:
	gcc ./db.c

run:
	./a.out

destroy:
	rm source

format:
	clang-format -i *.c

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
