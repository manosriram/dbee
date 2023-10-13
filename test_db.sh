#!/bin/sh

make clean;
gcc -g -o db db.c;
./db << EOF
insert 1 one
insert 2 two
insert 3 three
insert 4 four
insert 5 five
insert 6 six
insert 7 seven
insert 8 eight
insert 9 nine
insert 10 ten
insert 11 eleven
insert 12 eleven
insert 13 eleven
insert 14 eleven
insert 15 eleven
insert 16 eleven
insert 17 eleven
insert 18 eleven
insert 19 eleven
insert 20 eleven
select
.exit
EOF
