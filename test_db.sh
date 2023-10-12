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
insert -11 minus-eleven
select
.exit
EOF
