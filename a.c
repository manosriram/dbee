#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int x[2];
} X;

int main() {
    /* int a[5]; */
    /* a[0] = 100; */
    /* a[1] = 200; */
    /* a[2] = 300; */

    /* void *address = &a; */
    /* int copy_address[5]; */

    /* memcpy(copy_address, address, sizeof(address)); */

    /* printf("%d\n", copy_address[1]); */

    printf("%lu\n", sizeof(X));
    return 0;
}
