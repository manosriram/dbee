#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct Node {
		char name[32];
		int x[2];
		int offset;
		int left, right;
		int is_leaf;
};

struct Node *get_node_from_offset(int offset) {
		struct Node *node = malloc(sizeof(struct Node));
		void *offset_address = malloc(1000);

    int file_descriptor = open("write", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
		lseek(file_descriptor, offset, SEEK_SET);
		int bytes_read = read(file_descriptor, offset_address, 1000);

		memcpy(&(node->name), offset_address, 32);
		memcpy(&(node->offset), offset_address + 32, 4);
		memcpy(&(node->is_leaf), offset_address + 36, 4);
		memcpy(&(node->left), offset_address + 40, 4);
		memcpy(&(node->right), offset_address + 44, 4);

		return node;
}

void print(int root_offset) {
		if (root_offset == '\0') return;

		struct Node *from_off = get_node_from_offset(root_offset);

		if (from_off->is_leaf == 1) {
				printf("row, %s -- %d\n", from_off->name, from_off->offset);
				return;
		}

		print(from_off->left);
		print(from_off->right);
}

int main() {

		struct Node *node = malloc(sizeof(struct Node));

		memcpy(&(node->name), "manosriram", 10);
		node->offset = 2;
		node->is_leaf = 0;
		node->left = node->offset + sizeof(struct Node);
		node->right = node->offset + (2 * sizeof(struct Node));


		struct Node *left = malloc(sizeof(struct Node));

		memcpy(&(left->name), "rann", 4);
		left->offset = 2 + sizeof(struct Node);
		left->is_leaf = 1;
		left->left = '\0';
		left->right = '\0';

		struct Node *right = malloc(sizeof(struct Node));

		memcpy(&(right->name), "gonn", 5);
		right->offset = 2 + (2 * sizeof(struct Node));
		right->is_leaf = 1;
		right->left = '\0';
		right->right = '\0';

		void *x = malloc(150);
		memcpy(x, &(node->name), 32);
		memcpy(x + 32, &(node->offset), 4);
		memcpy(x + 36, &(node->is_leaf), 4);
		memcpy(x + 40, &(node->left), 4);
		memcpy(x + 44, &(node->right), 4);

		memcpy(x + 48, &(left->name), 32);
		memcpy(x + 80, &(left->offset), 4);
		memcpy(x + 84, &(left->is_leaf), 4);
		memcpy(x + 88, &(left->left), 4);
		memcpy(x + 92, &(left->right), 4);

		memcpy(x + 96, &(right->name), 32);
		memcpy(x + 128, &(right->offset), 4);
		memcpy(x + 132, &(right->is_leaf), 4);
		memcpy(x + 136, &(right->left), 4);
		memcpy(x + 140, &(right->right), 4);

    int file_descriptor = open("write", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
		lseek(file_descriptor, node->offset, SEEK_SET);
		int bytes_written = write(file_descriptor, x, 150);

		/* struct Node *from_off = get_node_from_offset(node->offset); */
		/* printf("%s, %d\n", from_off->name, from_off->offset); */

		/* printf("%d", node->offset); */
		print(node->offset);
		return 0;
}
