#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#define DEGREE 3


struct BTreeNode {
		int *keys;
		struct BTreeNode *children[DEGREE+1];
		struct BTreeNode *parent;
		int is_leaf_node;
		int filled_keys_count;
};

struct BTreeNode *insert(struct BTreeNode *, int);

int median(int n) {
		return (n)/2;
}

struct BTreeNode *new_btree(int root_value, int is_leaf_node) {
		struct BTreeNode *root = malloc(sizeof(struct BTreeNode));
		root->keys = (int *)malloc(DEGREE);

		root->keys[0] = root_value;
		root->parent = NULL;
		root->is_leaf_node = is_leaf_node;
		root->filled_keys_count = 1;
		for (int t=0;t<DEGREE;t++) {
				root->children[t] = NULL;
		}

		return root;
}

int search_insert_position(struct BTreeNode *root, int key) {
		for (int t=0;t<DEGREE;t++) {
				if (root->keys[t] > key) {
						/* printf("pos = %d\n", t); */
						return t;
				}
				if (root->keys[t] == NULL) {
						return t;
				}
		}
		return -1;
}

struct BTreeNode *split_node(struct BTreeNode *root) {
		int median_index = DEGREE/2;
		int median_value = root->keys[median_index];
		printf("median val = %d\n", median_value);

		/* struct BTreeNode *new_root = new_btree(median_value, 0); */

		struct BTreeNode *left_child = new_btree(root->keys[0], root->is_leaf_node);
		for (int t=1;t<median_index;t++) {
				insert(left_child, root->keys[t]);
		}

		struct BTreeNode *right_child = new_btree(root->keys[median_index + 1], root->is_leaf_node);
		/* printf("median + 1 val = %d\n", root->keys[median_index+1]); */
		for (int t=median_index+2;t<DEGREE;t++) {
				insert(right_child, root->keys[t]);
		}
		root->keys[0] = median_value;
		root->is_leaf_node = 0;

		root->children[0] = left_child;
		root->children[1] = right_child;
		return root;
		/* root = new_root; */
}

struct BTreeNode *insert(struct BTreeNode *root, int key) {
		if (!root) return NULL;
		/* if (!root) return new_btree(key, 1); */
		if (root->filled_keys_count == DEGREE - 1) {
				/* printf("splitting\n"); */
				return split_node(root);
		}

		int insert_position = search_insert_position(root, key);
		/* printf("posss = %d\n", insert_position); */
		if (root->keys[insert_position] != NULL) {
				for (int t=DEGREE-1;t>insert_position;t--) {
						root->keys[t] = root->keys[t-1];
				}
		}

		root->keys[insert_position] = key;
		root->filled_keys_count++;

		return root;
}

struct BTreeNode *find_and_insert_key(struct BTreeNode *root, int key) {
		if (root->is_leaf_node) {
				printf("inserting %d\n", key);
				return insert(root, key);
		}


		for (int t=0;t<root->filled_keys_count;t++) {
				if (root->keys[t] > key) {
						printf("recurring\n");
						return find_and_insert_key(root->children[t], key);
				}
		}
		return find_and_insert_key(root->children[root->filled_keys_count], key);
}

void print_tree(struct BTreeNode *root) {
		if (!root) return;

		for (int t=0;t<DEGREE;t++) {
				if (root->children[t] != NULL) {
						/* printf("recur children"); */
						print_tree(root->children[t]);
				}
		}

		for (int t=0;t<root->filled_keys_count;t++) {
				printf("%d ", root->keys[t]);
		}
}

int main() {
		struct BTreeNode *root = new_btree(10, 1);
		struct BTreeNode *real_root = root;
		root = find_and_insert_key(root, 12);
		root = find_and_insert_key(root, 11);
		root = find_and_insert_key(root, 13);
		/* printf("> = %d\n", root->keys[0]); */
		/* printf("is_leaf = %d\n", real_root->is_leaf_node); */
		/* printf("free = %d\n", root->filled_keys_count); */

		print_tree(root);

		return 0;
}
