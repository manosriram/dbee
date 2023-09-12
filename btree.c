#include <stdio.h>
#include <stdlib.h>

#define MAX 5
#define NIL '\0'

struct BTreeNode {
		int keys[MAX];
		struct BTreeNode *children[MAX+1];
		struct BTreeNode *parent;
		int is_leaf_node, key_count;
};


struct BTreeNode *new_btree_node(int key, int is_leaf_node) {
		struct BTreeNode *node = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
		node->is_leaf_node = is_leaf_node;
		node->parent = NULL;
		node->keys[0] = key;
		for (int t=0;t<=MAX;t++) {
				node->children[t] = NULL;
		}
		node->key_count = 1;
		return node;
}

void insert(struct BTreeNode *, int);
struct BTreeNode *split_node(struct BTreeNode *);

int get_insert_position(struct BTreeNode *root, int key) {
		for (int t=0;t<MAX;t++) {
				if (root->keys[t] == NIL || root->keys[t] > key) {
						return t;
				}
		}
		return -1;
}

struct BTreeNode *push_to_parent_node(struct BTreeNode *root, struct BTreeNode *left_child, struct BTreeNode *right_child, int key) {
		struct BTreeNode *parent = root->parent;
		int key_position = get_insert_position(parent, key);
		for (int t=MAX-1;t>key_position;t--) {
				parent->keys[t] = parent->keys[t-1];
				parent->children[t] = parent->children[t-1];
		}
		parent->keys[key_position] = key;
		parent->key_count += 1;

		parent->children[key_position] = left_child;
		parent->children[key_position + 1] = right_child;

		parent->is_leaf_node = 0;

		return parent;
}

struct BTreeNode *split_node(struct BTreeNode *root) {
		if (!root) return NULL;
		struct BTreeNode *left, *right;
		int median = MAX/2;

		left = new_btree_node(root->keys[0], root->is_leaf_node);
		for (int t=1;t<median;t++) {
				left->keys[t] = root->keys[t];
				left->key_count++;
		}

		int MAX_CHILDREN = MAX + 1;

		for (int t=0;t<MAX_CHILDREN/2;t++) {
				if (root->children[t] != NULL) {
						left->children[t] = root->children[t];
				}
		}

		right = new_btree_node(root->keys[median+1], root->is_leaf_node);
		int right_pos = 1;
		for (int t=median+2;t<MAX;t++) {
				right->keys[right_pos] = root->keys[t];
				right->key_count++;
				right_pos++;
		}

		right_pos = 0;
		for (int t=median+1;t<=MAX;t++) {
				if (root->children[right_pos] != NULL) {
						right->children[right_pos] = root->children[t];
						right_pos++;
				}
		}

		for (int t=0;t<MAX;t++) {
				if (left->children[t])
						left->children[t]->parent = left;
				if (right->children[t])
						right->children[t]->parent = right;
		}

		if (root->parent != NULL) {
				root = push_to_parent_node(root, left, right, root->keys[median]);
				right->parent = root;
				left->parent = root;
		} else {
				root->is_leaf_node = 0;
				root->keys[0] = root->keys[median];
				root->key_count = 1;
				for (int t=1;t<MAX;t++) {
						root->keys[t] = NIL;
				}
				for (int t=0;t<MAX+1;t++) root->children[t] = NULL;

				root->children[0] = left;
				root->children[1] = right;
				root->parent = NULL;

				right->parent = root;
				left->parent = root;
		}
		return root;
}

void insert(struct BTreeNode *root, int key) {

		int position = get_insert_position(root, key);
		if (position == -1) {
				printf("got -1 position for %d\n", key);
				exit(1);
		}
		if (root->keys[position] != NIL) {
				for (int t=MAX-1;t>position;t--) {
						root->keys[t] = root->keys[t-1];
						root->children[t] = root->children[t-1];
				}
				root->keys[position] = key;
				root->key_count += 1;
		} else {
				root->keys[position] = key;
				root->key_count += 1;
		}

		if (root->key_count == MAX) {
				while (root->key_count == MAX) {
						root = split_node(root);
				}
		}
}

void find_and_insert_node(struct BTreeNode *root, int key) {
		if (root->is_leaf_node) {
				insert(root, key);
				return;
		}

		for (int t=0;t<MAX;t++) {
				if (root->keys[t] == NIL)
						return find_and_insert_node(root->children[t], key);

				if (root->keys[t] > key) {
						return find_and_insert_node(root->children[t], key);
				}
		}

		return find_and_insert_node(root->children[root->key_count], key);
}

void print(struct BTreeNode *root) {
		if (!root) return;

		for (int t=0;t<MAX;t++) {
				if (root->keys[t] != NIL) {
						printf("%d ", root->keys[t]);
				}
		}

		printf("\n");
		for (int t=0;t<MAX+1;t++) {
				if (root->children[t] != NULL) {
						print(root->children[t]);
				}
		}
}

int main() {
		struct BTreeNode *root = new_btree_node(10, 1);

		find_and_insert_node(root, 12);
		find_and_insert_node(root, 11);
		find_and_insert_node(root, 13);
		find_and_insert_node(root, 12);
		/* find_and_insert_node(root, 14); */
		/* find_and_insert_node(root, 15); */

		/* find_and_insert_node(root, 16); */
		/* find_and_insert_node(root, 17); */
		
		/* find_and_insert_node(root, 18); */
		/* find_and_insert_node(root, 20); */

		/* find_and_insert_node(root, 21); */
		/* find_and_insert_node(root, 1); */
		/* find_and_insert_node(root, 2); */
		/* find_and_insert_node(root, 32); */
		/* find_and_insert_node(root, 33); */
		/* find_and_insert_node(root, 34); */

		print(root);
		return 0;
}
