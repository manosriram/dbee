#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX 5
#define NIL '\0'

struct BTreeNode {
    int keys[MAX];
    int children[MAX + 1];
    int parent;
    int offset;
    /* struct BTreeNode *children[MAX + 1]; */
    /* struct BTreeNode *parent; */
    int is_leaf_node, key_count;
};

void *tree;
int num_nodes = 0;

const uint32_t BTREE_NODE_SIZE = sizeof(struct BTreeNode);

const uint32_t NODE_OFFSET_SIZE = sizeof(int);
const uint32_t NODE_PARENT_SIZE = sizeof(int);
const uint32_t NODE_IS_LEAF_NODE_SIZE = sizeof(int);
const uint32_t NODE_KEYS_SIZE = sizeof(int[MAX]);
const uint32_t NODE_CHILDREN_SIZE = sizeof(int[MAX + 1]);
const uint32_t NODE_KEY_COUNT_SIZE = sizeof(int);

const uint32_t NODE_OFFSET_OFFSET = 0;
const uint32_t NODE_PARENT_OFFSET = NODE_OFFSET_SIZE;
const uint32_t NODE_IS_LEAF_NODE_OFFSET = NODE_PARENT_OFFSET + NODE_PARENT_SIZE;
const uint32_t NODE_KEYS_OFFSET =
    NODE_IS_LEAF_NODE_OFFSET + NODE_IS_LEAF_NODE_SIZE;
const uint32_t NODE_CHILDREN_OFFSET = NODE_KEYS_OFFSET + NODE_KEYS_SIZE;
const uint32_t NODE_KEY_COUNT_OFFSET =
    NODE_CHILDREN_OFFSET + NODE_CHILDREN_SIZE;

void *addresses[100];

void serialize_row(void *destination, struct BTreeNode *row) {
    memcpy(destination + NODE_OFFSET_OFFSET, &(row->offset), NODE_OFFSET_SIZE);
    memcpy(destination + NODE_PARENT_OFFSET, &(row->parent), NODE_PARENT_SIZE);
    memcpy(destination + NODE_IS_LEAF_NODE_OFFSET, &(row->is_leaf_node),
           NODE_IS_LEAF_NODE_SIZE);
    memcpy(destination + NODE_KEYS_OFFSET, &(row->keys), NODE_KEYS_SIZE);
    memcpy(destination + NODE_CHILDREN_OFFSET, &(row->children),
           NODE_CHILDREN_SIZE);
    memcpy(destination + NODE_KEY_COUNT_OFFSET, &(row->key_count),
           NODE_KEY_COUNT_SIZE);
}

void deserialize_row(void *source, struct BTreeNode *destination) {
    memcpy(&(destination->offset), source + NODE_OFFSET_OFFSET,
           NODE_OFFSET_SIZE);
    memcpy(&(destination->parent), source + NODE_PARENT_OFFSET,
           NODE_PARENT_SIZE);
    memcpy(&(destination->is_leaf_node), source + NODE_IS_LEAF_NODE_OFFSET,
           NODE_IS_LEAF_NODE_SIZE);
    memcpy(&(destination->keys), source + NODE_KEYS_OFFSET, NODE_KEYS_SIZE);
    memcpy(&(destination->children), source + NODE_CHILDREN_OFFSET,
           NODE_CHILDREN_SIZE);
    memcpy(&(destination->key_count), source + NODE_KEY_COUNT_OFFSET,
           NODE_KEY_COUNT_SIZE);
}

struct BTreeNode *new_btree_node(int key, int node_offset, int is_leaf_node) {
    struct BTreeNode *node =
        (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
    node->is_leaf_node = is_leaf_node;
    node->parent = NIL;
    node->keys[0] = key;
    node->offset = node_offset;
    for (int t = 0; t <= MAX; t++) {
        node->children[t] = NIL;
    }
    node->key_count = 1;
    return node;
}

// use ftell to get offset location and use that

void *get_offset_address(int offset) {
    void *offset_address = malloc(BTREE_NODE_SIZE);

    int file_descriptor = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    lseek(file_descriptor, offset, SEEK_SET);
    /* int bytes_read = read(file_descriptor, offset_address, BTREE_NODE_SIZE); */
    return offset_address;
}

int flush_to_disk() {
    int file_descriptor = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
		/* void *addr = get_offset_address(0); */
		int done = 0, c = 0;
		int sz = 0;

		while (!done) {
				lseek(file_descriptor, 0, SEEK_SET);

				int bytes_written = write(file_descriptor, addr, 1000);
				if (addresses[c++] != NULL) {
						done = 1;
				}
		}

    return bytes_written;
}

void insert(struct BTreeNode *, int);
/* struct BTreeNode *split_node(struct BTreeNode *); */

int get_insert_position(struct BTreeNode *root, int key) {
    for (int t = 0; t < MAX; t++) {
        if (root->keys[t] == NIL || root->keys[t] > key) {
            return t;
        }
    }
    return -1;
}

struct BTreeNode *get_node_from_offset(int offset) {
		void *offset_address;
		if (addresses[offset]) {
				offset_address = addresses[offset];
		}
		else {
				offset_address = get_offset_address(offset);
				addresses[offset] = offset_address;
		}

    struct BTreeNode *row =
        (struct BTreeNode *)malloc(sizeof(struct BTreeNode));

    deserialize_row(offset_address, row);
    return row;
}

int get_current_offset() {
		return num_nodes * BTREE_NODE_SIZE;
		/* void *prev; */
		/* int c = 0; */
		/* while (1) { */
				/* if (addresses[c++] == NULL) { */
						/* return prev; */
				/* } */
		/* } */
}

/* void insert_at_offset(int offset, struct BTreeNode *node) { */

/* } */

struct BTreeNode *push_to_parent_node(struct BTreeNode *root,
                                      struct BTreeNode *left_child,
                                      struct BTreeNode *right_child, int key) {
    struct BTreeNode *parent = get_node_from_offset(root->parent);
    int key_position = get_insert_position(parent, key);
    for (int t = MAX - 1; t > key_position; t--) {
        parent->keys[t] = parent->keys[t - 1];
        parent->children[t] = parent->children[t - 1];
    }
    parent->keys[key_position] = key;
    parent->key_count += 1;

    parent->children[key_position] = left_child->offset;
    parent->children[key_position + 1] = right_child->offset;

    parent->is_leaf_node = 0;

    return parent;
}

struct BTreeNode *split_node(struct BTreeNode *root) {
    if (!root)
        return NULL;
    struct BTreeNode *left, *right;
    int median = MAX / 2;

    left =
        new_btree_node(root->keys[0], get_current_offset(), root->is_leaf_node);
    for (int t = 1; t < median; t++) {
        left->keys[t] = root->keys[t];
        left->key_count++;
    }

    int MAX_CHILDREN = MAX + 1;

    for (int t = 0; t < MAX_CHILDREN / 2; t++) {
        if (root->children[t] != NIL) {
            left->children[t] = root->children[t];
        }
    }

		num_nodes += 1;

    right = new_btree_node(root->keys[median + 1], get_current_offset(),
                           root->is_leaf_node);
    int right_pos = 1;
    for (int t = median + 2; t < MAX; t++) {
        right->keys[right_pos] = root->keys[t];
        right->key_count++;
        right_pos++;
    }

    right_pos = 0;
    for (int t = median + 1; t <= MAX; t++) {
        if (root->children[right_pos] != NIL) {
            right->children[right_pos] = root->children[t];
            right_pos++;
        }
    }

		num_nodes += 1;

    for (int t = 0; t < MAX; t++) {
        if (left->children[t])
            left->children[t] = left->offset;
        if (right->children[t])
            right->children[t] = right->offset;
    }

    if (root->parent != NIL) {
        root = push_to_parent_node(root, left, right, root->keys[median]);
        right->parent = root->offset;
        left->parent = root->offset;
    } else {
        root->is_leaf_node = 0;
        root->keys[0] = root->keys[median];
        root->key_count = 1;
        for (int t = 1; t < MAX; t++) {
            root->keys[t] = NIL;
        }
        for (int t = 0; t < MAX + 1; t++)
            root->children[t] = NIL;

        root->children[0] = left->offset;
        root->children[1] = right->offset;
        root->parent = NIL;

        right->parent = root->offset;
        left->parent = root->offset;
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
        for (int t = MAX - 1; t > position; t--) {
            root->keys[t] = root->keys[t - 1];
            root->children[t] = root->children[t - 1];
        }
        root->keys[position] = key;
        root->key_count += 1;
    } else {
        root->keys[position] = key;
        root->key_count += 1;
    }

    void *destination = get_offset_address(get_current_offset() + BTREE_NODE_SIZE);
    serialize_row(destination, root);
    /* num_nodes += 1; */

    if (root->key_count == MAX) {
        while (root->key_count == MAX) {
            root = split_node(root);
        }
    } else {
				num_nodes += 1;
		}
}

void find_and_insert_node(int root_offset, int key) {
		if (root_offset < 0) return;

		struct BTreeNode *root = get_node_from_offset(root_offset);
    if (root->is_leaf_node) {
				/* printf("inserting %d ", key); */
        insert(root, key);
        return;
    }

    for (int t = 0; t < MAX; t++) {
        if (root->keys[t] != NIL)
            return find_and_insert_node(root->children[t], key);

        if (root->keys[t] > key) {
            return find_and_insert_node(root->children[t], key);
        }
    }

    return find_and_insert_node(root->children[root->key_count], key);
}

void print(int root_offset) {
    struct BTreeNode *root = get_node_from_offset(root_offset);

    if (!root)
        return;

    for (int t = 0; t < MAX; t++) {
        if (root->keys[t] != NIL) {
            printf("%d ", root->keys[t]);
        }
    }

    printf("\n");
    for (int t = 0; t < MAX + 1; t++) {
        if (root->children[t] != NIL) {
            print(root->children[t]);
        }
    }
}


int main() {
    /* void *addr = get_offset_address(1); */
    /* char *a; */
    /* memcpy(a, addr, 2); */
    /* printf("%s", a); */
    /* tree = malloc(400); */

		int off = get_current_offset();
    struct BTreeNode *root = new_btree_node(10, off, 1);
		num_nodes = 1;

		void *addr = get_offset_address(root->offset);
		serialize_row(addr, root);

		addresses[off] = addr;

		find_and_insert_node(0, 12);
		find_and_insert_node(0, 11);

		for (int t=0;t<10;t++) {
				printf("%p ", addresses[t]);
		}
		/* addr = get_offset_address(addresses[0]); */
		flush_to_disk();

		for (int t=0;t<MAX;t++) {
				printf("%d ", root->keys[t]);
		}

    /* find_and_insert_node(root, 13); */ /* find_and_insert_node(root, 12); */

    /* int bytes_written = write(file_descriptor, tree, 400); */

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

    /* print(0); */
    return 0;



		/* struct BTreeNode *r = (struct BTreeNode *) malloc(sizeof(struct BTreeNode)); */
		/* deserialize_row(addr, r); */
		/* printf("%d\n", r->keys[0]); */
}
