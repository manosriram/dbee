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
#define ROOT_OFFSET 44
#define PAGE_SIZE 4096

int get_next_block_offset(void *);
uint32_t *page_header_num_nodes_value(void *);

typedef struct {
    int id;
    char name[32];
} Row;

typedef struct {
    Row *keys[MAX];
    int children[MAX + 1];
    int is_leaf_node, key_count, parent, offset;
} BTreeNode;

void _serialize_row(void *destination, BTreeNode *source) {
    memcpy(destination, &(source->offset), 4);
    memcpy(destination + 4, &(source->parent), 4);
    memcpy(destination + 8, &(source->is_leaf_node), 4);
    memcpy(destination + 12, &(source->key_count), 4);

    void *dest = destination + 16;
    for (int t = 0; t < MAX; t++) {
        if (t >= source->key_count) {
            int x = 0;
            char ss[32];
            memset(ss, '\0', 32);

            memcpy(dest, &x, 4);
            memcpy(dest + 4, ss, 32);
            dest += 36;
            continue;
        }
        memcpy(dest, &(source->keys[t]->id), 4);
        dest += 4;
        memcpy(dest, &(source->keys[t]->name), 32);
        dest += 32;
    }
		// TODO
    memcpy(dest + 72, &(source->children), sizeof(int[MAX + 1]));
}

void _deserialize_row(void *source, BTreeNode *destination) {
    memcpy(&(destination->offset), source, 4);
    memcpy(&(destination->parent), source + 4, 4);
    memcpy(&(destination->is_leaf_node), source + 8, 4);
    memcpy(&(destination->key_count), source + 12, 4);
    void *src = source + 16;
    for (int t = 0; t < MAX; t++) {
        destination->keys[t] = (Row *)malloc(sizeof(Row));
        if (t >= destination->key_count) {
            int x = 0;
            char ss[32];
            memset(ss, '\0', 32);

            memcpy(&(destination->keys[t]->id), &x, 4);
            memcpy(&(destination->keys[t]->name), &ss, 32);
            src += 36;
            continue;
        }
        memcpy(&(destination->keys[t]->id), src, 4);
        src += 4;
        memcpy(&(destination->keys[t]->name), src, 32);
        src += 32;
    }
		// TODO
    memcpy(&(destination->children), src + 72, sizeof(int[MAX + 1]));
}

BTreeNode *new_btree_node(Row *key, int is_leaf_node, void *page) {
    BTreeNode *node =
        (BTreeNode *)malloc(sizeof(BTreeNode));
    node->is_leaf_node = is_leaf_node;
    node->parent = NIL;
    node->offset = get_next_block_offset(page);
    node->keys[0] = key;
    for (int t = 0; t <= MAX; t++) {
        node->children[t] = NIL;
        if (t > 0)
            node->keys[t] = NULL;
    }
    node->key_count = 1;
    return node;
}

void _insert(BTreeNode *, Row *, void *);
BTreeNode *_split_node(BTreeNode *, void *page);

int get_insert_position(BTreeNode *root, Row *key) {
    for (int t = 0; t < MAX; t++) {
        if (root->keys[t]->id == 0 || root->keys[t]->id > key->id) {
            return t;
        }
    }
    return -1;
}

BTreeNode *push_to_parent_node(BTreeNode *root,
                                      BTreeNode *left_child,
                                      BTreeNode *right_child,
                                      Row *key,
                                      void *page) {
    BTreeNode *parent = (BTreeNode *)malloc(sizeof(BTreeNode));
    _deserialize_row(page + root->parent, parent);

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

BTreeNode *_split_node(BTreeNode *root, void *page) {
    if (!root)
        return NULL;
    BTreeNode *left, *right;
    int median = MAX / 2;

    left = new_btree_node(root->keys[0], root->is_leaf_node, page);
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
    right = new_btree_node(root->keys[median + 1], root->is_leaf_node, page);
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

    left->offset = get_next_block_offset(page);
    *page_header_num_nodes_value(page) += 1;

    right->offset = get_next_block_offset(page);
    *page_header_num_nodes_value(page) += 1;

    for (int t = 0; t < MAX; t++) {
        if (left->children[t]) {
            BTreeNode *left_node_child =
                malloc(sizeof(BTreeNode));
            _deserialize_row(page + left->children[t], left_node_child);

            left_node_child->parent = left->offset;
            _serialize_row(page + left->children[t], left_node_child);
        }
        if (right->children[t]) {
            BTreeNode *right_node_child =
                malloc(sizeof(BTreeNode));
            _deserialize_row(page + right->children[t], right_node_child);

            right_node_child->parent = right->offset;
            _serialize_row(page + right->children[t], right_node_child);
        }
    }

    if (root->parent != NIL) {
        root = push_to_parent_node(root, left, right, root->keys[median], page);
        right->parent = root->offset;
        left->parent = root->offset;
    } else {
        root->is_leaf_node = 0;
        root->keys[0] = root->keys[median];
        root->key_count = 1;
        for (int t = 1; t < MAX; t++) {
            root->keys[t] = NULL;
						root->children[t] = NIL;
				}

        root->children[0] = left->offset;
        root->children[1] = right->offset;
        root->parent = NIL;

        right->parent = root->offset;
        left->parent = root->offset;
    }

    _serialize_row(page + root->offset, root);
    _serialize_row(page + left->offset, left);
    _serialize_row(page + right->offset, right);
    return root;
}

void _insert(BTreeNode *root, Row *key, void *page) {
    int position = get_insert_position(root, key);
    if (position == -1) {
        printf("got -1 position for %d\n", key->id);
        exit(1);
    }
    if (root->keys[position]->id != 0) {
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

    if (root->key_count == MAX) {
        while (root->key_count == MAX) {
            root = _split_node(root, page);
        }
    } else {
        *page_header_num_nodes_value(page) += 1;
        _serialize_row(page + root->offset, root);
    }
    // write_page(page);
}

void find_and_insert_node(int root_offset, Row *key, void *page) {
    if (root_offset == NIL)
        return;

    BTreeNode *root = malloc(sizeof(BTreeNode));
    _deserialize_row(page + root_offset, root);
    printf("got root %d, offset %d\n", root->keys[0]->id, root_offset);

    if (root->is_leaf_node) {
        _insert(root, key, page);
        return;
    }

    for (int t = 0; t < MAX; t++) {
        if (root->keys[t]->id == 0)
            return find_and_insert_node(root->children[t], key, page);

        if (root->keys[t]->id > key->id) {
            return find_and_insert_node(root->children[t], key, page);
        }
    }

    return find_and_insert_node(root->children[root->key_count], key, page);
}

void p(int offset, void *page) {
    if (offset == NIL)
        return;

    BTreeNode *root = malloc(sizeof(BTreeNode));
    _deserialize_row(page + offset, root);
    // if (root->key_count == 0) return;
		printf("got offset %d, root_offset %d, parent %d, is_leaf %d\n", offset, root->offset, root->parent, root->is_leaf_node);

    for (int t = 0; t < root->key_count; t++) {
        printf("id: %d, name: %s | \t", root->keys[t]->id, root->keys[t]->name);
    }
    printf("\n");

    for (int t = 0; t<=MAX; t++) {
        if (root->children[t] != NIL) {
						// printf("offset %d, child %d\n", root->offset, root->children[t]);
            p(root->children[t], page);
        }
    }
}
