#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX 3
#define NIL '\0'

#define ROOT_OFFSET 4

void *page;
static int fd;

int *number_of_nodes(void *node) { return (int *)node; }

int *root_node_offset(void *node) { return (int *)node + 4; }

struct Row {
    int id;
    char name[32];
};

/*
   root offset has to be updated when original root is split.
   need to add a new header space of 4bytes for root node's offset and update
   it during splitting.
*/

struct BTreeNode {
    struct Row *keys[MAX];
    int children[MAX + 1];
    /* struct BTreeNode *children[MAX + 1]; */
    /* struct BTreeNode *parent; */
    int is_leaf_node, key_count, parent, offset;
};

void insert(struct BTreeNode *, struct Row *);
struct BTreeNode *split_node(struct BTreeNode *);

void _serialize_row(void *destination, struct BTreeNode *source) {
    memcpy(destination, &(source->offset), 4);
    memcpy(destination + 4, &(source->parent), 4);
    memcpy(destination + 8, &(source->is_leaf_node), 4);
    memcpy(destination + 12, &(source->key_count), 4);
    void *dest = destination + 16;
    int t;
    for (t = 0; t < source->key_count; t++) {
        memcpy(dest, &(source->keys[t]->id), 4);
        memcpy(dest + 4, &(source->keys[t]->name), 32);
        dest += 36;
    }
    for (int t = source->key_count + 1; t < MAX; t++)
        dest += 36;

    destination = dest;

    memcpy(destination, &(source->children), sizeof(int[MAX + 1]));
}

void _deserialize_row(void *source, struct BTreeNode *destination) {
    memcpy(&(destination->offset), source, 4);
    memcpy(&(destination->parent), source + 4, 4);
    memcpy(&(destination->is_leaf_node), source + 8, 4);
    memcpy(&(destination->key_count), source + 12, 4);
    /* memcpy(&(destination->keys), source + 12, sizeof(int[MAX])); */
    void *src = source + 16;
    int t;
    for (t = 0; t < destination->key_count; t++) {
        /* if (*(int *)src != 0) { */
        /* if (destination->keys[t] != NULL) { */
        destination->keys[t] = malloc(sizeof(struct Row));

        memcpy(&(destination->keys[t]->id), src, 4);
        memcpy(&(destination->keys[t]->name), src + 4, 32);
        /* } */

        src += 36;
    }
    for (int t = destination->key_count + 1; t < MAX; t++)
        src += 36;

    source = src;

    memcpy(&(destination->children), source, sizeof(int[MAX + 1]));
}

int get_next_block_offset() {
    return (*number_of_nodes(page) * sizeof(struct BTreeNode)) + 8;
}

void write_page() {
    int file_descriptor = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    lseek(file_descriptor, 0, SEEK_SET);
    int bytes_written = write(file_descriptor, page, 1000);
    close(file_descriptor);
}

struct BTreeNode *get_node_from_offset(int offset) {
    struct BTreeNode *node = malloc(sizeof(struct BTreeNode));
    void *offset_address = malloc(1000);

    int file_descriptor = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    lseek(file_descriptor, offset, SEEK_SET);
    int bytes_read = read(file_descriptor, offset_address, 1000);

    _deserialize_row(offset_address, node);

    /* memcpy(&(node->name), offset_address, 32); */
    /* memcpy(&(node->offset), offset_address + 32, 4); */
    /* memcpy(&(node->is_leaf), offset_address + 36, 4); */
    /* memcpy(&(node->left), offset_address + 40, 4); */
    /* memcpy(&(node->right), offset_address + 44, 4); */

    close(file_descriptor);
    return node;
}

struct BTreeNode *new_btree_node(struct Row *key, int is_leaf_node) {
    struct BTreeNode *node =
        (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
    node->is_leaf_node = is_leaf_node;
    node->parent = NIL;
    node->offset = get_next_block_offset();
    node->keys[0] = key;
    for (int t = 0; t <= MAX; t++) {
        node->children[t] = NIL;
        if (t > 0)
            node->keys[t] = NULL;
    }
    node->key_count = 1;
    return node;
}

int get_insert_position(struct BTreeNode *root, int key) {
    for (int t = 0; t < MAX; t++) {
        if (root->keys[t] == NULL || root->keys[t]->id > key) {
            return t;
        }
    }
    return -1;
}

struct BTreeNode *push_to_parent_node(struct BTreeNode *root,
                                      struct BTreeNode *left_child,
                                      struct BTreeNode *right_child,
                                      struct Row *key) {
    struct BTreeNode *parent = malloc(sizeof(struct BTreeNode));
    _deserialize_row(page + root->parent, parent);
    /* printf("key = %d\n", key->id); */

    printf("parent offset = %d\n", parent->offset);
    int key_position = get_insert_position(parent, key->id);
    printf("position = %d\n", key_position);
    /* for (int t=0;t<MAX;t++) { */
    /* if (parent->keys[t] == NULL) { */
    /* parent->keys[t] = malloc(sizeof(struct Row)); */
    /* } */
    /* } */
    for (int t = MAX - 1; t > key_position; t--) {
        /* printf("in loop"); */
        // parent->keys[t] = parent->keys[t - 1];
        parent->keys[t] = parent->keys[t - 1];
        parent->children[t] = parent->children[t - 1];
    }

    parent->keys[key_position] = key;
    parent->key_count += 1;

    parent->children[key_position] = left_child->offset;
    parent->children[key_position + 1] = right_child->offset;
    parent->is_leaf_node = 0;
    /* parent->offset = root->offset; */

    /* insert(parent, key); */
    /* root->offset = parent->offset; */

    /* printf("leftt = %d\n", left_child->offset); */
    /* printf("rightt = %d\n", right_child->offset); */
    /* for (int t=0;t<MAX;t++) printf("%d ", parent->keys[t]->id); */

    printf("pushing to parent node\n");
    return parent;
}

struct BTreeNode *split_node(struct BTreeNode *root) {
    if (!root)
        return NULL;
    struct BTreeNode *left, *right;
    int median = MAX / 2;

    left = new_btree_node(root->keys[0], root->is_leaf_node);
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
    right = new_btree_node(root->keys[median + 1], root->is_leaf_node);
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

    left->offset = get_next_block_offset();
    *number_of_nodes(page) += 1;

    right->offset = get_next_block_offset();
    *number_of_nodes(page) += 1;

    for (int t = 0; t < MAX; t++) {
        if (left->children[t]) {
            struct BTreeNode *left_node_child =
                get_node_from_offset(left->children[t]);
            _deserialize_row(page + left->children[t], left_node_child);
            printf("left->children[t] = %d, left child = %d\n",
                   left->children[t], left_node_child->offset);

            left_node_child->parent = left->offset;
            _serialize_row(page + left->children[t], left_node_child);
        }
        if (right->children[t]) {
            struct BTreeNode *right_node_child =
                get_node_from_offset(right->children[t]);
            /* _deserialize_row(page + right->children[t], right_node_child); */
            printf("right->children[t] = %d, right child = %d\n",
                   right->children[t], right_node_child->offset);

            right_node_child->parent = right->offset;
            _serialize_row(page + right->children[t], right_node_child);
        }
    }
    printf("root off = %d\n", root->offset);

    if (root->parent != NIL) {
        printf("before root = %d\n", root->offset);
        root = push_to_parent_node(root, left, right, root->keys[median]);
        /* printf("after done\n"); */
        printf("after root = %d\n", root->offset);
        right->parent = root->offset;
        left->parent = root->offset;
    } else {
        root->is_leaf_node = 0;
        root->keys[0] = root->keys[median];
        root->key_count = 1;
        for (int t = 1; t < MAX; t++) {
            root->keys[t] = NULL;
        }
        for (int t = 0; t < MAX + 1; t++)
            root->children[t] = NIL;

        root->children[0] = left->offset;
        root->children[1] = right->offset;
        root->parent = NIL;

        right->parent = root->offset;
        left->parent = root->offset;

        *root_node_offset(page) = root->offset;
    }

    /* printf("root = %d\n", root->keys[0]); */
    /* printf("left = %d\n", left->keys[0]); */
    /* printf("right = %d\n", right->keys[0]); */
    _serialize_row(page + root->offset, root);
    _serialize_row(page + left->offset, left);
    _serialize_row(page + right->offset, right);

    return root;
}
void insert(struct BTreeNode *root, struct Row *key) {

    int position = get_insert_position(root, key->id);
    /* printf("got position = %d\n", position); */
    if (position == -1) {
        printf("got -1 position for %d\n", key->id);
        exit(1);
    }
    if (root->keys[position] != NULL) {
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
            /* lseek(fd, 0, SEEK_SET); */
            /* int bytes_read = read(fd, page, 1000); */
            root = split_node(root);
            write_page();
        }
        /* printf("done11"); */
    } else {
        *number_of_nodes(page) += 1;
        _serialize_row(page + root->offset, root);
    }
    write_page();
}

void find_and_insert_node(int root_offset, struct Row *key) {
    if (root_offset == NIL)
        return;

    struct BTreeNode *root = malloc(sizeof(struct BTreeNode));
    _deserialize_row(page + root_offset, root);
    printf("got root %d\n", root->keys[0]->id);

    if (root->is_leaf_node) {
        /* printf("inserting %d\n", root->keys[0]->id); */
        insert(root, key);
        return;
    }

    for (int t = 0; t < MAX; t++) {
        if (root->keys[t] == NULL)
            return find_and_insert_node(root->children[t], key);

        if (root->keys[t]->id > key->id) {
            return find_and_insert_node(root->children[t], key);
        }
    }

    return find_and_insert_node(root->children[root->key_count], key);
}

void print(int offset) {
    if (offset == NIL)
        return;

    struct BTreeNode *root = malloc(sizeof(struct BTreeNode));
    _deserialize_row(page + offset, root);

    for (int t = 0; t < root->key_count; t++) {
        printf("%d: %s", root->keys[t]->id, root->keys[t]->name);
    }
    printf("\n");

    for (int t = 0; t < MAX + 1; t++) {
        if (root->children[t] != NIL) {
            print(root->children[t]);
        }
    }
}

static int ct;
struct Row *create_key() {
    struct Row *r = malloc(sizeof(struct Row));
    r->id = ct++;
    memcpy(&(r->name), "mano", 4);
    return r;
}

int main() {
    page = malloc(4000);

    struct Row *r = malloc(sizeof(struct Row));
    scanf("%d", &r->id);
    scanf("%s", r->name);
    struct BTreeNode *root = new_btree_node(r, 1);

    fd = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    lseek(fd, 0, SEEK_SET);
    int bytes_read = read(fd, page, 1000);

    /* *number_of_nodes(page) += 1; */
    printf("no = %d\n", *number_of_nodes(page));

    int cell_count = *number_of_nodes(page);
    if (cell_count == 0) {
        /* root->keys[0] = r; */
        *root_node_offset(page) = 8;

        _serialize_row(page + 8, root);
        *number_of_nodes(page) = 1;

        write_page();
    } else {
        /* find_and_insert_node(4, r); */
        /* find_and_insert_node(4, create_key()); */
        /* find_and_insert_node(4, 14); */ /* find_and_insert_node(4, 15); */
        find_and_insert_node(8, r);
        /* find_and_insert_node(4, 20); */
        /* find_and_insert_node(4, 22); */
        /* find_and_insert_node(4, 25); */
        /* find_and_insert_node(4, 17); */
    }
    /* printf("sz = %lu\n", sizeof(struct BTreeNode)); */

    /* find_and_insert_node(root, 1123); */
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
    print(8);

    return 0;
}
