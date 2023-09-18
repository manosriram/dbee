#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
#define MAX_PAGES 100
#define BTREE_MAX_KEY_SIZE 3
#define NIL '\0'

#define QUERY_TYPE_INSERT "insert"
#define QUERY_TYPE_SELECT "select"
#define QUERY_TYPE_EXIT ".exit"
#define QUERY_TYPE_INFO ".info"

enum QUERY_TYPE {
    QUERY_INSERT,
    QUERY_SELECT,
    QUERY_EXIT,
    QUERY_INFO,
};

typedef struct {
    /* char *name; */
    int id;
    char name[32];
} Row;

typedef struct {
    uint32_t file_length;
    void *pages[MAX_PAGES];
    int file_descriptor;
    int num_pages;
    uint32_t available_bytes;
} Pager;

typedef struct {
    Pager *pager;
    uint32_t no_of_rows;
} Table;

typedef struct {
    int cell_no;
    int page_no;
    int end_of_table;
    Table *table;
} Cursor;

typedef struct {
    enum QUERY_TYPE statement_type;
    Table *table;
    Row *row;
} Statement;

typedef struct {
    Row *keys[BTREE_MAX_KEY_SIZE];
    int children[BTREE_MAX_KEY_SIZE + 1];
    int parent;
    int offset;
    /* struct BTreeNode *children[MAX + 1]; */
    /* struct BTreeNode *parent; */
    int is_leaf_node, key_count;
} BTreeNode;

const uint32_t PAGE_SIZE = 2000;
/* const uint32_t NAME_SIZE = 100; */

const uint32_t PAGE_HEADER_NUM_NODES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_NUM_NODES_OFFSET = 0;
const uint32_t PAGE_HEADER_FREE_BYTES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_FREE_BYTES_OFFSET = PAGE_HEADER_NUM_NODES_SIZE;
const uint32_t PAGE_HEADER_NUM_PAGES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_NUM_PAGES_OFFSET =
    PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE;
const uint32_t PAGE_HEADER_ROOT_NODE_OFFSET = PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE;
const uint32_t PAGE_HEADER_ROOT_NODE_SIZE = sizeof(uint32_t);

const uint32_t HEADER_SIZE =
    PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE + PAGE_HEADER_NUM_PAGES_SIZE + PAGE_HEADER_ROOT_NODE_SIZE;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t ID_OFFSET = 0;
const uint32_t NAME_SIZE = size_of_attribute(Row, name);
const uint32_t NAME_OFFSET = ID_OFFSET + ID_SIZE;

const uint32_t BTREE_NODE_SIZE = sizeof(BTreeNode);

const uint32_t ROW_SIZE = NAME_SIZE + ID_SIZE;
const uint32_t MAX_QUERY_TYPE_SIZE = 8;
const uint32_t MAX_NODES_IN_A_PAGE = (PAGE_SIZE - HEADER_SIZE) / BTREE_NODE_SIZE;

// BTree constants
const uint32_t NODE_OFFSET_SIZE = sizeof(int);
const uint32_t NODE_PARENT_SIZE = sizeof(int);
const uint32_t NODE_IS_LEAF_NODE_SIZE = sizeof(int);
const uint32_t NODE_KEYS_SIZE = BTREE_MAX_KEY_SIZE * sizeof(Row);
const uint32_t NODE_CHILDREN_SIZE = sizeof(int[BTREE_MAX_KEY_SIZE + 1]);
const uint32_t NODE_KEY_COUNT_SIZE = sizeof(int);

const uint32_t NODE_OFFSET_OFFSET = 0;
const uint32_t NODE_PARENT_OFFSET = NODE_OFFSET_SIZE;
const uint32_t NODE_IS_LEAF_NODE_OFFSET = NODE_PARENT_OFFSET + NODE_PARENT_SIZE;
const uint32_t NODE_KEYS_OFFSET =
    NODE_IS_LEAF_NODE_OFFSET + NODE_IS_LEAF_NODE_SIZE;
const uint32_t NODE_CHILDREN_OFFSET = NODE_KEYS_OFFSET + NODE_KEYS_SIZE;
const uint32_t NODE_KEY_COUNT_OFFSET =
    NODE_CHILDREN_OFFSET + NODE_CHILDREN_SIZE;

uint32_t *page_header_root_node_offset_value(void *node) {
    return (uint32_t *)node + PAGE_HEADER_ROOT_NODE_OFFSET;
}

uint32_t *page_header_num_nodes_value(void *node) {
    return (uint32_t *)node + PAGE_HEADER_NUM_NODES_OFFSET;
}

uint32_t *page_header_free_bytes_value(void *node) {
    return (uint32_t *)node + PAGE_HEADER_FREE_BYTES_OFFSET;
}

uint32_t *page_header_num_pages_value(void *node) {
    return (uint32_t *)node + PAGE_HEADER_NUM_PAGES_OFFSET;
}

void *get_cell_address(void *node, uint32_t cell_no) {
    /* return node + PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE +
     * ((cell_no + 1)* ROW_SIZE); */
    return node + HEADER_SIZE + ((cell_no + 1) * BTREE_NODE_SIZE);
}

void serialize_row(void *destination, Row *row) {
    memcpy(destination + ID_OFFSET, &(row->id), ID_SIZE);
    memcpy(destination + NAME_OFFSET, &(row->name), NAME_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->name), source + NAME_OFFSET, NAME_SIZE);
}

void _serialize_row(void *destination, BTreeNode *row) {
		printf("NODE_OFFSET_OFFSET = %d, NODE_OFFSET_SIZE = %d\n", NODE_OFFSET_OFFSET, NODE_OFFSET_SIZE);
		printf("NODE_PARENT_OFFSET = %d, NODE_PARENT_SIZE = %d\n", NODE_PARENT_OFFSET, NODE_PARENT_SIZE);
		printf("NODE_IS_LEAF_NODE_OFFSET = %d, NODE_IS_LEAF_NODE_SIZE = %d\n", NODE_IS_LEAF_NODE_OFFSET, NODE_IS_LEAF_NODE_SIZE);
		printf("NODE_KEYS_OFFSET = %d, NODE_KEYS_SIZE = %d\n", NODE_KEYS_OFFSET, NODE_KEYS_SIZE);
		printf("NODE_CHILDREN_OFFSET = %d, NODE_CHILDREN_SIZE = %d\n", NODE_CHILDREN_OFFSET, NODE_CHILDREN_SIZE);
		printf("NODE_KEY_COUNT_OFFSET = %d, NODE_KEY_COUNT_SIZE = %d\n", NODE_KEY_COUNT_OFFSET, NODE_KEY_COUNT_SIZE);

		/* destination += HEADER_SIZE; */

    memcpy(destination + NODE_OFFSET_OFFSET, &(row->offset), NODE_OFFSET_SIZE);
    memcpy(destination + NODE_PARENT_OFFSET, &(row->parent), NODE_PARENT_SIZE);
    memcpy(destination + NODE_IS_LEAF_NODE_OFFSET, &(row->is_leaf_node),
           NODE_IS_LEAF_NODE_SIZE);

		/* printf("sz = %lu\n", sizeof(Row)); */
		void *dest = destination + NODE_KEYS_OFFSET;
		for (int t=0;t<BTREE_MAX_KEY_SIZE;t++) {
				if (row->keys[t] != NULL) {
						dest += ((t+1) * sizeof(Row));
						serialize_row(dest, row->keys[t]);
				}
		}
    /* memcpy(destination + NODE_KEYS_OFFSET, &(row->keys), NODE_KEYS_SIZE); */

    memcpy(destination + NODE_CHILDREN_OFFSET, &(row->children),
           NODE_CHILDREN_SIZE);
    memcpy(destination + NODE_KEY_COUNT_OFFSET, &(row->key_count),
           NODE_KEY_COUNT_SIZE);
}

void _deserialize_row(void *source, BTreeNode *destination) {

		/* source += HEADER_SIZE; */

    memcpy(&(destination->offset), source + NODE_OFFSET_OFFSET,
           NODE_OFFSET_SIZE);
    memcpy(&(destination->parent), source + NODE_PARENT_OFFSET,
           NODE_PARENT_SIZE);
    memcpy(&(destination->is_leaf_node), source + NODE_IS_LEAF_NODE_OFFSET,
           NODE_IS_LEAF_NODE_SIZE);
    /* memcpy(&(destination->keys), source + NODE_KEYS_OFFSET, NODE_KEYS_SIZE); */
		void *dest = source + NODE_KEYS_OFFSET;
		for (int t=0;t<BTREE_MAX_KEY_SIZE;t++) {
				/* void *dest = source + ((t) * sizeof(Row)); */
				/* void *dest = source + (NODE_OFFSET_SIZE + NODE_PARENT_SIZE + NODE_IS_LEAF_NODE_SIZE) + ((t) * sizeof(Row)); */
				dest += ((t+1) * sizeof(Row));
				destination->keys[t] = malloc(sizeof(Row[0]));
				deserialize_row(dest, destination->keys[t]);
		}
    memcpy(&(destination->children), source + NODE_CHILDREN_OFFSET,
           NODE_CHILDREN_SIZE);
    memcpy(&(destination->key_count), source + NODE_KEY_COUNT_OFFSET,
           NODE_KEY_COUNT_SIZE);
}

BTreeNode *new_btree_node(Row *key, uint32_t node_offset, uint32_t is_leaf_node) {
    BTreeNode *node = (BTreeNode *)malloc(sizeof(BTreeNode));
    for (int t = 0; t <= BTREE_MAX_KEY_SIZE; t++) {
        node->children[t] = NIL;
    }
		node->keys[0] = malloc(sizeof(Row));

    node->is_leaf_node = is_leaf_node;
    node->parent = NIL;
    node->keys[0] = key;
    node->offset = node_offset;
    node->key_count = 1;
    return node;
}

void *get_page(Pager *pager, int page_no) {
    if (page_no > MAX_PAGES) {
        printf("max pages used: %d\n", page_no);
        // TODO: write and exit.
        return NULL;
    }

    if (pager->pages[page_no] == NULL) {
        // get to the nth page address
        // allocate PAGE_SIZE and get starting address of the page
        //
        int no_of_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            no_of_pages++;
        }

        void *page = malloc(PAGE_SIZE);
        *page_header_free_bytes_value(page) = PAGE_SIZE - HEADER_SIZE;
        *page_header_num_nodes_value(page) = 0;

        int page_bytes = page_no * PAGE_SIZE;
        if (page_no <= no_of_pages) {
            lseek(pager->file_descriptor, page_bytes, SEEK_SET);
            int bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("error reading file\n");
                exit(1);
            }
        }

        pager->pages[page_no] = page;
        if (page_no > pager->num_pages) {
            pager->num_pages = page_no + 1;
        }
    }
    return pager->pages[page_no];
}

void *get_offset_address(int offset) {
    /* void *offset_address = malloc(BTREE_NODE_SIZE); */

    /* int file_descriptor = open("tree", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR); */
    /* lseek(file_descriptor, offset, SEEK_SET); */
    /* int bytes_read = read(file_descriptor, offset_address, BTREE_NODE_SIZE); */
    /* return offset_address; */
		return NULL;
}

int get_insert_position(BTreeNode *root, Row *key) {
    for (int t = 0; t < BTREE_MAX_KEY_SIZE; t++) {
        if (root->keys[t] == NULL || root->keys[t]->id > key->id) {
            return t;
        }
    }
    return -1;
}

BTreeNode *get_node_from_offset(void *page, int offset) {
		BTreeNode *root = malloc(sizeof(BTreeNode));
		void *src = page + offset;
		_deserialize_row(src, root);

		return root;
}

uint32_t get_current_offset(void *page) {
		return HEADER_SIZE + (*page_header_num_nodes_value(page) * BTREE_NODE_SIZE);
}

BTreeNode *push_to_parent_node(void *page, BTreeNode *root, BTreeNode *left_child,
                               BTreeNode *right_child, Row *key) {
    BTreeNode *parent = get_node_from_offset(page, root->parent);
    int key_position = get_insert_position(parent, key);
    for (int t = BTREE_MAX_KEY_SIZE - 1; t > key_position; t--) {
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

Cursor *cursor_at_table_end(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;

    void *page = get_page(table->pager, 0);
    int no_of_cells = *page_header_num_nodes_value(page);

    cursor->cell_no = no_of_cells;
    cursor->page_no = *page_header_num_pages_value(page);
    return cursor;
}

BTreeNode *split_node(Table *table, BTreeNode *root) {
    if (!root)
        return NULL;
    BTreeNode *left, *right;
    int median = BTREE_MAX_KEY_SIZE / 2;
		uint32_t space_available;
    Cursor *cursor = cursor_at_table_end(table);
		/* printf("cur -> %d, %d\n", cursor->cell_no, cursor->page_no); */

    Pager *pager = table->pager;
		void *page;
		
		page = get_page(pager, cursor->page_no);
    space_available = *page_header_free_bytes_value(page);
		if (space_available < BTREE_NODE_SIZE) {
        pager->num_pages += 1;
        page = get_page(pager, pager->num_pages);
        *page_header_num_pages_value(get_page(pager, 0)) = pager->num_pages;
        printf("created new page %d\n", cursor->page_no);
		}

		int left_offset = get_current_offset(page);
    left =
        new_btree_node(root->keys[0], left_offset, root->is_leaf_node);
    for (int t = 1; t < median; t++) {
        left->keys[t] = root->keys[t];
        left->key_count++;
    }

    int MAX_CHILDREN = BTREE_MAX_KEY_SIZE + 1;

    for (int t = 0; t < MAX_CHILDREN / 2; t++) {
        if (root->children[t] != NIL) {
            left->children[t] = root->children[t];
        }
    }

		// new page iff space_available < BTREE_NODE_SIZE
		*page_header_free_bytes_value(page) -= BTREE_NODE_SIZE;
		*page_header_num_nodes_value(page) += 1;

		int right_offset = get_current_offset(page);
    right = new_btree_node(root->keys[median + 1], right_offset,
                           root->is_leaf_node);
    int right_pos = 1;
    for (int t = median + 2; t < BTREE_MAX_KEY_SIZE; t++) {
        right->keys[right_pos] = root->keys[t];
        right->key_count++;
        right_pos++;
    }

    right_pos = 0;
    for (int t = median + 1; t <= BTREE_MAX_KEY_SIZE; t++) {
        if (root->children[right_pos] != NIL) {
            right->children[right_pos] = root->children[t];
            right_pos++;
        }
    }

		*page_header_free_bytes_value(page) -= BTREE_NODE_SIZE;
		*page_header_num_nodes_value(page) += 1;

    for (int t = 0; t < BTREE_MAX_KEY_SIZE; t++) {
        if (left->children[t])
            left->children[t] = left->offset;
        if (right->children[t])
            right->children[t] = right->offset;
    }

    if (root->parent != NIL) {
        root = push_to_parent_node(page, root, left, right, root->keys[median]);
        right->parent = root->offset;
        left->parent = root->offset;

				_serialize_row(page + root->offset, root);
    } else {
        root->is_leaf_node = 0;
        root->keys[0] = root->keys[median];
        root->key_count = 1;
        for (int t = 1; t < BTREE_MAX_KEY_SIZE; t++) {
            root->keys[t] = NULL;
        }
        for (int t = 0; t < BTREE_MAX_KEY_SIZE + 1; t++)
            root->children[t] = NIL;

        root->children[0] = left->offset;
        root->children[1] = right->offset;
        root->parent = NIL;

        right->parent = root->offset;
        left->parent = root->offset;

				*page_header_num_nodes_value(page) += 1;
				_serialize_row(page + root->offset, root);
    }

		void *left_destination = page + left_offset;
		_serialize_row(left_destination, left);

		void *right_destination = page + right_offset;
		_serialize_row(right_destination, right);

    return root;
}

void insert(Table *table, BTreeNode *root, Row *key) {
    int position = get_insert_position(root, key);
    if (position == -1) {
        printf("got -1 position for %d\n", key->id);
        exit(1);
    }
    if (root->keys[position] != NULL) {
        for (int t = BTREE_MAX_KEY_SIZE - 1; t > position; t--) {
            root->keys[t] = root->keys[t - 1];
            root->children[t] = root->children[t - 1];
        }
        root->keys[position] = key;
        root->key_count += 1;
    } else {
        root->keys[position] = key;
        root->key_count += 1;
    }

    /* void *destination = get_offset_address(get_current_offset(page)); */
    /* _serialize_row(destination, root); */
    /* num_nodes += 1; */

    if (root->key_count == BTREE_MAX_KEY_SIZE) {
        while (root->key_count == BTREE_MAX_KEY_SIZE) {
            root = split_node(table, root);
        }
    } else {
				_serialize_row(get_page(table->pager, table->pager->num_pages), root);
		}
}

BTreeNode *get_root_node(void *page) {
		return NULL;
}


void find_and_insert_node(void *page, int root_offset, Row *key) {
		if (root_offset < 0) return;

		BTreeNode *root = malloc(sizeof(BTreeNode));
		void *src = page + PAGE_HEADER_ROOT_NODE_OFFSET;
		_deserialize_row(src, root);
    /* BTreeNode *root = get_node_from_offset(page, root_offset); */
		printf("got root = %d, is_leaf = %d, key = %d\n", root->offset, root->is_leaf_node, root->keys[0]->id);

    if (root->is_leaf_node) {
        insert(page, root, key);
        return;
    }

    for (int t = 0; t < BTREE_MAX_KEY_SIZE; t++) {
        if (root->keys[t] == NULL)
            return find_and_insert_node(page, root->children[t], key);

        if (root->keys[t]->id > key->id) {
            return find_and_insert_node(page, root->children[t], key);
        }
    }

    return find_and_insert_node(page, root->children[root->key_count], key);
}


Pager *open_pager() {
    Pager *pager = malloc(sizeof(Pager));

    int file_descriptor = open("source", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    int file_length = lseek(file_descriptor, 0, SEEK_END);

    for (int t = 0; t < MAX_PAGES; t++) {
        pager->pages[t] = NULL;
    }

    pager->file_descriptor = file_descriptor;
    pager->file_length = file_length;

    void *header_page = get_page(pager, 0);
    pager->num_pages = *page_header_num_pages_value(header_page);
    printf("pager num pages = %d\n", pager->num_pages);

    return pager;
}

Table *open_table() {
    Table *new_table = malloc(sizeof(Table));
    new_table->pager = open_pager();
    new_table->no_of_rows = new_table->pager->file_length / BTREE_NODE_SIZE;

    if (new_table->pager->file_length == 0) {
        new_table->pager->pages[0] = get_page(new_table->pager, 0);
        *page_header_num_nodes_value(new_table->pager->pages[0]) = 0;
        *page_header_free_bytes_value(new_table->pager->pages[0]) =
            PAGE_SIZE - BTREE_NODE_SIZE;
        *page_header_num_pages_value(new_table->pager->pages[0]) = 0;
    }
    /* printf("free bytes = %d\n",
     * *page_header_free_bytes_value(new_table->pager->pages[0])); */

    return new_table;
}

int flush_page_to_disk(Pager *pager, int page_no) {
    void *page = get_page(pager, page_no);
    lseek(pager->file_descriptor, page_no * PAGE_SIZE, SEEK_SET);
    int bytes_written = write(pager->file_descriptor, page, PAGE_SIZE);

    return bytes_written;
}

void write_to_disk(Table *table) {
    Pager *pager = table->pager;
    int total_bytes_written_to_disk = 0;
    for (int t = 0; t < MAX_PAGES; t++) {
        if (pager->pages[t] == NULL) {
            continue;
        }

        total_bytes_written_to_disk += flush_page_to_disk(pager, t);
    }
    printf("written %d bytes to disk\n", total_bytes_written_to_disk);
}


void insert_query(Table *table, Row *row) {
    Cursor *cursor = cursor_at_table_end(table);
		/* printf("cur -> %d, %d\n", cursor->cell_no, cursor->page_no); */

    Pager *pager = table->pager;
    void *page;
    uint32_t space_available;

    page = get_page(pager, cursor->page_no);
    space_available = *page_header_free_bytes_value(page);
    if (space_available < BTREE_NODE_SIZE) {
        pager->num_pages += 1;
        page = get_page(pager, pager->num_pages);
        *page_header_num_pages_value(get_page(pager, 0)) = pager->num_pages;
        printf("created new page %d\n", cursor->page_no);
    }

    uint32_t no_of_cells = *page_header_num_nodes_value(get_page(pager, 0));
    uint32_t cell_to_be_inserted = no_of_cells;
    space_available = *page_header_free_bytes_value(page);

		/* void *destination = get_cell_address(page, cell_to_be_inserted); */
		if (no_of_cells == 0) {
				/* uint32_t offset = get_current_offset(page); */
				/* printf("inserted at %d\n", offset); */
				BTreeNode *root = new_btree_node(row, 16, 1);
				/* *page_header_root_node_offset_value(get_page(pager, 0)) = 16; */
				void *dest = page + PAGE_HEADER_ROOT_NODE_OFFSET;
				_serialize_row(dest, root);

		} else {
				printf("hit\n");

				BTreeNode *root = malloc(sizeof(BTreeNode));
				void *src = page + PAGE_HEADER_ROOT_NODE_OFFSET;
				_deserialize_row(src, root);

				/* printf("offset = %d, is_leaf = %d, parent = %d, root_key_count = %d, key_name = %s\n", root->offset, root->is_leaf_node, root->parent, root->key_count, root->keys[0]->name); */

				find_and_insert_node(page, 16, row);
		}

		*page_header_num_nodes_value(page) = no_of_cells + 1;
    *page_header_free_bytes_value(page) = space_available - BTREE_NODE_SIZE;

    /* table->no_of_rows += 1; */
		/* printf("cells = %d\n", *page_header_num_nodes_value(get_page(pager, 0))); */
    write_to_disk(table);
}

Cursor *cursor_at_table_start(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));

    cursor->table = table;
    cursor->page_no = 0;
    cursor->cell_no = 0;

    void *page = get_page(table->pager, 0);
    int no_of_cells = *page_header_num_nodes_value(page);
    printf("page count = %d\n", *page_header_num_pages_value(page));

    cursor->end_of_table = (no_of_cells == 0);

    return cursor;
}

void cursor_next(Cursor *cursor) {
    cursor->cell_no += 1;

    if (cursor->cell_no == MAX_NODES_IN_A_PAGE ||
        cursor->page_no ==
            *page_header_num_pages_value(get_page(cursor->table->pager, 0))) {
        cursor->end_of_table = 1;
        return;
    }

    if (cursor->cell_no % MAX_NODES_IN_A_PAGE == 0) {
        cursor->cell_no = 0;
        cursor->page_no += 1;
        return;
    }
}

void *cursor_value(Cursor *cursor) {
    void *page = get_page(cursor->table->pager, cursor->page_no);
    return get_cell_address(page, cursor->cell_no);
}

void select_query(Table *table) {
    Cursor *cursor = cursor_at_table_start(table);
    Pager *pager = table->pager;
		/* printf("MAX = %d", MAX_NODES_IN_A_PAGE); */

    uint32_t no_of_pages = *page_header_num_pages_value(get_page(pager, 0));
		printf("pages = %d, end = %d\n", no_of_pages, cursor->end_of_table);

		BTreeNode *root = get_root_node(get_page(pager, 0));
    while (!cursor->end_of_table) {
        /* void *page = get_page(pager, cursor->page_no); */
        /* uint32_t no_of_cells = *page_header_num_nodes_value(page); */

        /* [> Row *row = malloc(sizeof(Row)); <] */
				/* [> BTreeNode *r = malloc(sizeof(BTreeNode)); <] */

				/* printf("cursor = %d\n", cursor->cell_no); */
        /* void *cell_address = */
            /* get_cell_address(page, cursor->cell_no); */

				/* BTreeNode *r = malloc(sizeof(BTreeNode)); */

				/* int src = *page_header_root_node_offset_value(get_page(pager, 0)); */
				/* BTreeNode *rr = get_node_from_offset(src); */

				/* [> _deserialize_row(, r); <] */
				/* [> deserialize_row(cell_address + NODE_KEYS_OFFSET, row); <] */

				/* printf("id = %d, name = %s\n", rr->keys[0]->id, rr->keys[0]->name); */

				/* for (int t=0;t<BTREE_MAX_KEY_SIZE;t++) { */
						/* if (r->keys[t] != NULL) */
								/* printf("%s ", r->keys[t]->name); */
				/* } */
        /* printf("page = %d, cell_no = %d, addr = %p, ID = %d, NAME = %s\n", */
               /* cursor->page_no, cursor->cell_no, cell_address, row->id, */
               /* row->name); */

				/* break; */
				cursor_next(cursor);
    }
}

int are_strings_equal(char *string_one, char *string_two) {
    return strcmp(string_one, string_two) == 0;
}

void info_query(Table *table) {
    Pager *pager = table->pager;
    void *header_page = get_page(pager, 0);

    uint32_t no_of_pages = *page_header_num_pages_value(header_page);

    printf("page_count = %d\ntotal_row_count = %d\nrow_space_consumed = %d "
           "bytes\ntable_space_consumed = %d bytes\n",
           no_of_pages, table->no_of_rows, table->no_of_rows * BTREE_NODE_SIZE,
           (table->no_of_rows * BTREE_NODE_SIZE) + HEADER_SIZE);
}

void prepare_statement(Statement *statement, Table *table) {
    switch (statement->statement_type) {
    case QUERY_SELECT:
        select_query(table);
        break;
    case QUERY_INSERT:
        insert_query(statement->table, statement->row);
        break;
    case QUERY_EXIT:
        write_to_disk(table);
        exit(0);
    case QUERY_INFO:
        info_query(table);
        break;
    default:
        printf("unsupported command\n");
        exit(1);
    }
}

Row *make_row(int id, char *name) {
    Row *row = malloc(sizeof(Row));
    row->id = id;
    strcpy(row->name, name);
    return row;
}

int main() {
    printf("opening db\n");
    Table *table = open_table();

    Cursor *cursor = cursor_at_table_end(table);
    printf("%d - %d\n", cursor->cell_no, cursor->page_no);

    while (1) {
        char *query_type = malloc(MAX_QUERY_TYPE_SIZE);
        Statement *statement = malloc(sizeof(Statement));
        statement->table = table;
        printf("> ");

        scanf("%s", query_type);

        if (query_type[0] == '.') {
            if (are_strings_equal(query_type, QUERY_TYPE_EXIT)) {
                statement->statement_type = QUERY_EXIT;
            } else if (are_strings_equal(query_type, QUERY_TYPE_INFO)) {
                statement->statement_type = QUERY_INFO;
            }
        }

        if (are_strings_equal(query_type, QUERY_TYPE_SELECT)) {
            statement->statement_type = QUERY_SELECT;
        } else if (are_strings_equal(query_type, QUERY_TYPE_INSERT)) {
            char *name = malloc(NAME_SIZE);
            int id;
            scanf("%d %s", &id, name);
            statement->row = make_row(id, name);
            statement->statement_type = QUERY_INSERT;
        }
        prepare_statement(statement, table);
    }
    return 0;
}
