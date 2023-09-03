// Bugs
//
// If the data goes over the current page_size, it does not
// create a new page and write data in it.
//
// Also, the total space for the source file is being calculated wrongly.
// It is being calculated as MAX_PAGES * PAGE_SIZE instead of num_pages * PAGE_SIZE


#include <stdio.h>
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
#define MAX_PAGES 10

#define QUERY_TYPE_INSERT "insert"
#define QUERY_TYPE_SELECT "select"
#define QUERY_TYPE_EXIT ".exit"

enum QUERY_TYPE {
		QUERY_INSERT,
		QUERY_SELECT,
		QUERY_EXIT,
};

typedef struct {
		/* char *name; */
		char name[100];
}Row;

typedef struct {
		uint32_t file_length;
		void *pages[MAX_PAGES];
		int file_descriptor;
		int num_pages;
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
		Cursor *cursor;
		Row *row;
} Statement;


const uint32_t PAGE_SIZE = 50;
const uint32_t NODE_TYPE_SIZE = sizeof(uint32_t); // 0 = internal, 1 = leaf
const uint32_t HEADER_SIZE = NODE_TYPE_SIZE;
const uint32_t NAME_SIZE = size_of_attribute(Row, name);
/* const uint32_t NAME_SIZE = 100; */
const uint32_t NAME_OFFSET = HEADER_SIZE;
const uint32_t ROW_SIZE = NAME_SIZE;
const uint32_t NODE_SIZE = HEADER_SIZE + ROW_SIZE;

const uint32_t GLOBAL_HEADER_NUM_NODES_SIZE = sizeof(uint32_t);
const uint32_t GLOBAL_HEADER_NUM_NODES_OFFSET = 0;

const uint32_t MAX_QUERY_TYPE_SIZE = 8;

uint32_t *get_num_nodes_address(void *node) {
		return node + GLOBAL_HEADER_NUM_NODES_OFFSET;
}

uint32_t get_num_nodes_value(void *node) {
		return *((uint32_t *)node + GLOBAL_HEADER_NUM_NODES_OFFSET);
}

void set_num_nodes(void *node, int value) {
		*get_num_nodes_address(node) = value;
}

void *get_cell_address(void *node, uint32_t cell_no) {
		return node + GLOBAL_HEADER_NUM_NODES_SIZE + (cell_no * ROW_SIZE);
}

void *get_page(Pager *pager, int page_no) {
		if (page_no > MAX_PAGES) {
				printf("max pages used\n");
				// TODO: write and exit.
				return NULL;
		}

		if (pager->pages[page_no] == NULL) {
				// get to the nth page address
				// allocate PAGE_SIZE and get starting address of the page
				//
        int no_of_pages = pager->file_length / PAGE_SIZE;
				printf("no_of_pages = %d\n", no_of_pages);

				if (pager->file_length % PAGE_SIZE) {
						no_of_pages++;
				}

				void *page = malloc(PAGE_SIZE);
				int page_bytes = page_no * PAGE_SIZE;
				if (page_no <= no_of_pages) {
						lseek(pager->file_descriptor, page_bytes, SEEK_SET);
						int bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
						printf("page = %s\n", (char *)page);
						/* printf("read = %d\n", bytes_read); */
						if (bytes_read == -1) {
								printf("error reading file\n");
								exit(1);
						}
				}

				pager->pages[page_no] = page;
				if (page_no > pager->num_pages) {
						pager->num_pages = page_no + 1;
				}
				printf("got page\n");
		}

		return pager->pages[page_no];
}

Pager *open_pager() {
		Pager *pager = malloc(sizeof(Pager));

		int file_descriptor = open("source", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    int file_length = lseek(file_descriptor, 0, SEEK_END);
		printf("len = %d\n", file_length);

		for (int t=0;t<MAX_PAGES;t++) {
				pager->pages[t] = NULL;
		}

		pager->file_descriptor = file_descriptor;
		pager->file_length = file_length;
		pager->num_pages = 0;

		return pager;
}

Table *open_table() {
		Table *new_table = malloc(sizeof(Table));
		new_table->pager = open_pager();
		new_table->no_of_rows = new_table->pager->file_length / ROW_SIZE;

		if (new_table->no_of_rows == 0) {
				new_table->pager->pages[0] = get_page(new_table->pager, 0);
				set_num_nodes(new_table->pager->pages[0], 0);
		}

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
		for (int t=0;t<=MAX_PAGES;t++) {
				if (pager->pages[t] == NULL) {
						continue;
				}

				total_bytes_written_to_disk += flush_page_to_disk(pager, t);
		}
		printf("written %d bytes to disk\n", total_bytes_written_to_disk);
}

void serialize_row(void *destination, Row *row) {
		memcpy(destination, &(row->name), NAME_SIZE);
}

void deserialize_row(void *source, Row *destination) {
		memcpy(&(destination->name), source, NAME_SIZE);
}

void insert_query(Cursor *cursor, Row *row) {
		Table *table = cursor->table;

		/* move_cursor_to_insert_position(cursor); */

		/* uint32_t insert_offset = table->table_row_count * (HEADER_SIZE + ROW_SIZE); */

		Pager *pager = table->pager;
		void *page = pager->pages[cursor->page_no];
		uint32_t no_of_cells = get_num_nodes_value(page);
		uint32_t cell_to_be_inserted = no_of_cells;

		void *destination = get_cell_address(page, cell_to_be_inserted);
		printf("inserted row at %p\n", destination);

		serialize_row(destination, row);

		table->no_of_rows += 1;
		set_num_nodes(page, cell_to_be_inserted + 1);

		printf("cells = %d\n", get_num_nodes_value(page));
		write_to_disk(table);
}

Cursor *cursor_at_table_start(Table *table) {
		Cursor *cursor = malloc(sizeof(Cursor));
		/* void *first_cell_first_page_location = table->pager->pages[0] + NUM_NODES_SIZE; */

		cursor->table = table;
		cursor->page_no = 0;
		cursor->cell_no = 0;
		printf("%d", cursor->page_no);

		void *page = get_page(table->pager, 0);
		int no_of_cells = get_num_nodes_value(page);
		printf("no of cells = %d\n", no_of_cells);

		cursor->end_of_table = (no_of_cells == 0);

		return cursor;
}

void cursor_next(Cursor *cursor) {
		void *page = get_page(cursor->table->pager, cursor->page_no);
		int no_of_cells = get_num_nodes_value(page);

		cursor->cell_no += 1;
		if (cursor->cell_no == no_of_cells) {
				cursor->end_of_table = 1;
		}
}

void *cursor_value(Cursor *cursor) {
		void *page = get_page(cursor->table->pager, cursor->page_no);
		return get_cell_address(page, cursor->cell_no);
}

void select_query(Table *table) {
		Cursor *cursor = cursor_at_table_start(table);

		Pager *pager = table->pager;
		void *page = pager->pages[cursor->page_no];
		uint32_t no_of_cells = get_num_nodes_value(page);

		void *start = page + GLOBAL_HEADER_NUM_NODES_SIZE;
		while (!cursor->end_of_table) {
				Row *row = malloc(sizeof(Row));
				/* void *cell_address = cursor_value(cursor); */
				void *cell_address = get_cell_address(page, cursor->cell_no);

				deserialize_row(cell_address, row);
				printf("cell_no = %d, name = %s, addr = %p\n", cursor->cell_no, row->name, cell_address);

				cursor_next(cursor);
		}

		/* for (int t=0;t<no_of_cells;++t) { */
				/* Row *row = malloc(sizeof(Row)); */
				/* deserialize_row(start, row); */
				/* printf("name = %s\n", row->name); */

				/* start += NODE_SIZE; */
		/* } */

}

int are_strings_equal(char *string_one, char *string_two) {
		return strcmp(string_one, string_two) == 0;
}

void prepare_statement(Statement *statement, Table *table) {
		switch (statement->statement_type) {
				case QUERY_SELECT:
						select_query(table);
						break;
				case QUERY_INSERT:
						insert_query(statement->cursor, statement->row);
						break;
				case QUERY_EXIT:
						write_to_disk(table);
						exit(0);
				default:
						printf("unsupported command\n");
						exit(1);
		}
}

Row *make_row(char *name) {
		Row *row = malloc(sizeof(Row));
		/* row->name = malloc(NAME_SIZE); */
		strcpy(row->name, name);
		return row;
}

int main() {
		printf("opening db\n");
		Table *table = open_table();
		Cursor *cursor = cursor_at_table_start(table);

		while (1) {
				char *query_type = malloc(MAX_QUERY_TYPE_SIZE);
				Statement *statement = malloc(sizeof(Statement));
				statement->cursor = cursor;
				printf("> ");

				scanf("%s", query_type);

				if (query_type[0] == '.') {
						if (are_strings_equal(query_type, QUERY_TYPE_EXIT)) {
								statement->statement_type = QUERY_EXIT;
						}
				}

				if (are_strings_equal(query_type, QUERY_TYPE_SELECT)) {
						statement->statement_type = QUERY_SELECT;
				} else if (are_strings_equal(query_type, QUERY_TYPE_INSERT)) {
						char *name = malloc(NAME_SIZE);
						scanf("%s", name);
						statement->row = make_row(name);
						statement->statement_type = QUERY_INSERT;
				}

				prepare_statement(statement, table);
		}
		return 0;
}
