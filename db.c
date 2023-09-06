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
#define MAX_PAGES 100

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
}Row;

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


const uint32_t PAGE_SIZE = 100;
/* const uint32_t NAME_SIZE = 100; */

const uint32_t PAGE_HEADER_NUM_NODES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_NUM_NODES_OFFSET = 0;
const uint32_t PAGE_HEADER_FREE_BYTES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_FREE_BYTES_OFFSET = PAGE_HEADER_NUM_NODES_SIZE;
const uint32_t PAGE_HEADER_NUM_PAGES_SIZE = sizeof(uint32_t);
const uint32_t PAGE_HEADER_NUM_PAGES_OFFSET = PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE;

const uint32_t HEADER_SIZE = PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t ID_OFFSET = 0;
const uint32_t NAME_SIZE = size_of_attribute(Row, name);
const uint32_t NAME_OFFSET = ID_OFFSET + ID_SIZE;

const uint32_t ROW_SIZE = NAME_SIZE + ID_SIZE;
const uint32_t MAX_QUERY_TYPE_SIZE = 8;
const uint32_t MAX_NODES_IN_A_PAGE = (PAGE_SIZE - HEADER_SIZE) / ROW_SIZE;

uint32_t *get_num_nodes_address(void *node) {
		return node + PAGE_HEADER_NUM_NODES_OFFSET;
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
		/* return node + PAGE_HEADER_NUM_NODES_SIZE + PAGE_HEADER_FREE_BYTES_SIZE + ((cell_no + 1)* ROW_SIZE); */
		return node + HEADER_SIZE + ((cell_no + 1)* ROW_SIZE);
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

Pager *open_pager() {
		Pager *pager = malloc(sizeof(Pager));

		int file_descriptor = open("source", O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    int file_length = lseek(file_descriptor, 0, SEEK_END);

		for (int t=0;t<MAX_PAGES;t++) {
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
		new_table->no_of_rows = new_table->pager->file_length / ROW_SIZE;

		if (new_table->pager->file_length == 0) {
				new_table->pager->pages[0] = get_page(new_table->pager, 0);
				*page_header_num_nodes_value(new_table->pager->pages[0]) = 0;
				*page_header_free_bytes_value(new_table->pager->pages[0]) = PAGE_SIZE - ROW_SIZE;
				*page_header_num_pages_value(new_table->pager->pages[0]) = 0;
		}
		/* printf("free bytes = %d\n", *page_header_free_bytes_value(new_table->pager->pages[0])); */

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
		for (int t=0;t<MAX_PAGES;t++) {
				if (pager->pages[t] == NULL) {
						continue;
				}

				total_bytes_written_to_disk += flush_page_to_disk(pager, t);
		}
		printf("written %d bytes to disk\n", total_bytes_written_to_disk);
}
void serialize_row(void *destination, Row *row) {
		memcpy(destination + ID_OFFSET, &(row->id), ID_SIZE);
		memcpy(destination + NAME_OFFSET, &(row->name), NAME_SIZE);
}

void deserialize_row(void *source, Row *destination) {
		memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
		memcpy(&(destination->name), source + NAME_OFFSET, NAME_SIZE);
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

void insert_query(Table *table, Row *row) {
		Cursor *cursor = cursor_at_table_end(table);
		/* printf("cur -> %d, %d\n", cursor->cell_no, cursor->page_no); */

		Pager *pager = table->pager;
		void *page;
		uint32_t space_available;

		page = get_page(pager, cursor->page_no);
		space_available = *page_header_free_bytes_value(page);
		if (space_available < ROW_SIZE) {
				pager->num_pages += 1;
				page = get_page(pager, pager->num_pages);
				*page_header_num_pages_value(get_page(pager, 0)) = pager->num_pages;
				printf("created new page %d\n", cursor->page_no);
		}

		uint32_t no_of_cells = *page_header_num_nodes_value(page);
		uint32_t cell_to_be_inserted = no_of_cells;
		space_available = *page_header_free_bytes_value(page);

		void *destination = get_cell_address(page, cell_to_be_inserted);

		serialize_row(destination, row);

		/* printf("space available = %d, row_size = %d\n", space_available, ROW_SIZE); */
		*page_header_num_nodes_value(page) = no_of_cells + 1;
		*page_header_free_bytes_value(page) = space_available - ROW_SIZE;

		table->no_of_rows += 1;
		/* printf("cells = %d\n", *page_header_num_nodes_value(page)); */
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

		if (cursor->cell_no == MAX_NODES_IN_A_PAGE && cursor->page_no == *page_header_num_pages_value(get_page(cursor->table->pager, 0))) {
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

		uint32_t no_of_pages = *page_header_num_pages_value(get_page(pager, 0));

		while (!cursor->end_of_table) {
				void *page = get_page(pager, cursor->page_no);
				uint32_t no_of_cells = *page_header_num_nodes_value(page);

				Row *row = malloc(sizeof(Row));
				void *cell_address = get_cell_address(page, cursor->cell_no % MAX_NODES_IN_A_PAGE);
				deserialize_row(cell_address, row);
				printf("page = %d, cell_no = %d, addr = %p, ID = %d, NAME = %s\n", cursor->page_no, cursor->cell_no, cell_address, row->id, row->name);

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

		printf("page_count = %d\ntotal_row_count = %d\nrow_space_consumed = %d bytes\ntable_space_consumed = %d bytes\n", no_of_pages, table->no_of_rows, table->no_of_rows * ROW_SIZE, (table->no_of_rows * ROW_SIZE) + HEADER_SIZE);
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
