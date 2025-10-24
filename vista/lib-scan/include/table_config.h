#ifndef TABLE_CONFIG_H
#define TABLE_CONFIG_H

#include "pg_types.h"
#include "pg_structs.h"

#define MAX_TABLE_NAME 64
#define MAX_COLUMN_NAME 64
#define MAX_COLUMNS 32

/* Column definition from JSON config */
typedef struct {
    char name[MAX_COLUMN_NAME];
    int type_oid;
    int16_t length;
    bool nullable;
    char alignment;
    bool by_value;
} ConfigColumn;

/* Table configuration loaded from JSON */
typedef struct {
    char table_name[MAX_TABLE_NAME];
    int db_node;
    int rel_node;
    int start_segment;
    int end_segment;
    int num_columns;
    ConfigColumn columns[MAX_COLUMNS];
} TableConfig;

/* Function declarations */
#ifdef __cplusplus
extern "C" {
#endif

TupleDesc create_tuple_desc_from_config(const TableConfig *config);

#ifdef __cplusplus
}
#endif

#endif /* TABLE_CONFIG_H */