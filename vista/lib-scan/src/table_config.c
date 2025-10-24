#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "table_config.h"
#include "pg_macros.h"
#include "memory_utils.h"

/* Create TupleDesc from config */
TupleDesc create_tuple_desc_from_config(const TableConfig *config) {
    int natts = config->num_columns;
    Size size = offsetof(TupleDescData, attrs) + natts * sizeof(FormData_pg_attribute);
    TupleDesc tupdesc = (TupleDesc)simple_malloc(size);
    
    if (!tupdesc) {
        return NULL;
    }
    
    tupdesc->natts = natts;
    tupdesc->tdtypeid = 0;
    tupdesc->tdtypmod = -1;
    tupdesc->tdrefcount = 1;
    tupdesc->constr = NULL;
    
    /* Define attributes from config */
    for (int i = 0; i < natts; i++) {
        Form_pg_attribute attr = &tupdesc->attrs[i];
        const ConfigColumn *col = &config->columns[i];
        
        memset(attr, 0, sizeof(FormData_pg_attribute));
        
        attr->attrelid = config->rel_node;
        strncpy(attr->attname.data, col->name, NAMEDATALEN - 1);
        attr->atttypid = col->type_oid;
        attr->attlen = col->length;
        attr->attnum = i + 1;
        attr->attcacheoff = -1;
        attr->atttypmod = -1;
        attr->attndims = 0;
        attr->attbyval = col->by_value;
        attr->attalign = col->alignment;
        attr->attstorage = 'p';  // plain storage
        attr->attnotnull = !col->nullable;
        attr->atthasdef = false;
        attr->attisdropped = false;
        attr->attislocal = true;
        attr->attinhcount = 0;
    }
    
    return tupdesc;
}
