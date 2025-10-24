#include "postgres.h"

#include "libvista.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "catalog/pg_class.h"
#include "storage/vista_internal_bitmap.h"
#include "storage/buf_internals.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(vista_add_all_tables_to_bitmap);

/*
 * UDF to iterate through all user tables in the current database and
 * add them to the Vista bitmap manager. This is useful for pre-populating
 * the manager for tables that have not yet been modified.
 */
Datum
vista_add_all_tables_to_bitmap(PG_FUNCTION_ARGS)
{
	int ret;
	int i;
	int processed_count = 0;
	/* This query selects the OIDs of all regular tables ('r') in user-defined namespaces. */
	char *query = "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%';";

	if (vista_BitmapMgr == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Vista bitmap manager is not initialized")));
	}

	SPI_connect();
	ret = SPI_exec(query, 0);

	if (ret == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		uint64 ntuples = tuptable->numvals;

		for (i = 0; i < ntuples; i++)
		{
			HeapTuple tuple = tuptable->vals[i];
			bool isnull;
			Oid table_oid = SPI_getbinval(tuple, tupdesc, 1, &isnull);

			if (!isnull)
			{
				if (vista_add_new_table_to_bitmap((void*)vista_BitmapMgr, table_oid))
				{
					processed_count++;
		            elog(LOG, "Added a table with oid %u", table_oid);
                }
				else
				{
					elog(WARNING, "Could not add table with OID %u to Vista bitmap manager. Already there, or the TCB pool might be full.", table_oid);
				}
			}
		}
	}

	SPI_finish();

	PG_RETURN_INT32(processed_count);
}
