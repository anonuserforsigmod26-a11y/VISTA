# Vista UDF: Add All Tables to Bitmap Manager

This document provides instructions on how to create and use the `vista_add_all_tables_to_bitmap` User-Defined Function (UDF) in PostgreSQL.

## Purpose

The `vista_add_all_tables_to_bitmap` function iterates through all user-created tables in the current database and registers them with the Vista bitmap manager. This is useful for pre-populating the manager, especially for tables that have not yet been modified, ensuring they are tracked by Vista from the start.

## Prerequisites

Before creating the function in SQL, you must compile the PostgreSQL source code to include the new UDF. Ensure that `vista_udf.c` has been added to the `Makefile` in `src/backend/storage/vista_buffer/`.

After a successful compilation, restart your PostgreSQL server for the new backend to take effect.

You don't need to do anything particular because it is compiled with build script('scripts/build_and_install.sh')

## Creating the Function

Connect to your target database using `psql` or another client and execute the following SQL command to define the function:

```sql
CREATE OR REPLACE FUNCTION vista_add_all_tables_to_bitmap()
RETURNS integer
AS 'vista_udf', 'vista_add_all_tables_to_bitmap'
LANGUAGE C STRICT;
```

## Calling the Function

Once the function is created, you can call it to register all tables with the bitmap manager. The function will return an integer indicating the number of tables that were successfully added.

Execute the following command:

```sql
SELECT vista_add_all_tables_to_bitmap();
```

### Example Output
```
 vista_add_all_tables_to_bitmap
--------------------------------
                             12
(1 row)
```
This output indicates that 12 tables were successfully registered with the Vista bitmap manager.
