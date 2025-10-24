CREATE OR REPLACE FUNCTION vista_add_all_tables_to_bitmap()
RETURNS integer
AS 'vista_udf', 'vista_add_all_tables_to_bitmap'
LANGUAGE C STRICT;
SELECT vista_add_all_tables_to_bitmap();
BEGIN;
SELECT 1;
COMMIT;