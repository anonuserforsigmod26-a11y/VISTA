LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
ATTACH 'user=vista port=5004 dbname=chbenchmark' AS pg (TYPE postgres);
USE pg.public;
