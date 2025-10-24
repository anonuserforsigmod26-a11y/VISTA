LOAD '{EXTENSION_PATH}';
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
SET threads=32;
SET memory_limit='40GB';
ATTACH 'user=vista port=5001 dbname=chbenchmark' AS pg (TYPE postgres);
USE pg.public;
.mode trash
