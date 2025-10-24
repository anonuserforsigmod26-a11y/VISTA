#include "vista_libscan_snapshot.h"

#include "vista_heapam_visibility.h"
#include "vista_pg_snapshot.h"

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <dirent.h>
#include <sys/stat.h>

#define VISTA_SNAPSHOT_EXPORT_DIR "pg_snapshots"

typedef int VistaBackendId;			/* unique currently active backend identifier */
typedef uint32_t VistaLocalTransactionId;	/* local transaction id type */
typedef struct
{
	VistaBackendId	backendId;		/* backendId from PGPROC */
	VistaLocalTransactionId localTransactionId;	/* lxid from PGPROC */
} VistaVirtualTransactionId;

static VistaTransactionId
VistaParseXidFromText(const char *prefix, char **s, const char *filename)
{
	char	   *ptr = *s;
	int			prefixlen = strlen(prefix);
	VistaTransactionId val;

	if (strncmp(ptr, prefix, prefixlen) != 0) {
        fprintf(stderr, "[libscan] (parsexid0) invalid snapshot data in file \"%s\"\n", filename);
        return VistaInvalidTransactionId;
    }
	ptr += prefixlen;
	if (sscanf(ptr, "%u", &val) != 1) {
        fprintf(stderr, "[libscan] (parsexid1) invalid snapshot data in file \"%s\"\n", filename);
        return VistaInvalidTransactionId;
    }
	ptr = strchr(ptr, '\n');
	if (!ptr) {
        fprintf(stderr, "[libscan] (parsexid2) invalid snapshot data in file \"%s\"\n", filename);
        return VistaInvalidTransactionId;
    }
	*s = ptr + 1;
	return val;
}

static int
VistaParseIntFromText(const char *prefix, char **s, const char *filename)
{
	char	   *ptr = *s;
	int			prefixlen = strlen(prefix);
	int			val;

	if (strncmp(ptr, prefix, prefixlen) != 0) {
        fprintf(stderr, "[libscan] (parseint0) invalid snapshot data in file \"%s\"\n", filename);
        return -1;
    }
	ptr += prefixlen;
	if (sscanf(ptr, "%d", &val) != 1) {
        fprintf(stderr, "[libscan] (parseint1) invalid snapshot data in file \"%s\"\n", filename);
        return -1;
    }
	ptr = strchr(ptr, '\n');
	if (!ptr) {
        fprintf(stderr, "[libscan] (parseint2) invalid snapshot data in file \"%s\"\n", filename);
        return -1;
    }
	*s = ptr + 1;
	return val;
}

static void
VistaParseVxidFromText(const char *prefix, char **s, const char *filename,
				  VistaVirtualTransactionId *vxid)
{
	char	   *ptr = *s;
	int			prefixlen = strlen(prefix);

	if (strncmp(ptr, prefix, prefixlen) != 0) {
        fprintf(stderr, "[libscan] (parsevxid0) invalid snapshot data in file \"%s\"\n", filename);
        return;
    }
	ptr += prefixlen;
	if (sscanf(ptr, "%d/%u", &vxid->backendId, &vxid->localTransactionId) != 2) {
        fprintf(stderr, "[libscan] (parsevxid1) invalid snapshot data in file \"%s\"\n", filename);
        return;
    }
	ptr = strchr(ptr, '\n');
	if (!ptr) {
        fprintf(stderr, "[libscan] (parsevxid2) invalid snapshot data in file \"%s\"\n", filename);
        return;
    }
	*s = ptr + 1;
}

int
VistaGetSnapshotName(char *name, uint32_t* generation)
{
	return vista_get_snapshot_name(VistaGetShmem(), name, generation);
}

/*
 * ImportSnapshot
 *		Import a previously exported snapshot.  The argument should be a
 *		filename in SNAPSHOT_EXPORT_DIR.  Load the snapshot from that file.
 *		This is called by "SET TRANSACTION SNAPSHOT 'foo'".
 */
void *
VistaImportSnapshot(const char *idstr)
{
	char		path[VISTA_MAXPGPATH];
	FILE	   *f;
	struct stat stat_buf;
	char	   *filebuf;
	int			xcnt;
	int			i;
	VistaVirtualTransactionId src_vxid;
	VistaSnapshotData *snapshot = (VistaSnapshot)calloc(1, sizeof(VistaSnapshotData));

	/*
	 * Verify the identifier: only 0-9, A-F and hyphens are allowed.  We do
	 * this mainly to prevent reading arbitrary files.
	 */
	if (strspn(idstr, "0123456789ABCDEF-") != strlen(idstr)) {
        fprintf(stderr, "[libscan] invalid snapshot identifier: \"%s\"\n", idstr);
        return NULL;
    }

	/* OK, read the file */
	snprintf(path, VISTA_MAXPGPATH, "%s/%s/%s", VISTA_PG_PATH, VISTA_SNAPSHOT_EXPORT_DIR, idstr);
	f = fopen(path, "r");

	if (!f) {
        fprintf(stderr, "[libscan] could not open file \"%s\"\n", path);
        return NULL;
    }

	/* get the size of the file so that we know how much memory we need */
	if (fstat(fileno(f), &stat_buf)) {
        fprintf(stderr, "[libscan] could not stat file \"%s\"\n", path);
        fclose(f);
        return NULL;
    }

	/* and read the file into a buffer */
	char *orig_filebuf = NULL;
	filebuf = (char *)malloc(stat_buf.st_size + 1);
	orig_filebuf = filebuf;
	if (fread(filebuf, stat_buf.st_size, 1, f) != 1) {
        fprintf(stderr, "[libscan] could not read file \"%s\"\n", path);
        fclose(f);
        free(filebuf);
        return NULL;
    }

	filebuf[stat_buf.st_size] = '\0';
	//fprintf(stderr, "[libscan] successfully read snapshot file \"%s\"\n", path);
	fclose(f);

	/*
	 * Construct a snapshot struct by parsing the file content.
	 */
	memset(snapshot, 0, sizeof(VistaSnapshotData));

	VistaParseVxidFromText("vxid:", &filebuf, path, &src_vxid);
	VistaParseIntFromText("pid:", &filebuf, path);
	VistaParseXidFromText("dbid:", &filebuf, path);
	VistaParseIntFromText("iso:", &filebuf, path);
	VistaParseIntFromText("ro:", &filebuf, path);

	snapshot->snapshot_type = VISTA_SNAPSHOT_MVCC;

	snapshot->xmin = VistaParseXidFromText("xmin:", &filebuf, path);
	snapshot->xmax = VistaParseXidFromText("xmax:", &filebuf, path);

	snapshot->xcnt = xcnt = VistaParseIntFromText("xcnt:", &filebuf, path);

	snapshot->xip = (TransactionId *)malloc(xcnt * sizeof(TransactionId));
	for (i = 0; i < xcnt; i++)
		snapshot->xip[i] = VistaParseXidFromText("xip:", &filebuf, path);

	snapshot->suboverflowed = VistaParseIntFromText("sof:", &filebuf, path);

	if (!snapshot->suboverflowed)
	{
		snapshot->subxcnt = xcnt = VistaParseIntFromText("sxcnt:", &filebuf, path);

		snapshot->subxip = (TransactionId *)malloc(xcnt * sizeof(TransactionId));
		for (i = 0; i < xcnt; i++)
			snapshot->subxip[i] = VistaParseXidFromText("sxp:", &filebuf, path);
	}
	else
	{
		snapshot->subxcnt = 0;
		snapshot->subxip = NULL;
	}

	snapshot->takenDuringRecovery = VistaParseIntFromText("rec:", &filebuf, path);
	free(orig_filebuf);

    return (void *)snapshot;
}

int 
VistaFreeSnapshot(void *snapshot)
{
    if (snapshot) {
        VistaSnapshot snap = (VistaSnapshot)snapshot;
        if (snap->xip) {
            free(snap->xip);
        }
        if (snap->subxip) {
            free(snap->subxip);
        }
        free(snap);
        return 0;
    }

    fprintf(stderr, "[libscan] invalid snapshot pointer to free\n");
    return -1;
}
