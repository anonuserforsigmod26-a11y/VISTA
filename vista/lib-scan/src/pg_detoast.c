#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "pg_constants.h"
#include "pg_types.h"
#include "pg_macros.h"

/* Size typedef */
typedef size_t Size;

/* Forward declarations */
static struct varlena *detoast_attr(struct varlena *attr);
struct varlena *detoast_external_attr(struct varlena *attr);
static struct varlena *toast_fetch_datum(struct varlena *attr);
static struct varlena *toast_decompress_datum(struct varlena *attr);

/*
 * pg_detoast_datum - exactly from PostgreSQL fmgr.c
 */
struct varlena *
pg_detoast_datum(struct varlena *datum)
{
	if (VARATT_IS_EXTENDED(datum))
		return detoast_attr(datum);
	else
		return datum;
}

/*
 * detoast_attr - exactly from PostgreSQL detoast.c
 */
static struct varlena *
detoast_attr(struct varlena *attr)
{
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/*
		 * This is an externally stored datum --- fetch it back from there
		 */
		attr = toast_fetch_datum(attr);
		/* If it's compressed, decompress it */
		if (VARATT_IS_COMPRESSED(attr))
		{
			struct varlena *tmp = attr;

			attr = toast_decompress_datum(tmp);
			free(tmp);
		}
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		/*
		 * This is an indirect pointer --- dereference it
		 */
		struct varatt_indirect redirect;

		VARATT_EXTERNAL_GET_POINTER(redirect, attr);
		attr = (struct varlena *) redirect.pointer;

		/* nested indirect Datums aren't allowed */
		assert(!VARATT_IS_EXTERNAL_INDIRECT(attr));

		/* recurse in case value is still extended in some other way */
		attr = detoast_attr(attr);

		/* if it isn't, we'd better copy it */
		if (attr == (struct varlena *) redirect.pointer)
		{
			struct varlena *result;

			result = (struct varlena *) malloc(VARSIZE_ANY(attr));
			memcpy(result, attr, VARSIZE_ANY(attr));
			attr = result;
		}
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		/*
		 * This is an expanded-object pointer --- get flat format
		 */
		attr = detoast_external_attr(attr);
		/* flatteners are not allowed to produce compressed/short output */
		assert(!VARATT_IS_EXTENDED(attr));
	}
	else if (VARATT_IS_COMPRESSED(attr))
	{
		/*
		 * This is a compressed value inside of the main tuple
		 */
		attr = toast_decompress_datum(attr);
	}
	else if (VARATT_IS_SHORT(attr))
	{
		/*
		 * This is a short-header varlena --- convert to 4-byte header format
		 */
		Size		data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
		Size		new_size = data_size + VARHDRSZ;
		struct varlena *new_attr;

		new_attr = (struct varlena *) malloc(new_size);
		SET_VARSIZE(new_attr, new_size);
		memcpy(VARDATA(new_attr), VARDATA_SHORT(attr), data_size);
		attr = new_attr;
	}

	return attr;
}

/* Stub implementations for unsupported TOAST features */
static struct varlena *
toast_fetch_datum(struct varlena *attr)
{
	printf("ERROR: External TOAST data not supported in file scanning\n");
	exit(1);
	return NULL;
}

static struct varlena *
toast_decompress_datum(struct varlena *attr)
{
	printf("ERROR: Compressed TOAST data not supported in file scanning\n");
	exit(1);
	return attr;
}

struct varlena *
detoast_external_attr(struct varlena *attr)
{
	printf("ERROR: Expanded TOAST data not supported in file scanning\n");
	exit(1);
	return attr;
}