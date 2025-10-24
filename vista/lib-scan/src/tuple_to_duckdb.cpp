#include "table_config.h"
#include "file_access.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <cassert>
#include <iostream>
#include <limits.h>
#include <unistd.h>

extern "C" {
#include "tuple_access.h"
#include "pg_macros.h"
#include "pg_constants.h"
#include "memory_utils.h"
#ifdef USE_VISTA_IO
#include "vista_wrapper.h"
#include "vista_heapam_visibility.h"
#endif
}

// Undefine conflicting macros before including DuckDB headers
#ifdef Min
#undef Min
#endif
#ifdef Max
#undef Max  
#endif
#ifdef SECS_PER_DAY
#undef SECS_PER_DAY
#endif


// DuckDB headers - include after macro cleanup
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "tuple_to_duckdb.h"
#include <map>
#include <climits>
#include <vector>
#include <algorithm>


// Use DuckDB namespace
using namespace duckdb;

// Track tuple count per block (commented out for performance)
// static std::map<BlockNumber, int> g_block_tuple_count;


/* Extract precision and scale from NUMERIC type modifier (from pg_duckdb) */
static inline int32
make_numeric_typmod(int precision, int scale) {
    return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

static inline int
numeric_typmod_precision(int32 typmod) {
    return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod) {
    return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

using duckdb::hugeint_t;
using duckdb::uhugeint_t;

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;

typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

struct DecimalConversionInteger {
	static int64_t
	GetPowerOfTen(idx_t index) {
		static const int64_t POWERS_OF_TEN[] {1,
		                                      10,
		                                      100,
		                                      1000,
		                                      10000,
		                                      100000,
		                                      1000000,
		                                      10000000,
		                                      100000000,
		                                      1000000000,
		                                      10000000000,
		                                      100000000000,
		                                      1000000000000,
		                                      10000000000000,
		                                      100000000000000,
		                                      1000000000000000,
		                                      10000000000000000,
		                                      100000000000000000,
		                                      1000000000000000000};
		if (index >= 19) {
			throw duckdb::InternalException("DecimalConversionInteger::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	template <class T>
	static T
	Finalize(const NumericVar &, T result) {
		return result;
	}
};

struct DecimalConversionHugeint {
	static hugeint_t
	GetPowerOfTen(idx_t index) {
		static const hugeint_t POWERS_OF_TEN[] {
		    hugeint_t(1),
		    hugeint_t(10),
		    hugeint_t(100),
		    hugeint_t(1000),
		    hugeint_t(10000),
		    hugeint_t(100000),
		    hugeint_t(1000000),
		    hugeint_t(10000000),
		    hugeint_t(100000000),
		    hugeint_t(1000000000),
		    hugeint_t(10000000000),
		    hugeint_t(100000000000),
		    hugeint_t(1000000000000),
		    hugeint_t(10000000000000),
		    hugeint_t(100000000000000),
		    hugeint_t(1000000000000000),
		    hugeint_t(10000000000000000),
		    hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(100),
		    hugeint_t(1000000000000000000) * hugeint_t(1000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};
		if (index >= 39) {
			throw duckdb::InternalException("DecimalConversionHugeint::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	static hugeint_t
	Finalize(const NumericVar &, hugeint_t result) {
		return result;
	}
};

struct DecimalConversionDouble {
	static double
	GetPowerOfTen(idx_t index) {
		return pow(10, double(index));
	}

	static double
	Finalize(const NumericVar &numeric, double result) {
		return result / GetPowerOfTen(numeric.dscale);
	}
};

/* PostgreSQL varbit_out function - adapted from src/backend/utils/adt/varbit.c */
/* Original uses PG_FUNCTION_ARGS, we adapt it to take VarBit* directly */
static char *
varbit_out(VarBit *s)
{
	char	   *result,
			   *r;
	bits8	   *sp;
	bits8		x;
	int			i,
				k,
				len;

	/* Assertion to help catch any bit functions that don't pad correctly */
	/* VARBIT_CORRECTLY_PADDED(s); - we skip this assertion */

	len = VARBITLEN(s);
	result = (char *) malloc(len + 1);  /* palloc -> malloc */
	sp = VARBITS(s);
	r = result;
	for (i = 0; i <= len - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++)
	{
		/* print full bytes */
		x = *sp;
		for (k = 0; k < BITS_PER_BYTE; k++)
		{
			*r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
			x <<= 1;
		}
	}
	if (i < len)
	{
		/* print the last partial byte */
		x = *sp;
		for (k = i; k < len; k++)
		{
			*r++ = IS_HIGHBIT_SET(x) ? '1' : '0';
			x <<= 1;
		}
	}
	*r = '\0';

	return result;  /* PG_RETURN_CSTRING -> return */
}

/* pg_duckdb functions */
static duckdb::interval_t
DatumGetInterval(Datum value) {
	/* Use :: to specify global (PostgreSQL) Interval type, not duckdb::Interval class */
	::Interval *pg_interval = DatumGetIntervalP(value);
	duckdb::interval_t duck_interval;
	duck_interval.months = pg_interval->month;
	duck_interval.days = pg_interval->day;
	duck_interval.micros = pg_interval->time;
	return duck_interval;
}

static std::string
DatumGetBitString(Datum value) {
	// Here we rely on postgres conversion function, instead of manual parsing,
	// because BIT string type involves padding and duckdb/postgres handle it
	// differently, it's non-trivial to memcpy the bits.
	//
	// NOTE: We use VarbitToString here, because BIT and VARBIT are both stored
	// internally as a VARBIT in postgres.
	VarBit *vb = DatumGetVarBitP(value);
	char *str = varbit_out(vb);
	std::string result(str);
	free(str);
	return result;
}

static duckdb::dtime_t
DatumGetTime(Datum value) {
	const TimeADT pg_time = DatumGetTimeADT(value);
	duckdb::dtime_t duckdb_time {pg_time};
	return duckdb_time;
}

static duckdb::dtime_tz_t
DatumGetTimeTz(Datum value) {
	TimeTzADT *tzt = static_cast<TimeTzADT *>(DatumGetTimeTzADTP(value));
	// pg and duckdb stores timezone with different signs, for example, for TIMETZ 01:02:03+05, duckdb stores offset =
	// 18000, while pg stores zone = -18000.
	const uint64_t bits = duckdb::dtime_tz_t::encode_micros(static_cast<int64_t>(tzt->time)) |
	                      duckdb::dtime_tz_t::encode_offset(-tzt->zone);
	const duckdb::dtime_tz_t duck_time_tz {bits};
	return duck_time_tz;
}

/* CHAR(n) true length function from PostgreSQL */
static int bpchartruelen(char *s, int len) {
    int i;
    
    /*
     * Note that we rely on the assumption that ' ' is a singleton unit on
     * every supported multibyte server encoding.
     */
    for (i = len - 1; i >= 0; i--) {
        if (s[i] != ' ')
            break;
    }
    return i + 1;
}

template <class T>
static void
Append(duckdb::Vector &result, T value, idx_t offset) {
	auto data = duckdb::FlatVector::GetData<T>(result);
	data[offset] = value;
}

static void
AppendString(duckdb::Vector &result, Datum value, idx_t offset, bool is_bpchar) {
	const char *text = VARDATA_ANY(value);
	/* Remove the padding of a BPCHAR type. DuckDB expects unpadded value. */
	auto len = is_bpchar ? bpchartruelen(VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value)) : VARSIZE_ANY_EXHDR(value);
	duckdb::string_t str(text, len);

	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, str);
}

static void
AppendDate(duckdb::Vector &result, Datum value, idx_t offset) {
	auto date = DatumGetDateADT(value);
	if (date == DATEVAL_NOBEGIN) {
		// -infinity value is different between PG and duck
		Append<duckdb::date_t>(result, duckdb::date_t::ninfinity(), offset);
		return;
	}
	if (date == DATEVAL_NOEND) {
		Append<duckdb::date_t>(result, duckdb::date_t::infinity(), offset);
		return;
	}

	Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(date + 10957)), offset);
}

static void
AppendTimestamp(duckdb::Vector &result, Datum value, idx_t offset) {
	int64_t timestamp = static_cast<int64_t>(DatumGetTimestamp(value));
	if (timestamp == DT_NOBEGIN) {
		// -infinity value is different between PG and duck
		Append<duckdb::timestamp_t>(result, duckdb::timestamp_t::ninfinity(), offset);
		return;
	}
	if (timestamp == DT_NOEND) {
		Append<duckdb::timestamp_t>(result, duckdb::timestamp_t::infinity(), offset);
		return;
	}

	// Bounds Check
	if (timestamp < -210866803200000000 || timestamp >= 9223371244800000000)
		throw duckdb::OutOfRangeException(
		    "The Timestamp value should be between min and max value");

	Append<duckdb::timestamp_t>(result, duckdb::timestamp_t(timestamp + 946684800000000), offset);
}

NumericVar
FromNumeric(Numeric num) {
	NumericVar dest;
	dest.ndigits = NUMERIC_NDIGITS(num);
	dest.weight = NUMERIC_WEIGHT(num);
	dest.sign = NUMERIC_SIGN(num);
	dest.dscale = NUMERIC_DSCALE(num);
	dest.digits = NUMERIC_DIGITS(num);
	dest.buf = NULL; /* digits array is not palloc'd */
	return dest;
}

template <class T, class OP = DecimalConversionInteger>
T
ConvertDecimal(const NumericVar &numeric) {
	auto scale_POWER = OP::GetPowerOfTen(numeric.dscale);

	if (numeric.ndigits == 0) {
		return 0;
	}

	T integral_part = 0, fractional_part = 0;
	if (numeric.weight >= 0) {
		int32_t digit_index = 0;
		integral_part = numeric.digits[digit_index++];
		for (; digit_index <= numeric.weight; digit_index++) {
			integral_part *= NBASE;
			if (digit_index < numeric.ndigits) {
				integral_part += numeric.digits[digit_index];
			}
		}
		integral_part *= scale_POWER;
	}

	// we need to find out how large the fractional part is in terms of powers
	// of ten this depends on how many times we multiplied with NBASE
	// if that is different from scale, we need to divide the extra part away
	// again
	// similarly, if trailing zeroes have been suppressed, we have not been multiplying t
	// the fractional part with NBASE often enough. If so, add additional powers
	if (numeric.ndigits > numeric.weight + 1) {
		auto fractional_power = (numeric.ndigits - numeric.weight - 1) * DEC_DIGITS;
		auto fractional_power_correction = fractional_power - numeric.dscale;
		fractional_part = 0;
		for (int32_t i = duckdb::MaxValue<int32_t>(0, numeric.weight + 1); i < numeric.ndigits; i++) {
			if (i + 1 < numeric.ndigits) {
				// more digits remain - no need to compensate yet
				fractional_part *= NBASE;
				fractional_part += numeric.digits[i];
			} else {
				// last digit, compensate
				T final_base = NBASE;
				T final_digit = numeric.digits[i];
				if (fractional_power_correction >= 0) {
					T compensation = OP::GetPowerOfTen(fractional_power_correction);
					final_base /= compensation;
					final_digit /= compensation;
				} else {
					T compensation = OP::GetPowerOfTen(-fractional_power_correction);
					final_base *= compensation;
					final_digit *= compensation;
				}
				fractional_part *= final_base;
				fractional_part += final_digit;
			}
		}
	}

	// finally
	auto base_res = OP::Finalize(numeric, integral_part + fractional_part);
	return numeric.sign == NUMERIC_NEG ? -base_res : base_res;
}

void
ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, idx_t offset) {
	auto &type = result.GetType();
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		Append<bool>(result, DatumGetBool(value), offset);
		break;
	case duckdb::LogicalTypeId::TINYINT: {
		Append<int16_t>(result, DatumGetInt16(value), offset);
		break;
	}
	case duckdb::LogicalTypeId::SMALLINT:
		Append<int16_t>(result, DatumGetInt16(value), offset);
		break;
	case duckdb::LogicalTypeId::INTEGER:
		Append<int32_t>(result, DatumGetInt32(value), offset);
		break;
	case duckdb::LogicalTypeId::UINTEGER:
		Append<uint32_t>(result, DatumGetUInt32(value), offset);
		break;
	case duckdb::LogicalTypeId::BIGINT:
		Append<int64_t>(result, DatumGetInt64(value), offset);
		break;
	case duckdb::LogicalTypeId::VARCHAR: {
		// NOTE: This also handles JSON
		if (attr_type == JSONBOID) {
			// AppendJsonb(result, value, offset);
			// For now, treat JSONB as string
			AppendString(result, value, offset, false);
			break;
		} else {
			AppendString(result, value, offset, attr_type == BPCHAROID);
		}
		break;
	}
	case duckdb::LogicalTypeId::DATE:
		AppendDate(result, value, offset);
		break;

	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
	case duckdb::LogicalTypeId::TIMESTAMP:
		AppendTimestamp(result, value, offset);
		break;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		AppendTimestamp(result, value, offset); // Timestamp and Timestamptz are basically same in PG
		break;
	case duckdb::LogicalTypeId::INTERVAL:
		Append<duckdb::interval_t>(result, DatumGetInterval(value), offset);
		break;
	case duckdb::LogicalTypeId::BIT:
		Append<duckdb::bitstring_t>(result, duckdb::Bit::ToBit(DatumGetBitString(value)), offset);
		break;
	case duckdb::LogicalTypeId::TIME:
		Append<duckdb::dtime_t>(result, DatumGetTime(value), offset);
		break;
	case duckdb::LogicalTypeId::TIME_TZ:
		Append<duckdb::dtime_tz_t>(result, DatumGetTimeTz(value), offset);
		break;
	case duckdb::LogicalTypeId::FLOAT:
		Append<float>(result, DatumGetFloat4(value), offset);
		break;
	case duckdb::LogicalTypeId::DOUBLE: {
		if (attr_type == NUMERICOID) {
			// This NUMERIC could not be converted to a DECIMAL, convert it as DOUBLE instead
			auto numeric = DatumGetNumeric(value);
			auto numeric_var = FromNumeric(numeric);
			auto double_val = ConvertDecimal<double, DecimalConversionDouble>(numeric_var);
			Append<double>(result, double_val, offset);
		} else {
			Append<double>(result, DatumGetFloat8(value), offset);
		}
		break;
	}
	case duckdb::LogicalTypeId::DECIMAL: {
		auto physical_type = type.InternalType();
		
		// Preserve original pointer to check if detoasting occurred
		Datum original_value = value;
		auto numeric = DatumGetNumeric(value);
		
		// Check if detoasting occurred (different pointers mean detoasting happened)
		bool was_detoasted = (DatumGetPointer(original_value) != (Pointer)numeric);
		
		auto numeric_var = FromNumeric(numeric);
		
		switch (physical_type) {
		case duckdb::PhysicalType::INT16: {
			Append(result, ConvertDecimal<int16_t>(numeric_var), offset);
			break;
		}
		case duckdb::PhysicalType::INT32: {
			Append(result, ConvertDecimal<int32_t>(numeric_var), offset);
			break;
		}
		case duckdb::PhysicalType::INT64: {
			Append(result, ConvertDecimal<int64_t>(numeric_var), offset);
			break;
		}
		case duckdb::PhysicalType::INT128: {
			Append(result, ConvertDecimal<hugeint_t, DecimalConversionHugeint>(numeric_var), offset);
			break;
		}
		default:
			throw duckdb::InternalException("Unrecognized physical type for DECIMAL value");
		}
		
		// Free memory if detoasting allocated new memory
		if (was_detoasted) {
			simple_free((void*)numeric);
		}
		break;
	}
	case duckdb::LogicalTypeId::UUID: {
		auto uuid = DatumGetPointer(value);
		hugeint_t duckdb_uuid;
		for (idx_t i = 0; i < 16; i++) {
			((uint8_t *)&duckdb_uuid)[16 - 1 - i] = ((uint8_t *)uuid)[i];
		}
		duckdb_uuid.upper ^= (uint64_t(1) << 63);
		Append(result, duckdb_uuid, offset);
		break;
	}
	case duckdb::LogicalTypeId::BLOB: {
		const char *bytea_data = VARDATA_ANY(value);
		size_t bytea_length = VARSIZE_ANY_EXHDR(value);
		const duckdb::string_t s(bytea_data, bytea_length);
		auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
		data[offset] = duckdb::StringVector::AddStringOrBlob(result, s);
		break;
	}
	default:
		throw duckdb::NotImplementedException("(DuckDB/ConvertPostgresToDuckValue) Unsupported pgduckdb type: %s",
		                                      result.GetType().ToString().c_str());
	}
}

/* Insert a single PostgreSQL tuple into DuckDB DataChunk - exact pg_duckdb pattern */
void InsertTupleIntoChunk(DataChunk &output, TupleTableSlot *slot, idx_t row_idx) {
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    
    for (int col_idx = 0; col_idx < tupdesc->natts; col_idx++) {
        auto &result_vector = output.data[col_idx];
        
        if (slot->tts_isnull[col_idx]) {
            /* Handle NULL values using pg_duckdb pattern */
            auto &validity = duckdb::FlatVector::Validity(result_vector);
            validity.SetInvalid(row_idx);
        } else {
            /* Handle non-NULL values */
            auto attr = &tupdesc->attrs[col_idx];
            
            /* For variable-length types, we may need detoasting like pg_duckdb
             * but for TPC-H simplicity, we assume data is already detoasted */
            ConvertPostgresToDuckValue(attr->atttypid, slot->tts_values[col_idx], 
                                     result_vector, row_idx);
        }
    }
}
/* Process one block - creates slot once and reuses */
idx_t ProcessBlock(DataChunk &chunk, TupleDesc tupdesc,
                   PageHeader page, idx_t start_offset,
                   OffsetNumber start_tuple_offset, OffsetNumber *next_tuple_offset,
                   VistaSnapshot snapshot, int *all_tuples_valid) {
    idx_t tuples_processed = 0;
    *all_tuples_valid = 1;

    if (!page || page->pd_lower < sizeof(PageHeaderData) ||
        page->pd_lower > page->pd_upper) {
        if (next_tuple_offset) *next_tuple_offset = FirstOffsetNumber;
        return 0;
    }

    /* Create slot once for this block */
    TupleTableSlot *slot = MakeTupleTableSlot(tupdesc);
    if (!slot) {
        if (next_tuple_offset) *next_tuple_offset = start_tuple_offset;
        return 0;
    }

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    OffsetNumber offnum;

    for (offnum = start_tuple_offset;
         offnum <= maxoff && start_offset + tuples_processed < STANDARD_VECTOR_SIZE;
         offnum++) {

        ItemId lp = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(lp)) {
            continue;
        }

        HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, lp);
        if (!htup) {
            continue;
        }

        /* Clear slot for reuse */
        ExecClearTuple(slot);

        /* Reuse the same slot */
        HeapTupleData htupdata;
        htupdata.t_len = ItemIdGetLength(lp);
        htupdata.t_data = htup;

        assert(snapshot != NULL);

        int tuple_valid;
        if (!VistaHeapTupleSatisfiesMVCC((VistaHeapTuple)&htupdata, snapshot, &tuple_valid)) {
            if (!tuple_valid) *all_tuples_valid = 0;
            continue; // Skip invisible tuples
        }
        if (!tuple_valid) *all_tuples_valid = 0;
        
        ExecStoreBufferHeapTuple(&htupdata, slot, InvalidBuffer);
        
        /* Deform tuple */
        BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *)slot;
        bslot->base.off = 0;  /* Initialize offset */
        slot_deform_heap_tuple(slot, &htupdata, &bslot->base.off, tupdesc->natts);
        
        /* Insert into DuckDB chunk */
        try {
            InsertTupleIntoChunk(chunk, slot, start_offset + tuples_processed);
            tuples_processed++;
        } catch (...) {
            // Skip this tuple but continue processing
        }
    }
    
    /* Set next tuple offset for continuation */
    if (next_tuple_offset) {
        if (offnum > maxoff) {
            /* Finished this block, start from beginning of next block */
            *next_tuple_offset = FirstOffsetNumber;
        } else {
            /* Chunk is full, continue from current tuple */
            *next_tuple_offset = offnum;
        }
    }
    
    /* Cleanup slot once at the end */
    ExecDropSingleTupleTableSlot(slot);
    
    return tuples_processed;
}

void CleanupFileBuffers() {
}


void InitFileBuffers(uint32_t db_node, uint32_t rel_node) {
    struct args_struct args = {(int)db_node, (int)rel_node, 0, MAX_SEGMENT_COUNT};
    FileBufferInit(&args);
}

/* Create TupleDesc from column information */
void* CreateTupleDescFromColumns(int num_columns,
                                int* type_oids,
                                int16_t* lengths,
                                bool* nullables,
                                char* alignments,
                                bool* by_values) {
    if (num_columns <= 0 || num_columns > MAX_COLUMNS) {
        return NULL;
    }
    
    // Create temporary TableConfig structure
    TableConfig config;
    config.num_columns = num_columns;
    
    for (int i = 0; i < num_columns; i++) {
        snprintf(config.columns[i].name, MAX_COLUMN_NAME, "col_%d", i);
        config.columns[i].type_oid = type_oids[i];
        config.columns[i].length = lengths[i];
        config.columns[i].nullable = nullables[i];
        config.columns[i].alignment = alignments[i];
        config.columns[i].by_value = by_values[i];
    }
    
    // Use existing function to create TupleDesc
    TupleDesc tupdesc = create_tuple_desc_from_config(&config);
    return (void*)tupdesc;
}

/* Free TupleDesc created by CreateTupleDescFromColumns */
void FreeTupleDesc(void* tupdesc) {
    if (tupdesc) {
        simple_free((TupleDesc)tupdesc);
    }
}

/* BlockGroup API Implementation */

BlockGroupInfo GetBlockGroupInfo(uint32_t db_node, uint32_t rel_node, uint32_t block_group_size) {
    BlockGroupInfo info;
    
    // Get total number of blocks for this table
    info.total_blocks = mdnblocks(db_node, rel_node);
    info.blocks_per_group = block_group_size;
    
    // Calculate total number of BlockGroups
    if (info.total_blocks == 0) {
        info.total_groups = 0;
    } else {
        info.total_groups = (info.total_blocks + block_group_size - 1) / block_group_size;
    }
    
    // printf("[libscan] GetBlockGroupInfo: db_node=%u, rel_node=%u, total_blocks=%u, block_group_size=%u, total_groups=%u\n",
    //        db_node, rel_node, info.total_blocks, block_group_size, info.total_groups);
    
    return info;
}

#ifdef USE_VISTA_IO
// Initialize Vista channel for BlockGroup using existing opened files
static bool InitVistaChannelForBlockGroup(PostgresScanBlockGroupState* state) {
    // If all SubGroups are cached (mask=0), no Vista initialization needed
    if (state->uncached_subgroup_mask == 0) {
        state->vd = -1;
        state->vista_initialized = false;
        return true;  // Success - no Vista needed
    }

    // Use existing opened file (segment 0)
    // Calculate correct segment number for this BlockGroup
    uint32_t segment_no = state->start_blkno / 131072;
    File file = OpenOrGetSegment(state->db_node, state->rel_node, segment_no);
    if (file < 0) {
        return false;
    }

    // Setup Vista with multi-range for uncached SubGroups only
    state->vd = vista_wrapper_open_multi_range(file, state->start_blkno, state->end_blkno,
                                               state->sub_group_size, state->uncached_subgroup_mask);
    if (state->vd < 0) {
        return false;
    }

    state->vista_initialized = true;
    return true;
}
#endif

void* InitPostgresScanForBlockGroup(uint32_t spc_node, uint32_t db_node, uint32_t rel_node, void* tupdesc,
                                   uint32_t block_group_id, uint32_t block_group_size, uint32_t sub_group_size,
                                   uint32_t uncached_subgroup_mask, VistaSnapshot snapshot) {
    PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)malloc(sizeof(PostgresScanBlockGroupState));
    if (!state) {
        return nullptr;
    }

    state->spc_node = spc_node;
    state->db_node = db_node;
    state->rel_node = rel_node;
    state->tupdesc = (TupleDesc)tupdesc;
    state->block_group_id = block_group_id;
    state->block_group_size = block_group_size;
    state->sub_group_size = sub_group_size;
    state->uncached_subgroup_mask = uncached_subgroup_mask;
    state->snapshot = snapshot;

    // Calculate BlockGroup range
    state->start_blkno = block_group_id * block_group_size;
    state->end_blkno = (block_group_id + 1) * block_group_size - 1;
    
    // Adjust end_blkno to not exceed total blocks
    uint32_t total_blocks = mdnblocks(db_node, rel_node);
    if (state->end_blkno >= total_blocks) {
        state->end_blkno = total_blocks - 1;
    }
    
    // Initialize scan position
    state->current_blkno = state->start_blkno;
    state->current_tuple_offset = FirstOffsetNumber;
    state->finished = false;
    state->current_subgroup_all_valid = 1;
    
#ifdef USE_VISTA_IO
    // Initialize Vista fields
    state->vd = -1;
    state->vista_initialized = false;
    state->current_page = NULL;
    state->has_active_page = false;
#ifdef USE_VISTA_REMAP
    state->using_remapped_page = false;
#endif
    
    // Initialize Vista channel for this BlockGroup
    if (!InitVistaChannelForBlockGroup(state)) {
        fprintf(stderr, "[libscan] Failed to initialize Vista channel for BlockGroup %u\n", block_group_id);
        free(state);
        return nullptr;
    }
#endif
    
    return (void*)state;
}

#ifdef USE_VISTA_IO
/* Vista-based BlockGroup implementation */
bool FillChunkFromBlockGroup_Vista(void* chunk, void* scan_state) {
    unsigned int gen;
    duckdb::DataChunk* duckdb_chunk = (duckdb::DataChunk*)chunk;
    PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)scan_state;
    
    if (!state || state->finished) {
        return false;
    }
    
    // Vista must be initialized at this point
    if (!state->vista_initialized) {
        fprintf(stderr, "[libscan] Error: Vista not initialized in FillChunkFromBlockGroup_Vista\n");
        return false;
    }

    // Reset chunk for new batch
    duckdb_chunk->Reset();
    duckdb::idx_t chunk_offset = 0;

    // Calculate current SubGroup boundaries
    BlockNumber current_subgroup_start = ((state->current_blkno - state->start_blkno) / state->sub_group_size) * state->sub_group_size + state->start_blkno;
    BlockNumber current_subgroup_end = current_subgroup_start + state->sub_group_size - 1;    // Ensure we don't go beyond BlockGroup boundary
    if (current_subgroup_end > state->end_blkno) {
        current_subgroup_end = state->end_blkno;
    }

    // Check if we're at the start of a new SubGroup
    if (state->current_blkno == current_subgroup_start) {
        state->current_subgroup_all_valid = 1;  // Reset for new SubGroup
    }

    // Fill chunk by processing blocks within current SubGroup range
    while (chunk_offset < STANDARD_VECTOR_SIZE &&
           state->current_blkno <= state->end_blkno &&
           state->current_blkno <= current_subgroup_end) {
        
        // 1. Get page if needed
        if (!state->has_active_page) {
            // Try remapped page first
            void* remapped_page = NULL;

retry_remap_access:
#ifdef USE_VISTA_REMAP
            remapped_page = vista_wrapper_try_read_remapped_page(state->spc_node, state->db_node,
                                                                 state->rel_node, state->current_blkno,
                                                                 &gen);
            // remapped_page = nullptr;
#endif
            // Always get Vista page (required for Vista's internal state management)
            void* vista_page = vista_wrapper_get_page(state->vd, gen);
            if (vista_page == (void*)VISTA_RETRY_REMAP_ACCESS_VALUE) {
            	if (remapped_page)
            		vista_wrapper_release_remapped_page();
                goto retry_remap_access;
            }

            // Check for Vista return values
            if (vista_page == (void*)VISTA_NO_MORE_PAGES_TO_READ_VALUE) {
                // We processed all of the blocks in the block group!
								// Normal end of data
                state->finished = true;
		if (remapped_page)
            		vista_wrapper_release_remapped_page();

                break;
            } else if (vista_page == (void*)VISTA_INVALID_VD_VALUE) {
                fprintf(stderr, "[Error] Vista returned VISTA_INVALID_VD for blkno %u, BlockGroup %u\n", state->current_blkno, state->block_group_id);
                state->finished = true;
		if (remapped_page)
            		vista_wrapper_release_remapped_page();

                break;
            } else if (vista_page == (void*)VISTA_NO_PUT_PAGE_CALLED_VALUE) {
                fprintf(stderr, "[Error] Vista returned VISTA_NO_PUT_PAGE_CALLED for blkno %u\n", state->current_blkno);
                state->finished = true;
		if (remapped_page)
            		vista_wrapper_release_remapped_page();

                break;
            } else if (vista_page == NULL) {
                fprintf(stderr, "[Error] Error: Vista returned NULL unexpectedly\n");
                state->finished = true;
		if (remapped_page)
            		vista_wrapper_release_remapped_page();

                break;
            }

            // Use remapped page if available, otherwise use Vista page
#ifdef USE_VISTA_REMAP
            if (remapped_page != NULL) {
                state->current_page = remapped_page;
                state->using_remapped_page = true;
            } else {
                state->current_page = vista_page;
                state->using_remapped_page = false;
            }
#else
            state->current_page = vista_page;
#endif

            state->has_active_page = true;
        }

        // 2. Process current page
        OffsetNumber next_tuple_offset;
        int block_all_valid = 1;
        duckdb::idx_t tuples_in_block = ProcessBlock(*duckdb_chunk, state->tupdesc,
                                                     (PageHeader)state->current_page, chunk_offset,
                                                     state->current_tuple_offset, &next_tuple_offset,
                                                     state->snapshot, &block_all_valid);

        // Update SubGroup validity status
        if (!block_all_valid) {
            state->current_subgroup_all_valid = 0;
        }

        chunk_offset += tuples_in_block;
        
        // 3. Check block completion status
        if (next_tuple_offset == FirstOffsetNumber) {
            // Block finished - release page and move to next block
            
            // Release remapped page if using one
#ifdef USE_VISTA_REMAP
            if (state->using_remapped_page) {
                vista_wrapper_release_remapped_page();
                state->using_remapped_page = false;
            }
#endif
            
            // Always release the original page from Vista
            vista_wrapper_put_page(state->vd, state->current_page);
            state->has_active_page = false;
            state->current_page = NULL;
            state->current_blkno++;
            state->current_tuple_offset = FirstOffsetNumber;
        } else {
            // Block not finished, chunk is full - keep page for next call
            state->current_tuple_offset = next_tuple_offset;
            break;
        }
    }

    // Check if we have any data
    if (chunk_offset > 0) {
        // Set the actual number of tuples in chunk
        duckdb_chunk->SetCardinality(chunk_offset);
        return true;
    } else {
        // Check if we've truly reached the end
        if (state->current_blkno > state->end_blkno) {
            state->finished = true;
        }

        // If already finished (Vista error or BlockGroup end), return false
        if (state->finished) {
            return false;
        }

        // Otherwise, this is just empty due to visibility filtering
        // Continue reading - next block may have visible tuples
        return true;
    }
}
#endif

/* Main FillChunkFromBlockGroup function */
bool FillChunkFromBlockGroup(void* chunk, void* scan_state) {
#ifdef USE_VISTA_IO
    return FillChunkFromBlockGroup_Vista(chunk, scan_state);
#endif
}

/* Cleanup PostgreSQL BlockGroup scanner state */
void CleanupPostgresScanBlockGroup(void* scan_state) {
    if (scan_state) {
#ifdef USE_VISTA_IO
        PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)scan_state;
        
        // Clean up Vista resources
        if (state->vista_initialized) {
            // Release any active page
            if (state->has_active_page && state->current_page) {
                // Release remapped page if using one
#ifdef USE_VISTA_REMAP
                if (state->using_remapped_page) {
                    vista_wrapper_release_remapped_page();
                    state->using_remapped_page = false;
                }
#endif
                
                // Always release the original page
                vista_wrapper_put_page(state->vd, state->current_page);
            }
            
            // Close Vista channel (Vista OLAP remains registered for process lifetime)
            vista_wrapper_close(state->vd);
        }
#endif
        free(scan_state);
    }
}

uint32_t GetCurrentSubGroupId(void* scan_state) {
    if (!scan_state) {
        return 0;
    }

    PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)scan_state;

    // Calculate relative block number within BlockGroup
    uint32_t relative_block = state->current_blkno - state->start_blkno;

    // Calculate SubGroup ID (0-based)
    // Vista multi-range preserves actual block numbers, so this calculation is correct
    uint32_t subgroup_id = relative_block / state->sub_group_size;

    return subgroup_id;
}

int GetCurrentSubGroupAllValid(void* scan_state) {
    if (!scan_state) {
        return 0;
    }

    PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)scan_state;
    return state->current_subgroup_all_valid;
}

bool SkipSubGroupInVista(void* scan_state, uint32_t subgroup_id) {
    if (!scan_state) {
        return false;
    }

    PostgresScanBlockGroupState* state = (PostgresScanBlockGroupState*)scan_state;

#ifdef USE_VISTA_IO
    if (!state->vista_initialized) {
        return false;
    }

    // Calculate SubGroup boundaries
    BlockNumber subgroup_start = state->start_blkno + (subgroup_id * state->sub_group_size);
    BlockNumber subgroup_end = subgroup_start + state->sub_group_size - 1;

    // Ensure we don't go beyond BlockGroup boundary
    if (subgroup_end > state->end_blkno) {
        subgroup_end = state->end_blkno;
    }

    // Skip pages in Vista for this SubGroup
    while (state->current_blkno <= subgroup_end) {
        // No Vista I/O needed for cached SubGroups
        // Vista only contains uncached SubGroups, so cached ones are skipped by advancing position only

        // Move to next block
        state->current_blkno++;
        state->current_tuple_offset = FirstOffsetNumber;
    }
#endif

    return true;
}
