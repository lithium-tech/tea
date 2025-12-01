#include "tea/gpext/tea_tuple.h"

#if GP_VERSION_NUM >= 60000
#include "access/htup_details.h"
#else
#include "access/heapam.h"
#endif

#define ATT_IS_PACKABLE(att) ((att)->attlen == -1 && (att)->attstorage != 'p')

static Size tea_heap_compute_data_size(TupleDesc tupleDesc, Datum* values, bool* isnull,
                                       const HeapFillTupleInfo* heap_fill_tuple_info) {
  Size data_length = 0;
  Form_pg_attribute* att = tupleDesc->attrs;

  for (uint32_t j = 0; j < heap_fill_tuple_info->retrieved_columns_indices_length; j++) {
    int i = heap_fill_tuple_info->retrieved_columns_indices[j];
    Datum val;

    if (isnull[i]) continue;

    val = values[i];

    if (ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))) {
      /*
       * we're anticipating converting to a short varlena header, so
       * adjust length and don't count any alignment
       */
      data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
    } else {
      data_length = att_align_datum(data_length, att[i]->attalign, att[i]->attlen, val);
      data_length = att_addlength_datum(data_length, att[i]->attlen, val);
    }
  }

  return data_length;
}

static Size tea_heap_fill_tuple(TupleDesc tupleDesc, Datum* values, bool* isnull, char* data, Size data_size,
                                uint16* infomask, bits8* bit, const HeapFillTupleInfo* heap_fill_tuple_info);

// GP6: https://<>/gpdb/-/blob/6X_STABLE/src/backend/access/common/heaptuple.c#L716
// GP5: https://<>/gpdb/-/blob/5.29.9/src/backend/access/common/heaptuple.c#L716
HeapTuple TeaHeapFormTuple(TupleDesc tupleDescriptor, Datum* values, bool* isnull,
                           const HeapFillTupleInfo* heap_fill_tuple_info) {
  HeapTuple tuple;    /* return tuple */
  HeapTupleHeader td; /* tuple data */
  Size actual_len;
  Size len, data_len;
  int hoff;
  bool hasnull = false;
  int numberOfAttributes = tupleDescriptor->natts;
  int i;

  if (numberOfAttributes > MaxTupleAttributeNumber)
    ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                    errmsg("number of columns (%d) exceeds limit (%d)", numberOfAttributes, MaxTupleAttributeNumber)));

#if GP_VERSION_NUM >= 60000
  /*
   * Check for nulls
   */
  for (i = 0; i < numberOfAttributes; i++) {
    if (isnull[i]) {
      hasnull = true;
      break;
    }
  }
#else
  /*
   * Check for nulls and embedded tuples; expand any toasted attributes in
   * embedded tuples.  This preserves the invariant that toasting can only
   * go one level deep.
   *
   * We can skip calling toast_flatten_tuple_attribute() if the attribute
   * couldn't possibly be of composite type.  All composite datums are
   * varlena and have alignment 'd'; furthermore they aren't arrays. Also,
   * if an attribute is already toasted, it must have been sent to disk
   * already and so cannot contain toasted attributes.
   */
  for (i = 0; i < numberOfAttributes; i++) {
    if (isnull[i]) {
      hasnull = true;
      break;
    }
    /* TEA does not support composite types, so toast_flatten_tuple_attribute can be skipped
    else if (att[i]->attlen == -1 && att[i]->attalign == 'd' && att[i]->attndims == 0 &&
             !VARATT_IS_EXTENDED(DatumGetPointer(values[i]))) {
      values[i] = toast_flatten_tuple_attribute(values[i], att[i]->atttypid, att[i]->atttypmod);
    }
    */
  }
#endif

  /*
   * Determine total space needed
   */
  len = offsetof(HeapTupleHeaderData, t_bits);

  if (hasnull) len += BITMAPLEN(numberOfAttributes);

  if (tupleDescriptor->tdhasoid) len += sizeof(Oid);

  hoff = len = MAXALIGN(len); /* align user data safely */

  data_len = tea_heap_compute_data_size(tupleDescriptor, values, isnull, heap_fill_tuple_info);

  len += data_len;

  tuple = (HeapTuple)palloc0(HEAPTUPLESIZE + len);

  /*
   * Allocate and zero the space needed.  Note that the tuple body and
   * HeapTupleData management structure are allocated in one chunk.
   */
  tuple->t_data = td = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);

  /*
   * And fill in the information.  Note we fill the Datum fields even though
   * this tuple may never become a Datum.  This lets HeapTupleHeaderGetDatum
   * identify the tuple type if needed.
   */
  tuple->t_len = len;
  ItemPointerSetInvalid(&(tuple->t_self));

  HeapTupleHeaderSetDatumLength(td, len);
  HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
  HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);

  HeapTupleHeaderSetNatts(td, numberOfAttributes);
  td->t_hoff = hoff;

  if (tupleDescriptor->tdhasoid) /* else leave infomask = 0 */
    td->t_infomask = HEAP_HASOID;

  actual_len = tea_heap_fill_tuple(tupleDescriptor, values, isnull, (char*)td + hoff, data_len, &td->t_infomask,
                                   (hasnull ? td->t_bits : NULL), heap_fill_tuple_info);

  (void)(actual_len);
  Assert(data_len == actual_len);
  Assert(!is_memtuple((GenericTuple)tuple));

  return tuple;
}

#define VARLENA_ATT_IS_PACKABLE(att) ((att)->attstorage != 'p')
/*
 * heap_fill_tuple
 *		Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 *
 *
 * @param isnull will only be used if <code>bit</code> is non-NULL
 * @param bit should be non-NULL (refer to td->t_bits) if isnull is set and contains non-null values
 */
// tea: bit == NULL <=> isnull has null
static Size tea_heap_fill_tuple(TupleDesc tupleDesc, Datum* values, bool* isnull, char* data, Size data_size,
                                uint16* infomask, bits8* bit, const HeapFillTupleInfo* heap_fill_tuple_info) {
  bits8* bitP;
  Form_pg_attribute* att = tupleDesc->attrs;
  char* start = data;

  *infomask &= ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);
  if (bit != NULL) {
    bitP = bit;
    memset(bitP, 0, (tupleDesc->natts + 7) / 8);
    *infomask |= HEAP_HASNULL;
  } else {
    /* just to keep compiler quiet */
    bitP = NULL;
  }

  for (uint32_t j = 0; j < heap_fill_tuple_info->retrieved_columns_indices_length; ++j) {
    int32_t i = heap_fill_tuple_info->retrieved_columns_indices[j];
    Size data_length;

    if (bit != NULL) {
      if (isnull[i]) {
        continue;
      }
      bitP[i >> 3] += (1 << (i & 7));
    }

    /*
     * XXX we use the att_align macros on the pointer value itself, not on
     * an offset.  This is a bit of a hack.
     */

    if (att[i]->attbyval) {
      /* pass-by-value */
      data = (char*)att_align_nominal(data, att[i]->attalign);
      store_att_byval(data, values[i], att[i]->attlen);
      data_length = att[i]->attlen;
    } else if (att[i]->attlen == -1) {
      /* varlena */
      Pointer val = DatumGetPointer(values[i]);

      *infomask |= HEAP_HASVARWIDTH;
      if (VARATT_IS_EXTERNAL(val)) {
        *infomask |= HEAP_HASEXTERNAL;
        /* no alignment, since it's short by definition */
        data_length = VARSIZE_EXTERNAL(val);
        memcpy(data, val, data_length);
      } else if (VARATT_IS_SHORT(val)) {
        /* no alignment for short varlenas */
        data_length = VARSIZE_SHORT(val);
        memcpy(data, val, data_length);
      } else if (VARLENA_ATT_IS_PACKABLE(att[i]) && VARATT_CAN_MAKE_SHORT(val)) {
        /* convert to short varlena -- no alignment */
        data_length = VARATT_CONVERTED_SHORT_SIZE(val);
        SET_VARSIZE_SHORT(data, data_length);
        memcpy(data + 1, VARDATA(val), data_length - 1);
      } else {
        /* full 4-byte header varlena */
        data = (char*)att_align_nominal(data, att[i]->attalign);
        data_length = VARSIZE(val);
        memcpy(data, val, data_length);
      }
    } else if (att[i]->attlen == -2) {
      /* cstring ... never needs alignment */
      *infomask |= HEAP_HASVARWIDTH;
      Assert(att[i]->attalign == 'c');
      data_length = strlen(DatumGetCString(values[i])) + 1;
      memcpy(data, DatumGetPointer(values[i]), data_length);
    } else {
      /* fixed-length pass-by-reference */
      data = (char*)att_align_nominal(data, att[i]->attalign);
      Assert(att[i]->attlen > 0);
      data_length = att[i]->attlen;
      memcpy(data, DatumGetPointer(values[i]), data_length);
    }

    data += data_length;
  }

  Assert((data - start) == data_size);

  return data - start;
}

void InitHeapFormTupleInfo(HeapFillTupleInfo* result, int* columns, int ncolumns) {
  result->retrieved_columns_indices = columns;
  result->retrieved_columns_indices_length = ncolumns;
}
