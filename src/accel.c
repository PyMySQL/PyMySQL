
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <Python.h>
#include <datetime.h>

#define MYSQLSV_OUT_TUPLES 0
#define MYSQLSV_OUT_NAMEDTUPLES 1
#define MYSQLSV_OUT_DICTS 2
#define MYSQLSV_OUT_NUMPY 3
#define MYSQLSV_OUT_PANDAS 4

#define MYSQL_FLAG_NOT_NULL 1
#define MYSQL_FLAG_PRI_KEY 2
#define MYSQL_FLAG_UNIQUE_KEY 4
#define MYSQL_FLAG_MULTIPLE_KEY 8
#define MYSQL_FLAG_BLOB 16
#define MYSQL_FLAG_UNSIGNED 32
#define MYSQL_FLAG_ZEROFILL 64
#define MYSQL_FLAG_BINARY 128
#define MYSQL_FLAG_ENUM 256
#define MYSQL_FLAG_AUTO_INCREMENT 512
#define MYSQL_FLAG_TIMESTAMP 1024
#define MYSQL_FLAG_SET 2048
#define MYSQL_FLAG_PART_KEY 16384
#define MYSQL_FLAG_GROUP 32767
#define MYSQL_FLAG_UNIQUE 65536

#define MYSQL_TYPE_DECIMAL 0
#define MYSQL_TYPE_TINY 1
#define MYSQL_TYPE_SHORT 2
#define MYSQL_TYPE_LONG 3
#define MYSQL_TYPE_FLOAT 4
#define MYSQL_TYPE_DOUBLE 5
#define MYSQL_TYPE_NULL 6
#define MYSQL_TYPE_TIMESTAMP 7
#define MYSQL_TYPE_LONGLONG 8
#define MYSQL_TYPE_INT24 9
#define MYSQL_TYPE_DATE 10
#define MYSQL_TYPE_TIME 11
#define MYSQL_TYPE_DATETIME 12
#define MYSQL_TYPE_YEAR 13
#define MYSQL_TYPE_NEWDATE 14
#define MYSQL_TYPE_VARCHAR 15
#define MYSQL_TYPE_BIT 16
#define MYSQL_TYPE_JSON 245
#define MYSQL_TYPE_NEWDECIMAL 246
#define MYSQL_TYPE_ENUM 247
#define MYSQL_TYPE_SET 248
#define MYSQL_TYPE_TINY_BLOB 249
#define MYSQL_TYPE_MEDIUM_BLOB 250
#define MYSQL_TYPE_LONG_BLOB 251
#define MYSQL_TYPE_BLOB 252
#define MYSQL_TYPE_VAR_STRING 253
#define MYSQL_TYPE_STRING 254
#define MYSQL_TYPE_GEOMETRY 255

#define MYSQL_TYPE_CHAR MYSQL_TYPE_TINY
#define MYSQL_TYPE_INTERVAL MYSQL_TYPE_ENUM

#define MYSQL_COLUMN_NULL 251
#define MYSQL_COLUMN_UNSIGNED_CHAR 251
#define MYSQL_COLUMN_UNSIGNED_SHORT 252
#define MYSQL_COLUMN_UNSIGNED_INT24 253
#define MYSQL_COLUMN_UNSIGNED_INT64 254

#define MYSQL_SERVER_MORE_RESULTS_EXISTS 8

// 2**24 - 1
#define MYSQL_MAX_PACKET_LEN 16777215

#define EPOCH_TO_DAYS 719528
#define SECONDS_PER_DAY (24 * 60 * 60)

#define MYSQLSV_OPTION_TIME_TYPE_TIMEDELTA 0
#define MYSQLSV_OPTION_TIME_TYPE_TIME 1
#define MYSQLSV_OPTION_JSON_TYPE_STRING 0
#define MYSQLSV_OPTION_JSON_TYPE_OBJ 1
#define MYSQLSV_OPTION_BIT_TYPE_BYTES 0
#define MYSQLSV_OPTION_BIT_TYPE_INT 1

#define CHR2INT1(x) ((x)[1] - '0')
#define CHR2INT2(x) ((((x)[0] - '0') * 10) + ((x)[1] - '0'))
#define CHR2INT3(x) ((((x)[0] - '0') * 1e2) + (((x)[1] - '0') * 10) + ((x)[2] - '0'))
#define CHR2INT4(x) ((((x)[0] - '0') * 1e3) + (((x)[1] - '0') * 1e2) + (((x)[2] - '0') * 10) + ((x)[3] - '0'))
#define CHR2INT6(x) ((((x)[0] - '0') * 1e5) + (((x)[1] - '0') * 1e4) + (((x)[2] - '0') * 1e3) + (((x)[3] - '0') * 1e2) + (((x)[4] - '0') * 10) + (((x)[5] - '0')))

#define CHECK_DATE_STR(s, s_l) \
    ((s_l) == 10 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == '-' && \
     (((s)[5] == '1' && ((s)[6] >= '0' && (s)[6] <= '2')) || \
      ((s)[5] == '0' && ((s)[6] >= '1' && (s)[6] <= '9'))) && \
     (s)[7] == '-' && \
     ((((s)[8] >= '0' && (s)[8] <= '2') && ((s)[9] >= '0' && (s)[9] <= '9')) || \
       ((s)[8] == '3' && ((s)[9] >= '0' && (s)[9] <= '1'))) && \
       !((s)[0] == '0' && (s)[1] == '0' && (s)[2] == '0' && (s)[3] == '0') && \
       !((s)[5] == '0' && (s)[6] == '0') && \
       !((s)[8] == '0' && (s)[9] == '0'))

#define CHECK_TIME_STR(s, s_l) \
    ((s_l) == 8 && \
     ((((s)[0] >= '0' && (s)[0] <= '1') && ((s)[1] >= '0' && (s)[1] <= '9')) || \
       ((s)[0] == '2' && ((s)[1] >= '0' && (s)[1] <= '3'))) && \
     (s)[2] == ':' && \
     (((s)[3] >= '0' && (s)[3] <= '5') && ((s)[4] >= '0' && (s)[4] <= '9')) && \
     (s)[5] == ':' && \
     (((s)[6] >= '0' && (s)[6] <= '5') && ((s)[7] >= '0' && (s)[7] <= '9')))

#define CHECK_MICROSECONDS_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] >= '0' && (s)[4] <= '9' && \
     (s)[5] >= '0' && (s)[5] <= '9' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_MILLISECONDS_STR(s, s_l) \
    ((s_l) == 4 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9')

#define CHECK_MICRO_TIME_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIME_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_MILLI_TIME_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIME_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_DATETIME_STR(s, s_l) \
    ((s_l) == 19 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_TIME_STR((s)+11, 8))

#define CHECK_MICRO_DATETIME_STR(s, s_l) \
    ((s_l) == 26 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 15))

#define CHECK_MILLI_DATETIME_STR(s, s_l) \
    ((s_l) == 23 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 12))

#define CHECK_ANY_DATETIME_STR(s, s_l) \
    (((s_l) == 19 && CHECK_DATETIME_STR(s, s_l)) || \
     ((s_l) == 23 && CHECK_MILLI_DATETIME_STR(s, s_l)) || \
     ((s_l) == 26 && CHECK_MICRO_DATETIME_STR(s, s_l)))

#define DATETIME_SIZE (19)
#define DATETIME_MILLI_SIZE (23)
#define DATETIME_MICRO_SIZE (26)

#define IS_DATETIME_MILLI(s, s_l) ((s_l) == 23)
#define IS_DATETIME_MICRO(s, s_l) ((s_l) == 26)

#define CHECK_ANY_TIME_STR(s, s_l) \
    (((s_l) == 8 && CHECK_TIME_STR(s, s_l)) || \
     ((s_l) == 12 && CHECK_MILLI_TIME_STR(s, s_l)) || \
     ((s_l) == 15 && CHECK_MICRO_TIME_STR(s, s_l)))

#define TIME_SIZE (8)
#define TIME_MILLI_SIZE (12)
#define TIME_MICRO_SIZE (15)

#define IS_TIME_MILLI(s, s_l) ((s_l) == 12)
#define IS_TIME_MICRO(s, s_l) ((s_l) == 15)

#define CHECK_TIMEDELTA1_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] == ':' && \
     (s)[2] >= '0' && (s)[2] <= '5' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == ':' && \
     (s)[5] >= '0' && (s)[5] <= '5' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_TIMEDELTA1_MILLI_STR(s, s_l) \
    ((s_l) == 11 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MILLISECONDS_STR((s)+7, 4))

#define CHECK_TIMEDELTA1_MICRO_STR(s, s_l) \
    ((s_l) == 14 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MICROSECONDS_STR((s)+7, 7))

#define CHECK_TIMEDELTA2_STR(s, s_l) \
    ((s_l) == 8 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+1, 7))

#define CHECK_TIMEDELTA2_MILLI_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_TIMEDELTA2_MICRO_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_TIMEDELTA3_STR(s, s_l) \
    ((s_l) == 9 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+2, 7))

#define CHECK_TIMEDELTA3_MILLI_STR(s, s_l) \
    ((s_l) == 13 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MILLISECONDS_STR((s)+9, 4))

#define CHECK_TIMEDELTA3_MICRO_STR(s, s_l) \
    ((s_l) == 16 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MICROSECONDS_STR((s)+9, 7))

//
// 0:00:00 / 0:00:00.000 / 0:00:00.000000
// 00:00:00 / 00:00:00.000 / 00:00:00.000000
// 000:00:00 / 000:00:00.000 / 000:00:00.000000
//
#define CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (((s_l) > 0 && (s)[0] == '-') ? \
     (-1 * (_CHECK_ANY_TIMEDELTA_STR((s)+1, (s_l)-1))) : \
     (_CHECK_ANY_TIMEDELTA_STR((s), (s_l))))

#define _CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (CHECK_TIMEDELTA1_STR(s, s_l) || \
     CHECK_TIMEDELTA2_STR(s, s_l) || \
     CHECK_TIMEDELTA3_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MICRO_STR(s, s_l))

#define TIMEDELTA1_SIZE (7)
#define TIMEDELTA2_SIZE (8)
#define TIMEDELTA3_SIZE (9)
#define TIMEDELTA1_MILLI_SIZE (11)
#define TIMEDELTA2_MILLI_SIZE (12)
#define TIMEDELTA3_MILLI_SIZE (13)
#define TIMEDELTA1_MICRO_SIZE (14)
#define TIMEDELTA2_MICRO_SIZE (15)
#define TIMEDELTA3_MICRO_SIZE (16)

#define IS_TIMEDELTA1(s, s_l) ((s_l) == 7 || (s_l) == 11 || (s_l) == 14)
#define IS_TIMEDELTA2(s, s_l) ((s_l) == 8 || (s_l) == 12 || (s_l) == 15)
#define IS_TIMEDELTA3(s, s_l) ((s_l) == 9 || (s_l) == 13 || (s_l) == 16)

#define IS_TIMEDELTA_MILLI(s, s_l) ((s_l) == 11 || (s_l) == 12 || (s_l) == 13)
#define IS_TIMEDELTA_MICRO(s, s_l) ((s_l) == 14 || (s_l) == 15 || (s_l) == 16)

typedef struct {
    int output_type;
    int parse_json;
    PyObject *invalid_date_value;
    PyObject *invalid_time_value;
    PyObject *invalid_datetime_value;
} MySQLAccelOptions;

inline int IMAX(int a, int b) { return((a) > (b) ? a : b); }
inline int IMIN(int a, int b) { return((a) < (b) ? a : b); }

//
// Array
//

typedef struct {
    PyObject_HEAD
    PyObject *array_interface;
} ArrayObject;

static void Array_dealloc(ArrayObject *self) {
    // Numpy arrays take ownership of our memory. This happens because we are
    // using the Python API to create our array, rather than the numpy C API.
#if 0
    if (self->array_interface && PyDict_Check(self->array_interface)) {
        PyObject *data = PyDict_GetItemString(self->array_interface, "data");
        if (data) {
            PyObject *buffer = PyTuple_GetItem(data, 0);
            if (buffer) {
                free((char*)PyLong_AsVoidPtr(buffer));
            }
        }
    }
#endif
    Py_CLEAR(self->array_interface);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *Array_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    ArrayObject *self = (ArrayObject*)type->tp_alloc(type, 0);
    if (self) {
        self->array_interface = Py_None;
        Py_INCREF(Py_None);
    }
    return (PyObject*)self;
}

static int Array_init(ArrayObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"array_interface", NULL};
    PyObject *array_interface = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &array_interface)) {
        return -1;
    }

    if (array_interface) {
        PyObject *tmp = self->array_interface;
        Py_INCREF(array_interface);
        self->array_interface = array_interface;
        Py_DECREF(tmp);
    }

    return 0;
}

static PyObject *Array_get__array_interface__(ArrayObject *self, void *closure) {
    Py_INCREF(self->array_interface);
    return self->array_interface;
}

static PyGetSetDef Array_getsetters[] = {
    {"__array_interface__", (getter)Array_get__array_interface__,
                            (setter)NULL, "array interface", NULL},
    {NULL}
};

static PyTypeObject ArrayType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pymysqlsv.Array",
    .tp_doc = PyDoc_STR("Array manager"),
    .tp_basicsize = sizeof(ArrayObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = Array_new,
    .tp_init = (initproc)Array_init,
    .tp_dealloc = (destructor)Array_dealloc,
    .tp_getset = Array_getsetters,
};

//
// End Array
//

//
// State
//

typedef struct {
    PyObject_HEAD
    PyObject *py_conn; // Database connection
    PyObject *py_fields; // List of table fields
    PyObject *py_decimal_mod; // decimal module
    PyObject *py_decimal; // decimal.Decimal
    PyObject *py_json_mod; // json module
    PyObject *py_json_loads; // json.loads
    PyObject *py_rows; // Output object
    PyObject *py_rfile; // Socket file I/O
    PyObject *py_read; // File I/O read method
    PyObject *py_sock; // Socket
    PyObject *py_read_timeout; // Socket read timeout value
    PyObject *py_settimeout; // Socket settimeout method
    PyObject **py_converters; // List of converter functions
    PyObject **py_names; // Column names
    PyObject *py_default_converters; // Dict of default converters
    PyObject *py_numpy_mod; // Numpy module
    PyObject *py_numpy_array; // numpy.array
    PyObject *py_pandas_mod; // pandas module
    PyObject *py_pandas_dataframe; // pandas.DataFrame
    PyObject *py_array_def; // numpy array definition
    PyObject *py_array_args; // Positional args for numpy.array
    PyObject *py_array_kwds; // Keyword args for numpy.array
    PyTypeObject *namedtuple; // Generated namedtuple type
    PyObject **py_encodings; // Encoding for each column as Python string
    const char **encodings; // Encoding for each column
    unsigned long long n_cols; // Total number of columns
    unsigned long long n_rows; // Total number of rows read
    unsigned long long n_rows_in_batch; // Number of rows in current batch (fetchmany size)
    unsigned long *type_codes; // Type code for each column
    unsigned long *flags; // Column flags
    unsigned long *scales; // Column scales
    unsigned long *offsets; // Column offsets in buffer
    unsigned long long next_seq_id; // MySQL packet sequence number
    MySQLAccelOptions options; // Packet reader options
    unsigned long long df_buffer_row_size; // Size of each df buffer row in bytes
    unsigned long long df_buffer_n_rows; // Total number of rows in current df buffer
    char *df_cursor; // Current position to write to in df buffer
    char *df_buffer; // Head of df buffer
    PyStructSequence_Desc namedtuple_desc;
    int unbuffered; // Are we running in unbuffered mode?
    int is_eof; // Have we hit the eof packet yet?
} StateObject;

static void read_options(MySQLAccelOptions *options, PyObject *dict);
static unsigned long long compute_row_size(StateObject *py_state);
static void build_array_interface(StateObject *py_state);
static PyObject *build_array(StateObject *py_state);

#define DESTROY(x) do { if (x) { free(x); (x) = NULL; } } while (0)

static void State_clear_fields(StateObject *self) {
    if (!self) return;
    self->df_cursor = NULL;
    self->df_buffer = NULL;
    DESTROY(self->namedtuple_desc.fields);
    DESTROY(self->offsets);
    DESTROY(self->scales);
    DESTROY(self->flags);
    DESTROY(self->type_codes);
    DESTROY(self->encodings);
    if (self->py_converters) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_converters[i]);
        }
        DESTROY(self->py_converters);
    }
    if (self->py_names) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_names[i]);
        }
        DESTROY(self->py_names);
    }
    if (self->py_encodings) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_encodings[i]);
        }
        DESTROY(self->py_encodings);
    }
    Py_CLEAR(self->py_array_def);
    Py_CLEAR(self->py_numpy_mod);
    Py_CLEAR(self->py_numpy_array);
    Py_CLEAR(self->py_pandas_mod);
    Py_CLEAR(self->py_pandas_dataframe);
    Py_CLEAR(self->py_array_args);
    Py_CLEAR(self->py_array_kwds);
    Py_CLEAR(self->namedtuple);
    Py_CLEAR(self->py_default_converters);
    Py_CLEAR(self->py_settimeout);
    Py_CLEAR(self->py_read_timeout);
    Py_CLEAR(self->py_sock);
    Py_CLEAR(self->py_read);
    Py_CLEAR(self->py_rfile);
    Py_CLEAR(self->py_rows);
    Py_CLEAR(self->py_json_loads);
    Py_CLEAR(self->py_json_mod);
    Py_CLEAR(self->py_decimal);
    Py_CLEAR(self->py_decimal_mod);
    Py_CLEAR(self->py_fields);
    Py_CLEAR(self->py_conn);
}

static void State_dealloc(StateObject *self) {
    State_clear_fields(self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *State_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    StateObject *self = (StateObject*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int State_init(StateObject *self, PyObject *args, PyObject *kwds) {
    int rc = 0;
    PyObject *py_res = NULL;
    PyObject *py_converters = NULL;
    PyObject *py_options = NULL;
    unsigned long long requested_n_rows = 0;

    if (!PyArg_ParseTuple(args, "OK", &py_res, &requested_n_rows)) {
        return -1;
    }

    py_options = PyObject_GetAttrString(py_res, "options");
    if (!py_options) {
        Py_INCREF(Py_None);
        py_options = Py_None;
    }

    if (PyDict_Check(py_options)) {
        self->py_default_converters = PyDict_GetItemString(py_options, "default_converters");
        if (self->py_default_converters && !PyDict_Check(self->py_default_converters)) {
            self->py_default_converters = NULL;
        }
        Py_XINCREF(self->py_default_converters);
        PyObject *py_unbuffered = PyDict_GetItemString(py_options, "unbuffered");
        if (py_unbuffered && PyObject_IsTrue(py_unbuffered)) {
            self->unbuffered = 1;
        }
    }

    if (self->unbuffered) {
        PyObject *unbuffered_active = PyObject_GetAttrString(py_res, "unbuffered_active");
        if (!unbuffered_active || !PyObject_IsTrue(unbuffered_active)) {
            Py_XDECREF(unbuffered_active);
            goto error;
        }
        Py_XDECREF(unbuffered_active);
    }

    // Import decimal module.
    self->py_decimal_mod = PyImport_ImportModule("decimal");
    if (!self->py_decimal_mod) goto error;
    self->py_decimal = PyObject_GetAttrString(self->py_decimal_mod, "Decimal");
    if (!self->py_decimal) goto error;

    // Import json module.
    self->py_json_mod = PyImport_ImportModule("json");
    if (!self->py_json_mod) goto error;
    self->py_json_loads = PyObject_GetAttrString(self->py_json_mod, "loads");
    if (!self->py_json_loads) goto error;

    // Retrieve type codes for each column.
    PyObject *py_field_count = PyObject_GetAttrString(py_res, "field_count");
    if (!py_field_count) goto error;
    self->n_cols = PyLong_AsUnsignedLong(py_field_count);
    Py_XDECREF(py_field_count);

    py_converters = PyObject_GetAttrString(py_res, "converters");
    if (!py_converters) goto error;

    self->py_converters = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_converters) goto error;

    self->type_codes = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->type_codes) goto error;

    self->flags = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->flags) goto error;

    self->scales = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->scales) goto error;

    self->encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->encodings) goto error;

    self->py_encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->py_encodings) goto error;

    self->py_names = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_names) goto error;

    self->py_fields = PyObject_GetAttrString(py_res, "fields");
    if (!self->py_fields) goto error;

    for (unsigned long i = 0; i < self->n_cols; i++) {
        // Get type codes.
        PyObject *py_field = PyList_GetItem(self->py_fields, i);
        if (!py_field) goto error;

        PyObject *py_flags = PyObject_GetAttrString(py_field, "flags");
        if (!py_flags) goto error;
        self->flags[i] = PyLong_AsUnsignedLong(py_flags);
        Py_XDECREF(py_flags);

        PyObject *py_scale = PyObject_GetAttrString(py_field, "scale");
        if (!py_scale) goto error;
        self->scales[i] = PyLong_AsUnsignedLong(py_scale);
        Py_XDECREF(py_scale);

        PyObject *py_field_type = PyObject_GetAttrString(py_field, "type_code");
        if (!py_field_type) goto error;
        self->type_codes[i] = PyLong_AsUnsignedLong(py_field_type);
        PyObject *py_default_converter = (self->py_default_converters) ?
                      PyDict_GetItem(self->py_default_converters, py_field_type) : NULL;
        Py_XDECREF(py_field_type);

        // Get field name.
        PyObject *py_field_name = PyObject_GetAttrString(py_field, "name");
        if (!py_field_name) goto error;
        self->py_names[i] = py_field_name;

        // Get field encodings (NULL means binary) and default converters.
        PyObject *py_tmp = PyList_GetItem(py_converters, i);
        if (!py_tmp) goto error;
        PyObject *py_encoding = PyTuple_GetItem(py_tmp, 0);
        if (!py_encoding) goto error;
        PyObject *py_converter = PyTuple_GetItem(py_tmp, 1);
        if (!py_converter) goto error;

        self->py_encodings[i] = (py_encoding == Py_None) ? NULL : py_encoding;
        Py_XINCREF(self->py_encodings[i]);

        self->encodings[i] = (!py_encoding || py_encoding == Py_None) ?
                              NULL : PyUnicode_AsUTF8AndSize(py_encoding, NULL);

        self->py_converters[i] = (!py_converter
                                  || py_converter == Py_None
                                  || py_converter == py_default_converter) ?
                                 NULL : py_converter;
        Py_XINCREF(self->py_converters[i]);
    }

    // Loop over all data packets.
    self->py_conn = PyObject_GetAttrString(py_res, "connection");
    if (!self->py_conn) goto error;

    // Cache socket timeout and read methods.
    self->py_sock = PyObject_GetAttrString(self->py_conn, "_sock");
    if (!self->py_sock) goto error;
    self->py_settimeout = PyObject_GetAttrString(self->py_sock, "settimeout");
    if (!self->py_settimeout) goto error;
    self->py_read_timeout = PyObject_GetAttrString(self->py_conn, "_read_timeout");
    if (!self->py_read_timeout) goto error;

    self->py_rfile = PyObject_GetAttrString(self->py_conn, "_rfile");
    if (!self->py_rfile) goto error;
    self->py_read = PyObject_GetAttrString(self->py_rfile, "read");
    if (!self->py_read) goto error;

    PyObject *py_next_seq_id = PyObject_GetAttrString(self->py_conn, "_next_seq_id");
    if (!py_next_seq_id) goto error;
    self->next_seq_id = PyLong_AsUnsignedLongLong(py_next_seq_id);
    Py_XDECREF(py_next_seq_id);

    if (py_options && PyDict_Check(py_options)) {
        read_options(&self->options, py_options);
    }

    switch (self->options.output_type) {
    case MYSQLSV_OUT_PANDAS:
        // Import pandas module.
        self->py_pandas_mod = PyImport_ImportModule("pandas");
        if (!self->py_pandas_mod) goto error;
        self->py_pandas_dataframe = PyObject_GetAttrString(self->py_pandas_mod, "DataFrame");
        if (!self->py_pandas_dataframe) goto error;

        // Fall through

    case MYSQLSV_OUT_NUMPY:
        // Import numpy module.
        self->py_numpy_mod = PyImport_ImportModule("numpy");
        if (!self->py_numpy_mod) goto error;
        self->py_numpy_array = PyObject_GetAttrString(self->py_numpy_mod, "array");
        if (!self->py_numpy_array) goto error;

        // Build array interface arguments.
        build_array_interface(self);
        if (!self->py_array_def || !self->py_array_args || !self->py_array_kwds) goto error;

        // Setup dataframe buffer.
        self->df_buffer_row_size = compute_row_size(self);
        if (requested_n_rows) {
            self->df_buffer_n_rows = requested_n_rows;
        } else if (self->unbuffered) {
            self->df_buffer_n_rows = 1;
        } else if (self->df_buffer_row_size > 10e6) {
            self->df_buffer_n_rows = 1;
        } else {
            self->df_buffer_n_rows = 10e6 / self->df_buffer_row_size;
        }
        self->df_buffer = calloc(self->df_buffer_row_size, self->df_buffer_n_rows);
        if (!self->df_buffer) goto error;
        self->df_cursor = self->df_buffer;

        // Construct the array to use for every fetch (it's reused each time).
        self->n_rows_in_batch = self->df_buffer_n_rows;
        self->py_rows = build_array(self);
        self->n_rows_in_batch = 0;
        if (!self->py_rows) goto error;

        PyObject_SetAttrString(py_res, "rows", self->py_rows);
        break;

    case MYSQLSV_OUT_NAMEDTUPLES:
        self->namedtuple_desc.name = "Row";
        self->namedtuple_desc.doc = "Row of data values";
        self->namedtuple_desc.n_in_sequence = self->n_cols;
        self->namedtuple_desc.fields = calloc(self->n_cols + 1, sizeof(PyStructSequence_Field));
        if (!self->namedtuple_desc.fields) goto error;
        for (unsigned long i = 0; i < self->n_cols; i++) {
            self->namedtuple_desc.fields[i].name = PyUnicode_AsUTF8AndSize(self->py_names[i], NULL);
            self->namedtuple_desc.fields[i].doc = NULL;
        }
        self->namedtuple = PyStructSequence_NewType(&self->namedtuple_desc);
        if (!self->namedtuple) goto error;

        // Fall through

    default:
        self->py_rows = PyList_New(0);
        if (!self->py_rows) goto error;

        PyObject_SetAttrString(py_res, "rows", self->py_rows);
    }

exit:
    Py_XDECREF(py_converters);
    Py_XDECREF(py_options);
    return rc;

error:
    State_clear_fields(self);
    rc = -1;
    goto exit;
}

static int State_reset_batch(StateObject *self, PyObject *py_res) {
    int rc = 0;
    PyObject *py_tmp = NULL;

    self->n_rows_in_batch = 0;

    switch (self->options.output_type) {
    case MYSQLSV_OUT_PANDAS:
    case MYSQLSV_OUT_NUMPY:
        break;
    default:
        py_tmp = self->py_rows;
        self->py_rows = PyList_New(0);
        Py_XDECREF(py_tmp);
        if (!self->py_rows) { rc = -1; goto error; }
        rc = PyObject_SetAttrString(py_res, "rows", self->py_rows);
    }

exit:
    return rc;

error:
    goto exit;
}

static PyTypeObject StateType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pymysqlsv.State",
    .tp_doc = PyDoc_STR("Rowdata state manager"),
    .tp_basicsize = sizeof(StateObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = State_new,
    .tp_init = (initproc)State_init,
    .tp_dealloc = (destructor)State_dealloc,
};

//
// End State
//

static void read_options(MySQLAccelOptions *options, PyObject *dict) {
    if (!options || !dict) return;

    PyObject *key = NULL;
    PyObject *value = NULL;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &value)) {
        if (PyUnicode_CompareWithASCIIString(key, "output_type") == 0) {
            if (PyUnicode_CompareWithASCIIString(value, "dict") == 0 ||
                PyUnicode_CompareWithASCIIString(value, "dicts") == 0 ) {
                options->output_type = MYSQLSV_OUT_DICTS;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "namedtuple") == 0 ||
                     PyUnicode_CompareWithASCIIString(value, "namedtuples") == 0) {
                options->output_type = MYSQLSV_OUT_NAMEDTUPLES;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "numpy") == 0) {
                options->output_type = MYSQLSV_OUT_NUMPY;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "pandas") == 0) {
                options->output_type = MYSQLSV_OUT_PANDAS;
            }
            else {
                options->output_type = MYSQLSV_OUT_TUPLES;
            }
        } else if (PyUnicode_CompareWithASCIIString(key, "parse_json") == 0) {
            options->parse_json = PyObject_IsTrue(value);
        } else if (PyUnicode_CompareWithASCIIString(key, "invalid_date_value") == 0) {
            options->invalid_date_value = value;
        } else if (PyUnicode_CompareWithASCIIString(key, "invalid_time_value") == 0) {
            options->invalid_time_value = value;
        } else if (PyUnicode_CompareWithASCIIString(key, "invalid_datetime_value") == 0) {
            options->invalid_datetime_value = value;
        }
    }
}

// mysql, for whatever reason, treats 0 as an actual year, but not
// a leap year
//
static int is_leap_year(int year)
{
    return (year % 4) == 0 && year != 0 && ((year % 100) != 0 || (year % 400) == 0);
}

static int days_in_previous_months(int month, int year)
{
    static const int previous_days[13] =
        {
            -31,
            0,
            31,
            31 + 28,
            31 + 28 + 31,
            31 + 28 + 31 + 30,
            31 + 28 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30,
            31 + 28 + 31 + 30 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
        };
    return previous_days[month] + (month > 2 && is_leap_year(year));
}

// NOTE: year 0 does not actually exist, but mysql pretends it does (and is NOT
// a leap year)
//
static int leap_years_before(int year)
{
    return (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400;
}

static int days_in_previous_years(int year)
{
    return 365 * year + leap_years_before(year);
}

static int64_t to_days(int year, int month, int day) {
    return days_in_previous_years(year) + days_in_previous_months(month, year) + day;
}

static void raise_exception(
    PyObject *self,
    char *err_type,
    unsigned long long err_code,
    char *err_str
) {
    PyObject *py_exc = NULL;
    PyObject *py_val = NULL;

    py_exc = PyObject_GetAttrString(self, err_type);
    if (!py_exc) goto error;

    py_val = Py_BuildValue("(Ks)", err_code, err_str);
    if (!py_val) goto error;

    PyErr_SetObject(py_exc, py_val);

exit:
    if (py_exc) { Py_DECREF(py_exc); }
    if (py_val) { Py_DECREF(py_val); }
    return;

error:
    goto exit;
}

static int is_error_packet(char *buff_bytes) {
    return buff_bytes && *(uint8_t*)buff_bytes == 0xFF;
}

static void force_close(PyObject *py_conn) {
    PyObject *py_sock = NULL;

    py_sock = PyObject_GetAttrString(py_conn, "_sock");
    if (!py_sock) goto error;

    Py_XDECREF(PyObject_CallMethod(py_sock, "close", NULL));
    PyErr_Clear();

    PyObject_SetAttrString(py_conn, "_sock", Py_None);
    PyObject_SetAttrString(py_conn, "_rfile", Py_None);

exit:
    Py_XDECREF(py_sock);
    return;

error:
    goto exit;
}

static PyObject *read_bytes(StateObject *py_state, unsigned long long num_bytes) {
    PyObject *py_num_bytes = NULL;
    PyObject *py_data = NULL;
    PyObject *py_exc = NULL;

    if (py_state->py_read_timeout && py_state->py_read_timeout != Py_None) {
        Py_XDECREF(PyObject_CallFunctionObjArgs(py_state->py_settimeout,
                                                py_state->py_read_timeout, NULL));
        if (PyErr_Occurred()) goto error;
    }

    py_num_bytes = PyLong_FromUnsignedLongLong(num_bytes);
    if (!py_num_bytes) goto error;

    while (1) {
        py_data = PyObject_CallFunctionObjArgs(py_state->py_read, py_num_bytes, NULL);

        if ((py_exc = PyErr_Occurred())) {
            if (PyErr_ExceptionMatches(PyExc_IOError) || PyErr_ExceptionMatches(PyExc_OSError)) {
                PyObject *py_errno = PyObject_GetAttrString(py_exc, "errno");
                if (!py_errno) goto error;

                unsigned long long err = PyLong_AsUnsignedLongLong(py_errno);
                Py_DECREF(py_errno);

                if (err == 4 /* errno.EINTER */) {
                    continue;
                }

                force_close(py_state->py_conn);
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to MySQL server during query");
                goto error;
            }
            else if (PyErr_ExceptionMatches(PyExc_BaseException)) {
                // Don't convert unknown exception to MySQLError.
                force_close(py_state->py_conn);
                goto error;
            }
        }

        if (py_data) {
            break;
        }
    }

    if (PyBytes_GET_SIZE(py_data) < (long int)num_bytes) {
        force_close(py_state->py_conn);
        raise_exception(py_state->py_conn, "OperationalError", 0,
                        "Lost connection to MySQL server during query");
        goto error;
    }

exit:
    Py_XDECREF(py_num_bytes);
    return py_data;

error:
    Py_CLEAR(py_data);
    goto exit;
}

static PyObject *read_packet(StateObject *py_state) {
    PyObject *py_buff = NULL;
    PyObject *py_new_buff = NULL;
    PyObject *py_packet_header = NULL;
    PyObject *py_bytes_to_read = NULL;
    PyObject *py_recv_data = NULL;
    unsigned long long bytes_to_read = 0;
    char *buff = NULL;
    uint64_t btrl = 0;
    uint8_t btrh = 0;
    uint8_t packet_number = 0;

    py_buff = PyByteArray_FromStringAndSize(NULL, 0);
    if (!py_buff) goto error;

    while (1) {
        py_packet_header = read_bytes(py_state, 4);
        if (!py_packet_header) goto error;

        buff = PyBytes_AsString(py_packet_header);

        btrl = *(uint16_t*)buff;
        btrh = *(uint8_t*)(buff+2);
        packet_number = *(uint8_t*)(buff+3);
        bytes_to_read = btrl + (btrh << 16);

        Py_CLEAR(py_packet_header);

        if (packet_number != py_state->next_seq_id) {
            force_close(py_state->py_conn);
            if (packet_number == 0) {
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to MySQL server during query");

                goto error;
            }
            raise_exception(py_state->py_conn, "InternalError", 0,
                            "Packet sequence number wrong");
            goto error;
        }

        py_state->next_seq_id = (py_state->next_seq_id + 1) % 256;

        py_recv_data = read_bytes(py_state, bytes_to_read);
        if (!py_recv_data) goto error;

        py_new_buff = PyByteArray_Concat(py_buff, py_recv_data);
        Py_CLEAR(py_recv_data);
        Py_CLEAR(py_buff);
        if (!py_new_buff) goto error;

        py_buff = py_new_buff;
        py_new_buff = NULL;

        if (bytes_to_read == 0xFFFFFF) {
            continue;
        }

        if (bytes_to_read < MYSQL_MAX_PACKET_LEN) {
            break;
        }
    }

    if (is_error_packet(PyByteArray_AsString(py_buff))) {
        PyObject *py_result = PyObject_GetAttrString(py_state->py_conn, "_result");
        if (py_result && py_result != Py_None) {
            PyObject *py_unbuffered_active = PyObject_GetAttrString(py_result, "unbuffered_active");
            if (py_unbuffered_active == Py_True) {
                PyObject_SetAttrString(py_result, "unbuffered_active", Py_False);
            }
            Py_XDECREF(py_unbuffered_active);
        }
        Py_XDECREF(py_result);
        Py_XDECREF(PyObject_CallMethod(py_state->py_conn, "_raise_mysql_exception",
                                       "O", py_buff, NULL));
    }

exit:
    Py_XDECREF(py_new_buff);
    Py_XDECREF(py_bytes_to_read);
    Py_XDECREF(py_recv_data);
    Py_XDECREF(py_packet_header);
    return py_buff;

error:
    Py_CLEAR(py_buff);
    goto exit;
}

static int is_eof_packet(char *data, unsigned long long data_l) {
    return data && (uint8_t)*(uint8_t*)data == 0xFE && data_l < 9;
}

static int check_packet_is_eof(
    char **data,
    unsigned long long *data_l,
    unsigned long long *warning_count,
    int *has_next
) {
    uint16_t server_status = 0;
    if (!data || !data_l) {
        return 0;
        if (has_next) *has_next = 0;
        if (warning_count) *warning_count = 0;
    }
    if (!is_eof_packet(*data, *data_l)) {
        return 0;
    }
    *data += 1; *data_l -= 1;
    if (warning_count) *warning_count = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    server_status = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    if (has_next) *has_next = server_status & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    return 1;
}

static unsigned long long read_length_encoded_integer(
    char **data,
    unsigned long long *data_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || *data_l == 0) {
        if (is_null) *is_null = 1;
        return 0;
    }

    uint8_t c = **(uint8_t**)data;
    *data += 1; *data_l -= 1;

    if (c == MYSQL_COLUMN_NULL) {
        if (is_null) *is_null = 1;
        return 0;
    }

    if (c < MYSQL_COLUMN_UNSIGNED_CHAR) {
        return c;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_SHORT) {
        if (*data_l < 2) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t out = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return out;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT24) {
        if (*data_l < 3) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t low = **(uint8_t**)data;
        *data += 1; *data_l -= 1;
        uint16_t high = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return low + (high << 16);
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT64) {
        if (*data_l < 8) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint64_t out = **(uint64_t**)data;
        *data += 8; *data_l -= 8;
        return out;
    }

    if (is_null) *is_null = 1;
    return 0;
}

static void read_length_coded_string(
    char **data,
    unsigned long long *data_l,
    char **out,
    unsigned long long *out_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || !out || !out_l) {
        if (is_null) *is_null = 1;
        return;
    }

    unsigned long long length = read_length_encoded_integer(data, data_l, is_null);

    if (is_null && *is_null) {
        return;
    }

    length = (length > *data_l) ? *data_l : length;

    *out = *data;
    *out_l = length;

    *data += length;
    *data_l -= length;

    return;
}

static void build_array_interface(StateObject *py_state) {
    PyObject *py_out = NULL;
    PyObject *py_typestr = NULL;
    PyObject *py_descr = NULL;
    PyObject *py_descr_item = NULL;
    PyObject *py_type = NULL;
    PyObject *py_array = NULL;

    py_out = PyDict_New();
    if (!py_out) goto error;

    py_typestr = PyUnicode_FromFormat("|V%llu", py_state->df_buffer_row_size);
    if (!py_typestr) goto error;
    PyDict_SetItemString(py_out, "typestr", py_typestr);
    Py_DECREF(py_typestr);

    py_descr = PyList_New(py_state->n_cols);
    if (!py_descr) goto error;
    PyDict_SetItemString(py_out, "descr", py_descr);
    Py_DECREF(py_descr);

    for (unsigned long i = 0; i < py_state->n_cols; i++) {
        py_descr_item = PyTuple_New(2);
        if (!py_descr_item) goto error;

        PyList_SetItem(py_descr, i, py_descr_item);

        PyTuple_SetItem(py_descr_item, 0, py_state->py_names[i]);
        Py_INCREF(py_state->py_names[i]);

        switch (py_state->type_codes[i]) {
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
            py_type = PyUnicode_FromString("|O");
            break;

        case MYSQL_TYPE_TINY:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                py_type = PyUnicode_FromString("<u1");
            } else {
                py_type = PyUnicode_FromString("<i1");
            }
            break;

        case MYSQL_TYPE_SHORT:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                py_type = PyUnicode_FromString("<u2");
            } else {
                py_type = PyUnicode_FromString("<i2");
            }
            break;

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                py_type = PyUnicode_FromString("<u4");
            } else {
                py_type = PyUnicode_FromString("<i4");
            }
            break;

        case MYSQL_TYPE_LONGLONG:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                py_type = PyUnicode_FromString("<u8");
            } else {
                py_type = PyUnicode_FromString("<i8");
            }
            break;

        case MYSQL_TYPE_FLOAT:
            py_type = PyUnicode_FromString("<f4");
            break;

        case MYSQL_TYPE_DOUBLE:
            py_type = PyUnicode_FromString("<f8");
            break;

        case MYSQL_TYPE_NULL:
            py_type = PyUnicode_FromString("<u8");
            break;

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            py_type = PyUnicode_FromString("<datetime64[ns]");
            break;

        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            py_type = PyUnicode_FromString("<datetime64[ns]");
            break;

        case MYSQL_TYPE_TIME:
            py_type = PyUnicode_FromString("<timedelta64[ns]");
            break;

        case MYSQL_TYPE_YEAR:
            py_type = PyUnicode_FromString("<u2");
            break;

        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            py_type = PyUnicode_FromString("|O");
            break;

        default:
            PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                         py_state->type_codes[i], NULL);
            goto error;
        }

        if (!py_type) goto error;

        PyTuple_SetItem(py_descr_item, 1, py_type);

        py_descr_item = NULL;
    }

    py_state->py_array_def = py_out;

    py_state->py_array_args = PyTuple_New(1);
    if (!py_state->py_array_args) goto error;
    PyTuple_SetItem(py_state->py_array_args, 0, py_array);
    py_state->py_array_kwds = PyDict_New();
    PyDict_SetItemString(py_state->py_array_kwds, "copy", Py_False);

exit:
    return;

error:
    Py_CLEAR(py_state->py_array_def);
    Py_CLEAR(py_state->py_array_args);
    Py_CLEAR(py_state->py_array_kwds);
    goto exit;
}

static PyObject *build_array(StateObject *py_state) {
    PyObject *py_out = NULL;
    PyObject *py_data = NULL;
    PyObject *py_array = NULL;
    PyObject *py_array_def = NULL;
    PyObject *py_args = NULL;

    py_data = PyTuple_New(2);
    if (!py_data) goto error;

    PyTuple_SetItem(py_data, 0, PyLong_FromVoidPtr(py_state->df_buffer));
    PyTuple_SetItem(py_data, 1, Py_False);
    Py_INCREF(Py_False);

    py_array_def = PyDict_Copy(py_state->py_array_def);
    if (!py_array_def) goto error;

    PyObject *py_shape = PyTuple_New(1);
    if (!py_shape) goto error;
    PyTuple_SetItem(py_shape, 0, PyLong_FromUnsignedLongLong(py_state->n_rows_in_batch));
    PyDict_SetItemString(py_array_def, "shape", py_shape);
    Py_CLEAR(py_shape);

    PyDict_SetItemString(py_array_def, "data", py_data);
    Py_CLEAR(py_data);

    py_args = PyTuple_New(1);
    if (!py_args) goto error;
    PyTuple_SetItem(py_args, 0, py_array_def);
    py_array_def = NULL;
    py_array = Array_new(&ArrayType, py_args, NULL);
    if (!py_array) goto error;
    Array_init((ArrayObject*)py_array, py_args, NULL);

    PyTuple_SetItem(py_state->py_array_args, 0, py_array);

    py_out = PyObject_Call(py_state->py_numpy_array, py_state->py_array_args,
                           py_state->py_array_kwds);
    if (!py_out) goto error;

    if (py_state->options.output_type == MYSQLSV_OUT_PANDAS) {
        PyObject *py_tmp = py_out;
        py_out = PyObject_CallFunctionObjArgs(py_state->py_pandas_dataframe, py_out, NULL);
        if (!py_out) goto error;
        Py_DECREF(py_tmp);
    }

exit:
    Py_XDECREF(py_args);
    return py_out;

error:
    Py_CLEAR(py_out);
    goto exit;
}

static unsigned long long compute_row_size(StateObject *py_state) {
    unsigned long long row_size = 0;

    for (unsigned long i = 0; i < py_state->n_cols; i++) {
        switch (py_state->type_codes[i]) {
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
            row_size += sizeof(PyObject*);
            break;

        case MYSQL_TYPE_TINY:
            row_size += sizeof(int8_t);
            break;

        case MYSQL_TYPE_SHORT:
            row_size += sizeof(int16_t);
            break;

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            row_size += sizeof(int32_t);
            break;

        case MYSQL_TYPE_LONGLONG:
            row_size += sizeof(int64_t);
            break;

        case MYSQL_TYPE_FLOAT:
            row_size += sizeof(float);
            break;

        case MYSQL_TYPE_DOUBLE:
            row_size += sizeof(double);
            break;

        case MYSQL_TYPE_NULL:
            row_size += sizeof(PyObject*);
            break;

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            row_size += sizeof(int64_t);
            break;

        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            row_size += sizeof(int64_t);
            break;

        case MYSQL_TYPE_TIME:
            row_size += sizeof(int64_t);
            break;

        case MYSQL_TYPE_YEAR:
            row_size += sizeof(int16_t);
            break;

        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            row_size += sizeof(PyObject*);
            break;

        default:
            PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                         py_state->type_codes[i], NULL);
            return 0;
        }
    }

    return row_size;
}

static void read_dataframe_row_from_packet(
    StateObject *py_state,
    char *data,
    unsigned long long data_l
) {
    char *out = NULL;
    unsigned long long out_l = 0;
    int is_null = 0;
    PyObject *item = NULL;
    PyObject *str = NULL;
    char *end = NULL;
    char *loc = py_state->df_cursor;

    int sign = 1;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    float float_nan = nanf("");
    double double_nan = nan("");

    if (!py_state->df_cursor) goto error;

    for (unsigned long i = 0; i < py_state->n_cols; i++) {

        read_length_coded_string(&data, &data_l, &out, &out_l, &is_null);
        end = &out[out_l];

        switch (py_state->type_codes[i]) {
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
            if (is_null) {
                *(PyObject**)loc = Py_None;
                Py_INCREF(Py_None);
            } else {
                str = NULL;
                str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                if (!str) goto error;

                item = PyObject_CallFunctionObjArgs(py_state->py_decimal, str, NULL);
                Py_DECREF(str); str = NULL;
                if (!item) goto error;

                // Free previous value if we are reusing a buffer.
                Py_XDECREF(*(PyObject**)loc);

                *(PyObject**)loc = item;
            }
            loc += sizeof(PyObject*);
            break;

        case MYSQL_TYPE_TINY:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *(uint8_t*)loc = (is_null) ? 0 : (uint8_t)strtoul(out, &end, 10);
            } else {
                *(int8_t*)loc = (is_null) ? INT8_MIN : (int8_t)strtol(out, &end, 10);
            }
            loc += sizeof(int8_t);
            break;

        case MYSQL_TYPE_SHORT:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *(uint16_t*)loc = (is_null) ? 0 : (uint16_t)strtoul(out, &end, 10);
            } else {
                *(int16_t*)loc = (is_null) ? INT16_MIN : (int16_t)strtol(out, &end, 10);
            }
            loc += sizeof(int16_t);
            break;

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *(uint32_t*)loc = (is_null) ? 0 : (uint32_t)strtoul(out, &end, 10);
            } else {
                *(int32_t*)loc = (is_null) ? INT32_MIN : (int32_t)strtol(out, &end, 10);
            }
            loc += sizeof(int32_t);
            break;

        case MYSQL_TYPE_LONGLONG:
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *(uint64_t*)loc = (is_null) ? 0 : (uint64_t)strtoul(out, &end, 10);
            } else {
                *(int64_t*)loc = (is_null) ? INT64_MIN : (int64_t)strtol(out, &end, 10);
            }
            loc += sizeof(int64_t);
            break;

        case MYSQL_TYPE_FLOAT:
            if (is_null) {
                *(float*)loc = (float)float_nan;
            } else {
                *(float*)loc = (float)strtod(out, &end);
            }
            loc += sizeof(float);
            break;

        case MYSQL_TYPE_DOUBLE:
            if (is_null) {
                *(double*)loc = (double)double_nan;
            } else {
                *(double*)loc = (double)strtod(out, &end);
            }
            loc += sizeof(double);
            break;

        case MYSQL_TYPE_NULL:
            *(PyObject**)loc = Py_None;
            loc += sizeof(PyObject*);
            break;

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            // TODO: Should use numpy's NaT
            if (!CHECK_ANY_DATETIME_STR(out, out_l)) {
                *(int64_t*)loc = (int64_t)(INT64_MIN);
                loc += sizeof(int64_t);
                break;
            }
            year = CHR2INT4(out); out += 5;
            month = CHR2INT2(out); out += 3;
            day = CHR2INT2(out); out += 3;
            hour = CHR2INT2(out); out += 3;
            minute = CHR2INT2(out); out += 3;
            second = CHR2INT2(out); out += 3;
            microsecond = (IS_DATETIME_MICRO(out, out_l)) ? CHR2INT6(out) :
                          (IS_DATETIME_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            *(int64_t*)loc = (int64_t)(((to_days(year, month, day) - EPOCH_TO_DAYS)
                                * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second)
                                * 1e9 + microsecond * 1e3);
            loc += sizeof(int64_t);
            break;

        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            if (!CHECK_DATE_STR(out, out_l)) {
                *(int64_t*)loc = (int64_t)(INT64_MIN);
                loc += sizeof(int64_t);
                break;
            }
            year = CHR2INT4(out); out += 5;
            month = CHR2INT2(out); out += 3;
            day = CHR2INT2(out); out += 3;
            *(int64_t*)loc = (int64_t)((to_days(year, month, day) - EPOCH_TO_DAYS)
                                        * SECONDS_PER_DAY * 1e9);
            loc += sizeof(int64_t);
            break;

        case MYSQL_TYPE_TIME:
            sign = CHECK_ANY_TIMEDELTA_STR(out, out_l);
            if (!sign) {
                *(int64_t*)loc = (int64_t)(INT64_MIN);
                loc += sizeof(int64_t);
                break;
            } else if (sign < 0) {
                out += 1; out_l -= 1;
            }
            if (IS_TIMEDELTA1(out, out_l)) {
                hour = CHR2INT1(out); out += 2;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            else if (IS_TIMEDELTA2(out, out_l)) {
                hour = CHR2INT2(out); out += 3;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            else if (IS_TIMEDELTA3(out, out_l)) {
                hour = CHR2INT3(out); out += 4;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            *(int64_t*)loc = (int64_t)((hour * 3600 + minute * 60 + second)
                                       * 1e9 + microsecond * 1e3) * sign;
            loc += sizeof(int64_t);
            break;

        case MYSQL_TYPE_YEAR:
            if (out_l == 0) {
                *(uint16_t*)loc = 0;
                loc += sizeof(uint16_t);
                break;
            }
            end = &out[out_l];
            *(uint16_t*)loc = (uint16_t)strtoul(out, &end, 10);
            loc += sizeof(uint16_t);
            break;

        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            if (py_state->encodings[i] == NULL) {
                item = PyBytes_FromStringAndSize(out, out_l);
                if (!item) goto error;
                break;
            }

            item = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
            if (!item) goto error;

            // Parse JSON string.
            if (py_state->type_codes[i] == MYSQL_TYPE_JSON && py_state->options.parse_json) {
                str = item;
                item = PyObject_CallFunctionObjArgs(py_state->py_json_loads, str, NULL);
                Py_DECREF(str); str = NULL;
                if (!item) goto error;
            }

            // Free previous value if we are reusing a buffer.
            Py_XDECREF(*(PyObject**)loc);

            *(PyObject**)loc = item;
            loc += sizeof(PyObject*);

            break;

        default:
            PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                         py_state->type_codes[i], NULL);
            goto error;
        }
    }

exit:
    return;

error:
    goto exit;
}

static PyObject *read_obj_row_from_packet(
    StateObject *py_state,
    char *data,
    unsigned long long data_l
) {
    char *out = NULL;
    char *orig_out = NULL;
    unsigned long long out_l = 0;
    unsigned long long orig_out_l = 0;
    int is_null = 0;
    PyObject *py_result = NULL;
    PyObject *py_item = NULL;
    PyObject *py_str = NULL;
    char *end = NULL;

    int sign = 1;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    switch (py_state->options.output_type) {
    case MYSQLSV_OUT_NAMEDTUPLES: {
        if (!py_state->namedtuple) goto error;
        py_result = PyStructSequence_New(py_state->namedtuple);
        break;
        }
    case MYSQLSV_OUT_DICTS:
        py_result = PyDict_New();
        break;
    default:
        py_result = PyTuple_New(py_state->n_cols);
    }

    for (unsigned long i = 0; i < py_state->n_cols; i++) {

        read_length_coded_string(&data, &data_l, &out, &out_l, &is_null);
        end = &out[out_l];

        orig_out = out;
        orig_out_l = out_l;

        py_item = Py_None;

        // Don't convert if it's a NULL.
        if (!is_null) {

            // If a converter was passed in, use it.
            if (py_state->py_converters[i]) {
                py_str = NULL;
                if (py_state->encodings[i] == NULL) {
                    py_str = PyBytes_FromStringAndSize(out, out_l);
                    if (!py_str) goto error;
                } else {
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_str) goto error;
                }
                py_item = PyObject_CallFunctionObjArgs(py_state->py_converters[i], py_str, NULL);
                Py_CLEAR(py_str);
                if (!py_item) goto error;
            }

            // If no converter was passed in, do the default processing.
            else {
                switch (py_state->type_codes[i]) {
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_DECIMAL:
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_str) goto error;

                    py_item = PyObject_CallFunctionObjArgs(py_state->py_decimal, py_str, NULL);
                    Py_CLEAR(py_str);
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_INT24:
                    if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                        py_item = PyLong_FromUnsignedLongLong(strtoul(out, &end, 10));
                    } else {
                        py_item = PyLong_FromLongLong(strtol(out, &end, 10));
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                    py_item = PyFloat_FromDouble(strtod(out, &end));
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NULL:
                    py_item = Py_None;
                    break;

                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_TIMESTAMP:
                    if (!CHECK_ANY_DATETIME_STR(out, out_l)) {
                        if (py_state->options.invalid_datetime_value) {
                            py_item = py_state->options.invalid_datetime_value;
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    hour = CHR2INT2(out); out += 3;
                    minute = CHR2INT2(out); out += 3;
                    second = CHR2INT2(out); out += 3;
                    microsecond = (IS_DATETIME_MICRO(out, out_l)) ? CHR2INT6(out) :
                                  (IS_DATETIME_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    py_item = PyDateTime_FromDateAndTime(year, month, day,
                                                      hour, minute, second, microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_DATE:
                    if (!CHECK_DATE_STR(out, out_l)) {
                        if (py_state->options.invalid_date_value) {
                            py_item = py_state->options.invalid_date_value;
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    py_item = PyDate_FromDate(year, month, day);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TIME:
                    sign = CHECK_ANY_TIMEDELTA_STR(out, out_l);
                    if (!sign) {
                        if (py_state->options.invalid_time_value) {
                            py_item = py_state->options.invalid_time_value;
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    } else if (sign < 0) {
                        out += 1; out_l -= 1;
                    }
                    if (IS_TIMEDELTA1(out, out_l)) {
                        hour = CHR2INT1(out); out += 2;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA2(out, out_l)) {
                        hour = CHR2INT2(out); out += 3;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA3(out, out_l)) {
                        hour = CHR2INT3(out); out += 4;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    py_item = PyDelta_FromDSU(0, sign * hour * 60 * 60 +
                                              sign * minute * 60 +
                                              sign * second,
                                              sign * microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_YEAR:
                    if (out_l == 0) {
                        goto error;
                        break;
                    }
                    end = &out[out_l];
                    year = strtoul(out, &end, 10);
                    py_item = PyLong_FromLong(year);
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_JSON:
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_STRING:
                    if (!py_state->encodings[i]) {
                        py_item = PyBytes_FromStringAndSize(out, out_l);
                        if (!py_item) goto error;
                        break;
                    }

                    py_item = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_item) goto error;

                    // Parse JSON string.
                    if (py_state->type_codes[i] == MYSQL_TYPE_JSON && py_state->options.parse_json) {
                        py_str = py_item;
                        py_item = PyObject_CallFunctionObjArgs(py_state->py_json_loads, py_str, NULL);
                        Py_CLEAR(py_str);
                        if (!py_item) goto error;
                    }

                    break;

                default:
                    PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                                 py_state->type_codes[i], NULL);
                    goto error;
                }
            }
        }

        if (py_item == Py_None) {
            Py_INCREF(Py_None);
        }

        switch (py_state->options.output_type) {
        case MYSQLSV_OUT_NAMEDTUPLES:
            PyStructSequence_SET_ITEM(py_result, i, py_item);
            break;
        case MYSQLSV_OUT_DICTS:
            PyDict_SetItem(py_result, py_state->py_names[i], py_item);
            Py_INCREF(py_state->py_names[i]);
            Py_DECREF(py_item);
            break;
        default:
            PyTuple_SET_ITEM(py_result, i, py_item);
        }
    }

exit:
    return py_result;

error:
    Py_CLEAR(py_result);
    goto exit;
}

static PyObject *read_rowdata_packet(PyObject *self, PyObject *args, PyObject *kwargs) {
    int rc = 0;
    StateObject *py_state = NULL;
    PyObject *py_res = NULL;
    PyObject *py_out = NULL;
    PyObject *py_next_seq_id = NULL;
    PyObject *py_zero = PyLong_FromUnsignedLong(0);
    unsigned long long requested_n_rows = 0;
    unsigned long long row_idx = 0;
    char *keywords[] = {"result", "size", NULL};

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|K", keywords, &py_res, &requested_n_rows)) {
        goto error;
    }

    // Get the rowdata state.
    py_state = (StateObject*)PyObject_GetAttrString(py_res, "_state");
    if (!py_state) {
        PyErr_Clear();

        PyObject *py_requested_n_rows = PyLong_FromUnsignedLongLong(requested_n_rows);
        if (!py_requested_n_rows) goto error;

        PyObject *py_args = PyTuple_New(2);
        if (!py_args) goto error;
        PyTuple_SET_ITEM(py_args, 0, py_res);
        PyTuple_SET_ITEM(py_args, 1, py_requested_n_rows);
        Py_INCREF(py_res);
        Py_INCREF(py_requested_n_rows);

        py_state = (StateObject*)State_new(&StateType, py_args, NULL);
        if (!py_state) { Py_DECREF(py_args); goto error; }
        rc = State_init((StateObject*)py_state, py_args, NULL);
        Py_DECREF(py_args);
        if (rc != 0) { Py_CLEAR(py_state); goto error; }

        PyObject_SetAttrString(py_res, "_state", (PyObject*)py_state);
    }
    else if (requested_n_rows > 0) {
        State_reset_batch(py_state, py_res);
    }

    if (requested_n_rows == 0) {
        requested_n_rows = UINTMAX_MAX;
    }

    if (py_state->is_eof) {
        goto exit;
    }

    while (row_idx < requested_n_rows) {
        PyObject *py_buff = read_packet(py_state);
        if (!py_buff) goto error;

        PyObject *py_row = NULL;
        char *data = PyByteArray_AsString(py_buff);
        unsigned long long data_l = PyByteArray_GET_SIZE(py_buff);
        unsigned long long warning_count = 0;
        int has_next = 0;

        if (check_packet_is_eof(&data, &data_l, &warning_count, &has_next)) {
            Py_CLEAR(py_buff);

            py_state->is_eof = 1;

            PyObject *py_long = NULL;

            py_long = PyLong_FromUnsignedLongLong(warning_count);
            PyObject_SetAttrString(py_res, "warning_count", py_long ? py_long : 0);
            Py_CLEAR(py_long);

            py_long = PyLong_FromLong(has_next);
            PyObject_SetAttrString(py_res, "has_next", py_long ? py_long : 0);
            Py_CLEAR(py_long);

            PyObject_SetAttrString(py_res, "connection", Py_None);
            PyObject_SetAttrString(py_res, "unbuffered_active", Py_False);

            break;
        }

        py_state->n_rows++;
        py_state->n_rows_in_batch++;

        switch (py_state->options.output_type) {
        case MYSQLSV_OUT_PANDAS:
        case MYSQLSV_OUT_NUMPY:
            // Add to df_buffer size as needed.
            if (!py_state->unbuffered && py_state->n_rows >= py_state->df_buffer_n_rows) {
                py_state->df_buffer_n_rows *= 1.7;
                py_state->df_buffer = realloc(py_state->df_buffer,
                                              py_state->df_buffer_row_size *
                                              py_state->df_buffer_n_rows);
                if (!py_state->df_buffer) { Py_CLEAR(py_buff); goto error; }
                py_state->df_cursor = py_state->df_buffer + 
                                      py_state->df_buffer_row_size * py_state->n_rows;
            }
            read_dataframe_row_from_packet(py_state, data, data_l);
            py_state->df_cursor += py_state->df_buffer_row_size;
            break;

        default:
            py_row = read_obj_row_from_packet(py_state, data, data_l);
            if (!py_row) { Py_CLEAR(py_buff); goto error; }

            rc = PyList_Append(py_state->py_rows, py_row);
            if (rc != 0) { Py_CLEAR(py_buff); goto error; }
            Py_DECREF(py_row);
        }

        row_idx++;

        Py_CLEAR(py_buff);
    }

    switch (py_state->options.output_type) {
    case MYSQLSV_OUT_PANDAS:
    case MYSQLSV_OUT_NUMPY:
        // Resize the buffer down to be only the required amount needed.
        if (py_state->n_rows_in_batch > row_idx) {
            py_state->df_buffer = realloc(py_state->df_buffer,
                                          py_state->df_buffer_row_size * py_state->n_rows_in_batch);
            if (!py_state->df_buffer) goto error;
            py_state->df_cursor = py_state->df_buffer + 
                                  py_state->df_buffer_row_size * py_state->n_rows_in_batch;
            PyObject *py_tmp = py_state->py_rows;
            py_state->py_rows = build_array(py_state);
            Py_DECREF(py_tmp);
            if (!py_state->py_rows) goto error;
            rc = PyObject_SetAttrString(py_res, "rows", py_state->py_rows);
            if (rc != 0) goto error;
        }
    }

exit:
    if (!py_state) return NULL;

    py_next_seq_id = PyLong_FromUnsignedLongLong(py_state->next_seq_id);
    if (!py_next_seq_id) goto error;
    PyObject_SetAttrString(py_state->py_conn, "_next_seq_id", py_next_seq_id);
    Py_DECREF(py_next_seq_id);

    py_out = NULL;

    if (py_state->unbuffered) {
        if (py_state->is_eof && row_idx == 0) {
            Py_INCREF(Py_None);
            py_out = Py_None;
            PyObject_SetAttrString(py_res, "rows", Py_None);
            PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
            PyObject_SetAttrString(py_res, "affected_rows", (py_n_rows) ? py_n_rows : Py_None);
            Py_XDECREF(py_n_rows);
            PyObject_DelAttrString(py_res, "_state");
            Py_CLEAR(py_state);
        }
        else {
            switch (py_state->options.output_type) {
            case MYSQLSV_OUT_PANDAS:
            case MYSQLSV_OUT_NUMPY:
                py_out = (requested_n_rows == 1) ?
                         PyObject_GetItem(py_state->py_rows, py_zero) : py_state->py_rows;
                break;
            default:
                py_out = (requested_n_rows == 1) ? 
                         PyList_GetItem(py_state->py_rows, 0) : py_state->py_rows;
            }
            Py_XINCREF(py_out);
        }
    }
    else {
        py_out = py_state->py_rows;
        Py_INCREF(py_out);
        PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
        PyObject_SetAttrString(py_res, "affected_rows", (py_n_rows) ? py_n_rows : Py_None);
        Py_XDECREF(py_n_rows);
        if (py_state->is_eof) {
            PyObject_DelAttrString(py_res, "_state");
            Py_CLEAR(py_state);
        }
    }

    Py_XDECREF(py_zero);

    return py_out;

error:
    goto exit;
}

static PyMethodDef PyMySQLAccelMethods[] = {
    {"read_rowdata_packet", (PyCFunction)read_rowdata_packet, METH_VARARGS | METH_KEYWORDS, "MySQL row data packet reader"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef _pymysqlsvmodule = {
    PyModuleDef_HEAD_INIT,
    "_pymysqlsv",
    "PyMySQL row data packet reader accelerator",
    -1,
    PyMySQLAccelMethods
};

PyMODINIT_FUNC PyInit__pymysqlsv(void) {
    PyDateTime_IMPORT;
    if (PyType_Ready(&ArrayType) < 0) {
        return NULL;
    }
    if (PyType_Ready(&StateType) < 0) {
        return NULL;
    }
    return PyModule_Create(&_pymysqlsvmodule);
}
