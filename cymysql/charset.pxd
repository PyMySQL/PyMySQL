import cython

cdef class Charset(object):
    cdef public int id
    cdef public str name, collation
    cdef public bint is_default

cdef class Charsets(object):
    cdef dict _by_id
    cdef add(self, Charset c)

    @cython.locals(c=Charset)
    cpdef by_name(self, name)

    cpdef object by_id(self, id)

cdef Charsets _charsets

