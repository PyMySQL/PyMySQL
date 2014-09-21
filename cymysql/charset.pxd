import cython

cdef class Charset(object):
    cdef public int id
    cdef public str name, collation
    cdef public bint is_default

cdef class Charsets(object):
    cdef dict _by_id
    cdef object by_id
    def add(self, Charset c)

    @cython.locals(c=Charset)    
    def by_name(self, name)

cdef Charsets _charsets

