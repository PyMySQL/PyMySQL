# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html

import collections as _collections
import struct as _struct


def _parse_proto_desc(desc, clsName):
    struct_types, fields = zip(*desc)
    expr = "<" + "".join(struct_types)
    non_empty_fields = (field for field in fields if field is not None)
    proto_fields = " ".join(non_empty_fields)

    newType = _collections.namedtuple(clsName, proto_fields)

    return expr, newType


def _expand_expression(expr, buffer):
    while True:
        pos = expr.find('z')
        if pos < 0:
            break
        asciiz_start = _struct.calcsize(expr[:pos])
        asciiz_len = buffer[asciiz_start:].find('\0')
        expr = '%s%dsx%s' % (expr[:pos], asciiz_len, expr[pos+1:])
    return expr


def _parse(definition, ret_type, data):
    parsed_fields = _struct.unpack_from(definition, data)
    return (ret_type)(*parsed_fields)


CLIENT_SECURE_CONNECTION = 0x00008000    # CLIENT.SECURE_CONNECTION
CLIENT_PLUGIN_AUTH       = 0x00080000    # CLIENT.PLUGIN_AUTH


# Define basic protocol
_basics_proto = (
    ("B",  "protocol_version"),
    ("z",  "server_version"),
    ("I",  "server_thread_id"),
    ("8s", "auth_plugin_data_part1"),
    ("x",  None),
    ("H",  "capabilities_lower"),
)

_basics_expr, Basics = _parse_proto_desc(_basics_proto, "Basics")

# Define extended protocol
_additional_proto = (
    ("B",   'character_set'),
    ("H",   'status_flags'),
    ("H",   'capabilities_upper'),
    ("B",   'auth_plugin_data_length'),
    ("10x", None),
)

_additional_expr, Additional = _parse_proto_desc(_additional_proto, "Additional")


def parse_basics(data):
    expanded_basics_expr = _expand_expression(_basics_expr, data)
    size = _struct.calcsize(expanded_basics_expr)
    parsed = _parse(expanded_basics_expr, Basics, data)
    return size, parsed

def parse_additional(data):
    size = _struct.calcsize(_additional_expr)
    parsed = _parse(_additional_expr, Additional, data)
    return size, parsed

