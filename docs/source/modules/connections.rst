Connection Object
=================

Resetting a connection
----------------------

``connection.reset()`` clears the server session using ``COM_RESET_CONNECTION``
without reconnecting or authenticating again. It rolls back outstanding work and
discards temporary tables, user variables, locks and prepared statements. The
selected database and PyMySQL's current charset/collation are retained.

Autocommit and other session settings return to the server's defaults, which may
differ from the options passed to ``connect()``. In particular, reset does not
restore PyMySQL's default ``autocommit=False`` or rerun ``init_command``. Apply any
required session settings explicitly before reuse::

    connection.reset()
    connection.autocommit(False)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")

Close existing cursors before resetting and create new ones afterwards. Reset
returns ``None`` on success and raises the usual connection/server exceptions on
failure. The command requires MySQL 5.7.3 or MariaDB 10.2.4 or newer; this does not
extend PyMySQL's supported-server policy. Unsupported servers receive no fallback
command. Charset/collation are reapplied after a successful protocol reset; if
that fails, the connection is closed and the error is raised.

API reference
-------------

.. module:: pymysql.connections

.. autoclass:: Connection
   :members:
   :exclude-members: DataError, DatabaseError, Error, InterfaceError,
                     IntegrityError, InternalError, NotSupportedError,
                     OperationalError, ProgrammingError, Warning,
                     escape, literal, write_packet
