from ..packet import MysqlPacket, FieldDescriptorPacket
from ..result import MySQLResult

from ..constants import SERVER_STATUS


SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS


class AsyncMySQLResult(MySQLResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def read_result(self):
        self.first_packet = MysqlPacket(
            await self.connection.socket.recv_packet(self.connection.loop),
            self.connection.charset,
            self.connection.encoding,
            self.connection.use_unicode,
        )

        if self.first_packet.is_ok_packet():
            (self.affected_rows, self.insert_id,
                self.server_status, self.warning_count,
                self.message) = self.first_packet.read_ok_packet()
            self.has_result = False
        else:
            self.field_count = ord(self.first_packet.read(1))
            await self._get_descriptions()
            self.has_result = True
            await self.read_rest_rowdata_packet()

    async def read_rest_rowdata_packet(self):
        """Read rest rowdata packets for each data row in the result set."""
        if (not self.has_result) or (self.rest_rows is not None):
            return
        rest_rows = []
        decoder = self.connection.conv
        while True:
            packet = MysqlPacket(
                await self.connection.socket.recv_packet(self.connection.loop),
                self.connection.charset,
                self.connection.encoding,
                self.connection.use_unicode,
            )
            is_eof, warning_count, server_status = packet.is_eof_and_status()
            if is_eof:
                self.warning_count = warning_count
                self.server_status = server_status
                self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
                break
            rest_rows.append(packet.read_decode_data(self.fields, decoder))
        self.rest_rows = rest_rows
        self.rest_row_index = 0

    async def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        description = []
        for i in range(self.field_count):
            field = FieldDescriptorPacket(
                await self.connection.socket.recv_packet(self.connection.loop),
                self.connection.charset,
                self.connection.encoding,
                self.connection.use_unicode,
            )
            self.fields.append(field)
            description.append(field.description())

        eof_packet = MysqlPacket(
            await self.connection.socket.recv_packet(self.connection.loop),
            self.connection.charset,
            self.connection.encoding,
            self.connection.use_unicode,
        )
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)

    async def fetchone(self):
        if not self.has_result:
            return None
        if self.rest_rows is None:
            packet = MysqlPacket(
                await self.connection.socket.recv_packet(self.connection.loop),
                self.connection.charset,
                self.connection.encoding,
                self.connection.use_unicode,
            )
            is_eof, warning_count, server_status = packet.is_eof_and_status()
            if is_eof:
                self.warning_count = warning_count
                self.server_status = server_status
                self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
                self.rest_rows = []
                return None
            return packet.read_decode_data(self.fields, self.connection.conv)
        elif len(self.rest_rows) != self.rest_row_index:
            self.rest_row_index += 1
            return self.rest_rows[self.rest_row_index - 1]
        return None
