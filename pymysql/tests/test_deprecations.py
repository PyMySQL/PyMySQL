from unittest import mock

import pytest

from pymysql.connections import Connection


def test_set_charset_deprecated():
    con = mock.Mock(spec=Connection)
    with pytest.warns(
        DeprecationWarning,
        match="'set_charset' is deprecated, use 'set_character_set' instead",
    ):
        Connection.set_charset(con, "utf8mb4")
    con.set_character_set.assert_called_once_with("utf8mb4")
