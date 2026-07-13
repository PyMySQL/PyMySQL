"""Tests for LOAD DATA LOCAL INFILE path traversal protection."""
import os

import pytest

import pymysql
from pymysql import err
from pymysql.connections import LoadLocalFile
from pymysql.constants import CR


def test_load_local_file_rejects_path_traversal():
    """A malicious server cannot read files outside the working directory."""
    # Mock a minimal connection object
    conn = object()

    with pytest.raises(err.OperationalError) as exc_info:
        LoadLocalFile("../../../etc/passwd", conn)

    assert exc_info.value.args[0] == CR.CR_LOAD_DATA_LOCAL_INFILE_REALPATH_FAIL


def test_load_local_file_rejects_absolute_path_outside_cwd():
    """Absolute paths outside the working directory are rejected."""
    conn = object()

    with pytest.raises(err.OperationalError) as exc_info:
        LoadLocalFile("/etc/passwd", conn)

    assert exc_info.value.args[0] == CR.CR_LOAD_DATA_LOCAL_INFILE_REALPATH_FAIL
