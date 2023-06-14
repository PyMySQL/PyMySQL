import pymysql.charset


def test_utf8():
    utf8mb3 = pymysql.charset.charset_by_name("utf8mb3")
    assert utf8mb3.name == "utf8mb3"
    assert utf8mb3.collation == "utf8mb3_general_ci"
    assert (
        repr(utf8mb3)
        == "Charset(id=33, name='utf8mb3', collation='utf8mb3_general_ci')"
    )

    # MySQL 8.0 changed the default collation for utf8mb4.
    # But we use old default for compatibility.
    utf8mb4 = pymysql.charset.charset_by_name("utf8mb4")
    assert utf8mb4.name == "utf8mb4"
    assert utf8mb4.collation == "utf8mb4_general_ci"
    assert (
        repr(utf8mb4)
        == "Charset(id=45, name='utf8mb4', collation='utf8mb4_general_ci')"
    )

    # utf8 is alias of utf8mb4 since MySQL 8.0, and PyMySQL v1.1.
    utf8 = pymysql.charset.charset_by_name("utf8")
    assert utf8 == utf8mb4
