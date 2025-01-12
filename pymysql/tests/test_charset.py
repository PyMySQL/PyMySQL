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
    lowercase_utf8 = pymysql.charset.charset_by_name("utf8")
    assert lowercase_utf8 == utf8mb4

    # Regardless of case, UTF8 (which is special cased) should resolve to the same thing
    uppercase_utf8 = pymysql.charset.charset_by_name("UTF8")
    mixedcase_utf8 = pymysql.charset.charset_by_name("UtF8")
    assert uppercase_utf8 == lowercase_utf8
    assert mixedcase_utf8 == lowercase_utf8

def test_case_sensitivity():
    lowercase_latin1 = pymysql.charset.charset_by_name("latin1")
    assert lowercase_latin1 is not None

    # lowercase and uppercase should resolve to the same charset
    uppercase_latin1 = pymysql.charset.charset_by_name("LATIN1")
    assert uppercase_latin1 == lowercase_latin1

    # lowercase and mixed case should resolve to the same charset
    mixedcase_latin1 = pymysql.charset.charset_by_name("LaTiN1")
    assert mixedcase_latin1 == lowercase_latin1
