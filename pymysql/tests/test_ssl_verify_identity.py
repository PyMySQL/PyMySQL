import ssl
import tempfile

import pymysql

_CA_PEM = """\
-----BEGIN CERTIFICATE-----
MIICqjCCAZICCQDrysU6sIP+rTANBgkqhkiG9w0BAQsFADAXMRUwEwYDVQQDDAxw
eW15c3FsLXRlc3QwHhcNMjYxMDA5MjIxMzA1WhcNMzYxMDA2MjIxMzA1WjAXMRUw
EwYDVQQDDAxweW15c3FsLXRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDjrdjsiligdE8FLZTfq+IWS+Rve80IFkhHuj8upM3/yoj0VYoi6xncAVdD
3UnEru6Qdic+gvLgsXkR19bWM+oeD71eyzHup8tQSMwCLV86HCpxCc8znJm+LkxW
gmfDXspM22rhxlGQUK1U241hVENMxLMpFKJALy6APWqGawjq22a4SeskU7Ty/FT1
ZxhhBXmjzGYKwcEoALsZJgaiy0TflAcackgvGUcX91Ug/EpR5gz/RxIHMCPBsjDZ
/wFNEPppMKaUj/tH1uXd7g7o/M012TJxZgbsEflgoDaWFTbEp9yZkncxU5nw1Uxy
HHGhKmJOzd/ZsEMPlg6dNMEnTeSnAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAJ+0
iTLdZDvYFWBsKIqYG2bmaDYkAVWOk7Jvzc7oY6jbUx7H87vqPvIQga0k+DZUvmEy
sqa3eouMAnczQhoPOmsP//RG4sNcP7J+qLNp4CALLySkIS9yJpurMnTvrkb4M4to
ghIPvLkF1+7C3RQuBLqiqxRoU/ZBHwAvdOlhthe2ctPtbH1TooMBZZbixxnPR3Iq
VLbD58+1Xit03Le4S0zF0Ng0howfDGDSqtXigLeEm7w+cEdJ+OFEXCM5RIn3UpFV
AeToaah+1URvtlTNDNBDNe0hbE02Xv42LyRahrKwJSrIrHRKgEE5bSK8cJLDr4Qe
XY6M5kQSUK1NrBQiTSk=
-----END CERTIFICATE-----
"""


def test_ssl_verify_identity_builds_a_context():
    # Identity checks need CERT_REQUIRED. CERT_NONE raises from ssl.
    with tempfile.NamedTemporaryFile("w", suffix=".pem") as ca:
        ca.write(_CA_PEM)
        ca.flush()
        checked = pymysql.connect(
            host="db.example.com",
            ssl_ca=ca.name,
            ssl_verify_identity=True,
            defer_connect=True,
        )
        assert checked.ctx.check_hostname
        assert checked.ctx.verify_mode == ssl.CERT_REQUIRED
        plain = pymysql.connect(
            host="db.example.com",
            ssl_ca=ca.name,
            defer_connect=True,
        )
        assert not plain.ctx.check_hostname
        assert plain.ctx.verify_mode == ssl.CERT_NONE
