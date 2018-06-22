import pymysql

pymysql.connections.DEBUG = True
pymysql._auth.DEBUG = True

host="127.0.0.1"
port=3306

ssl = {'ca': 'ca.pem', 'check_hostname': False}
#ssl = None

print("### trying sha2 without password")
con = pymysql.connect(user="user_sha2_nopass", host=host, port=port, ssl=ssl)
print("OK\n\n\n")

print("### trying sha2 with password")
con = pymysql.connect(user="user_sha256", password="pass_sha256", host=host, port=port, ssl=ssl)
print("OK\n\n\n")

print("### trying caching sha2 without password")
con = pymysql.connect(user="user_csha2_nopass", host=host, port=port, ssl=ssl)
print("OK\n\n\n")

print("### trying caching sha2 with password")
con = pymysql.connect(user="user_caching_sha2", password="pass_caching_sha2", host=host, port=port, ssl=ssl)
print("OK\n\n\n")
