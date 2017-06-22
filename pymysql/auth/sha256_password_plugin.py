from ..constants import CLIENT
from ..err import OperationalError

# Import cryptography for RSA_PKCS1_OAEP_PADDING algorithm
# which is needed when use sha256_password_plugin with no SSL
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    HAVE_CRYPTOGRAPHY = True
except ImportError:
    HAVE_CRYPTOGRAPHY = False


def _xor_password(password, salt):
    password_bytes = bytearray(password, 'ascii')
    salt_len = len(salt)
    for i in range(len(password_bytes)):
        password_bytes[i] ^= ord(salt[i % salt_len])
    return password_bytes


def _sha256_rsa_crypt(password, salt, public_key):
    if not HAVE_CRYPTOGRAPHY:
        raise OperationalError("cryptography module not found for sha256_password_plugin")
    message = _xor_password(password + b'\0', salt)
    rsa_key = serialization.load_pem_public_key(public_key, default_backend())
    return rsa_key.encrypt(
        message.decode('latin1').encode('latin1'), padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA1()),
            algorithm=hashes.SHA1(),
            label=None))


class SHA256PasswordPlugin(object):
    def __init__(self, con):
        self.con = con

    def authenticate(self, pkt):
        if self.con.ssl and self.con.server_capabilities & CLIENT.SSL:
            data = self.con.password.encode('latin1') + b'\0'
        else:
            if pkt.is_auth_switch_request():
                self.con.salt = pkt.read_all()
                if self.con.server_public_key == '':
                    self.con.write_packet(b'\1')
                    pkt = self.con._read_packet()
            if pkt.is_extra_auth_data() and self.con.server_public_key == '':
                pkt.read_uint8() 
                self.con.server_public_key = pkt.read_all()
            data = _sha256_rsa_crypt(
                self.con.password,
                self.con.salt,
                self.con.server_public_key)
        self.con.write_packet(data)
        pkt = self.con._read_packet()
        pkt.check_error()
        return pkt
