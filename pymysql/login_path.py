# -*- coding: utf-8 -*-
"""Funtions that read and decrypt MySQL's login path file."""

from io import BytesIO, TextIOWrapper
import os
import struct

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
except ImportError:
    raise ImportError('You must install the package "cryptography" in order '
                      'to read the login path file.')

# Buffer at the beginning of the login path file.
UNUSED_BUFFER_LENGTH = 4

# The key stored in the file.
LOGIN_KEY_LENGTH = 20

# The entire login key header.
LOGIN_KEY_HEADER_LENGTH = UNUSED_BUFFER_LENGTH + LOGIN_KEY_LENGTH

# Number of bytes used to store the length of ciphertext.
CIPHER_STORE_LENGTH = 4


def open_login_path_file():
    """Open a decrypted version of the login path file."""
    path = get_login_path_file()
    try:
        with open(path, 'rb') as fp:
            key = read_key(fp)
            cipher = get_aes_cipher(key)
            plaintext = decrypt_file(fp, cipher.decryptor())
    except (OSError, IOError):
        return None

    if not isinstance(plaintext, BytesIO):
        return None

    return TextIOWrapper(plaintext)


def get_login_path_file():
    """Return the login path file's path or None if it doesn't exist."""
    app_data = os.getenv('APPDATA')
    default_dir = os.path.join(app_data, 'MySQL') if app_data else '~'
    file_path = os.path.join(default_dir, '.mylogin.cnf')

    return os.getenv('MYSQL_TEST_LOGIN_FILE',
                     os.path.expanduser(file_path))


def read_key(fp):
    """Read the key from the login path file header."""
    # Move past the unused buffer.
    _buffer = fp.read(UNUSED_BUFFER_LENGTH)

    if not _buffer or len(_buffer) != UNUSED_BUFFER_LENGTH:
        # Login path file is blank or incomplete.
        return None

    return create_key(fp.read(LOGIN_KEY_LENGTH))


def create_key(key):
    """Create the AES key from the login path file header."""
    rkey = [0] * 16
    for i in range(len(key)):
        try:
            rkey[i % len(rkey)] ^= ord(key[i:i + 1])
        except TypeError:
            # ord() was unable to get the value of the byte.
            return None
    return struct.pack('16B', *rkey)


def get_aes_cipher(key):
    """Get the AES cipher object."""
    return Cipher(algorithms.AES(key), modes.ECB(),
                  backend=default_backend())


def decrypt_file(f, decryptor):
    """Decrypt a file *f* using *decryptor*."""
    plaintext = BytesIO()

    f.seek(LOGIN_KEY_HEADER_LENGTH)
    while True:
        # Read the length of the line.
        length_buffer = f.read(CIPHER_STORE_LENGTH)
        if len(length_buffer) < CIPHER_STORE_LENGTH:
            break
        line_length, = struct.unpack('<i', length_buffer)
        line = read_line(f, line_length, decryptor)
        plaintext.write(line)

    plaintext.seek(0)
    return plaintext


def read_line(f, length, decryptor):
    """Read a line of length *length* from file *f* using *decryptor*."""
    line = f.read(length)
    return remove_pad(decryptor.update(line))


def remove_pad(line):
    """Remove the pad from the *line*."""
    try:
        pad_length = ord(line[-1:])
    except TypeError:
        # ord() was unable to get the value of the byte.
        return None

    if pad_length > len(line):
        # Pad length should be less than or equal to the length of the
        # plaintext.
        return None

    return line[:-pad_length]
