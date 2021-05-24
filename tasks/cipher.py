import base64
import gzip
import hashlib
import io
import json
from Crypto import Random
from Crypto.Cipher import AES


class AESCipher(object):
    def __init__(self, key):
        self.bs = 32
        self.key = hashlib.sha256(key).digest()

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.urlsafe_b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = base64.urlsafe_b64decode(enc + '===')  # === Hack to allow decoding of tokens created by any encoder
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:]))

    def _pad(self, s):
        return s + ((self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)).encode('utf-8')

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s)-1:])]


TOKEN_ENCRYPT_KEY = b'cliffordpiano'


def create_auth_token(content):
    aes = AESCipher(TOKEN_ENCRYPT_KEY)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(json.dumps(content).encode('utf-8'))
    return aes.encrypt(out.getvalue())


def parse_auth_token(encrypted_gzipped_json_str):
    aes = AESCipher(TOKEN_ENCRYPT_KEY)
    data = io.BytesIO(aes.decrypt(encrypted_gzipped_json_str))
    with gzip.GzipFile(fileobj=data) as f:
        content = f.read()
    return json.loads(content.decode('utf-8'))
