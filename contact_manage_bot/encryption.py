from cryptography.fernet import Fernet


class SessionCipher:
    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("SESSION_SECRET is required")
        self._fernet = Fernet(secret.encode("ascii"))

    def encrypt(self, value: str) -> str:
        token = self._fernet.encrypt(value.encode("utf-8"))
        return token.decode("ascii")

    def decrypt(self, value: str) -> str:
        raw = self._fernet.decrypt(value.encode("ascii"))
        return raw.decode("utf-8")