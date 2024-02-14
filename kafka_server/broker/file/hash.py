import hashlib


def hash_md5(key: str):
    return hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()
