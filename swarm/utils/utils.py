import hashlib
import string
import random


def generate_id() -> str:
    return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]