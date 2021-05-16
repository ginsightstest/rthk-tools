import os


def to_abs_path(path: str) -> str:
    return os.path.abspath(os.path.expanduser(path))
