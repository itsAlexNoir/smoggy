import sys

def add_path(path):
    if path not in sys.path:
        sys.path.insert(0, path)


__all__ = ["dataclean", "database"]