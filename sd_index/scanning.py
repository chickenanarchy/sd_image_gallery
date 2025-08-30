"""Filesystem scanning & serialization helpers."""
from __future__ import annotations
import os

def serialize_obj(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, list):
        return [serialize_obj(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize_obj(value) for key, value in obj.items()}
    else:
        if hasattr(obj, "__dict__"):
            return {key: serialize_obj(value) for key, value in obj.__dict__.items() if not key.startswith("_")}
        else:
            return str(obj)

def scan_dir(path):
    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        yield from scan_dir(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        yield entry.path
                except (PermissionError, FileNotFoundError, OSError):
                    continue
    except (PermissionError, FileNotFoundError, OSError):
        return

__all__ = ['serialize_obj', 'scan_dir']
