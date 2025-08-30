"""Lightweight progress / spinner helpers.

Separated to avoid optional dependency (alive-progress) in core logic imports.
"""
from __future__ import annotations
import threading, time

def _progress_bar(total=None, title: str = ""):
    try:
        from alive_progress import alive_bar as _alive_bar  # type: ignore
        return _alive_bar(total=total, title=title)
    except Exception:
        class _NoBar:
            def __enter__(self_inner):
                return (lambda: None)
            def __exit__(self_inner, exc_type, exc, tb):
                return False
        return _NoBar()

def _run_with_spinner(title: str, func, *args, **kwargs):
    """Run a function in a worker thread while showing an alive-progress spinner."""
    result = {}
    error = {}
    def _target():
        try:
            result['value'] = func(*args, **kwargs)
        except Exception as e:  # pragma: no cover (pass-through)
            error['exc'] = e
    t = threading.Thread(target=_target, daemon=True)
    t.start()
    try:
        from alive_progress import alive_bar as _alive_bar  # type: ignore
        with _alive_bar(total=None, title=title) as bar:
            while t.is_alive():
                time.sleep(0.1)
                bar()
    except Exception:
        while t.is_alive():
            time.sleep(0.1)
    finally:
        t.join()
    if 'exc' in error:
        raise error['exc']
    return result.get('value')

__all__ = ["_progress_bar", "_run_with_spinner"]
