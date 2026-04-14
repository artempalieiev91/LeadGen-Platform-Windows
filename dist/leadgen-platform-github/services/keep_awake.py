"""Під час довгих задач утримує систему від idle sleep (macOS: caffeinate; Windows: SetThreadExecutionState)."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def prevent_idle_sleep() -> Iterator[None]:
    """
    macOS: ``caffeinate -dims`` (екран і простій без сну).
    Windows: ``SetThreadExecutionState`` (система та дисплей не переходять у режим енергозбереження).
    Інші ОС: без дії.
    Повне засинання ноутбука (кришка, ручний Sleep) не завжди можна обійти.
    """
    if sys.platform == "win32":
        import ctypes

        es_continuous = 0x80000000
        es_system_required = 0x00000001
        es_display_required = 0x00000002
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                es_continuous | es_system_required | es_display_required
            )
        except Exception:
            yield
            return
        try:
            yield
        finally:
            try:
                ctypes.windll.kernel32.SetThreadExecutionState(es_continuous)
            except Exception:
                pass
        return

    if sys.platform != "darwin":
        yield
        return
    try:
        proc = subprocess.Popen(
            ["caffeinate", "-dims"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        yield
        return
    try:
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
