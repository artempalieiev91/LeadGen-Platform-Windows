"""Під час довгих задач утримує систему від переходу в idle sleep (macOS: caffeinate; Windows: SetThreadExecutionState)."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def prevent_idle_sleep() -> Iterator[None]:
    """
    На macOS запускає ``caffeinate -dims`` на час блоку (екран і простій без сну).
    На Windows викликає ``SetThreadExecutionState`` (SYSTEM_REQUIRED), щоб ОС менше охоче переходила в простій-сон під час задачі.
    Якщо ноутбук повністю засинає (кришка, ручний Sleep) — це не завжди можна обійти.
    """
    if sys.platform == "win32":
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        try:
            import ctypes

            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)  # type: ignore[attr-defined]
        except Exception:
            yield
            return
        try:
            yield
        finally:
            try:
                import ctypes

                ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)  # type: ignore[attr-defined]
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
