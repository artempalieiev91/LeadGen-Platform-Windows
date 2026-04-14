"""Запуск Name2Email: за замовчуванням Node+Puppeteer (як Name2Email-PythonMixed-Windows), резерв — Playwright."""

from __future__ import annotations

import asyncio
import io
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

NODE_RUNNER_DIR = Path(__file__).resolve().parent.parent / "vendor" / "name2emails" / "node_runner"
NODE_SCRIPT = NODE_RUNNER_DIR / "name2email_platform.cjs"


def _ensure_windows_playwright_event_loop() -> None:
    """
    На Windows Playwright (sync) запускає subprocess для драйвера; потрібен ProactorEventLoop.
    Інакше asyncio.create_subprocess_exec дає NotImplementedError (зокрема під Streamlit).
    """
    if sys.platform != "win32":
        return
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


_VENDOR = Path(__file__).resolve().parent.parent / "vendor" / "name2emails"
if str(_VENDOR) not in sys.path:
    sys.path.insert(0, str(_VENDOR))


def name2emails_supported_platform() -> bool:
    """Локальний десктоп: macOS або Windows (Chrome + CDP). На Linux без GUI — ні."""
    return sys.platform in ("darwin", "win32")


def _node_runner_ready() -> bool:
    if shutil.which("node") is None:
        return False
    return (NODE_RUNNER_DIR / "node_modules" / "puppeteer-core" / "package.json").is_file()


def _run_node_puppeteer_pipeline(
    tmp_path: Path,
    *,
    chrome_profile_dir: str | None,
    wait_for_login: bool,
    on_progress: Callable[[int, int, str, str, str], None] | None,
    log: io.StringIO,
) -> None:
    """
    Як у Name2Email-PythonMixed-Windows / name2email.py: лише підняти Chrome з CDP (subprocess),
    без Playwright — Gmail, compose і Name2Email відкриває та обробляє Node (puppeteer.connect).
    Раніше Playwright відкривав «Написати» і чекав маркери логіну, через що UI зависав на «Initializing…».
    """
    from contextlib import redirect_stderr, redirect_stdout

    from gmail_name2email_client import EMAIL_NOT_FOUND_PLACEHOLDER, Name2EmailClient  # noqa: E402

    client = Name2EmailClient(
        chrome_profile_dir=chrome_profile_dir,
        wait_for_login=False,
        on_progress=None,
    )
    input_path = Path("Input.csv")
    rows, fn, q_col, e_col = client._read_input_csv(input_path)
    out_path = Path("Output_With_Emails.csv")
    client._merge_output_into_rows(rows, fn, out_path, e_col)
    meta = {
        "email_col": e_col,
        "query_col": q_col,
        "fieldnames": fn,
        "email_not_found_placeholder": EMAIL_NOT_FOUND_PLACEHOLDER,
    }
    Path("meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    with redirect_stdout(log), redirect_stderr(log):
        client._ensure_chrome_debugging_ready()

    env = os.environ.copy()
    env["NAME2EMAIL_INPUT"] = str((tmp_path / "Input.csv").resolve())
    env["NAME2EMAIL_OUTPUT"] = str((tmp_path / "Output_With_Emails.csv").resolve())
    env["NAME2EMAIL_META"] = str((tmp_path / "meta.json").resolve())
    env["NAME2EMAIL_BROWSER_URL"] = "http://localhost:9222"

    proc = subprocess.Popen(
        ["node", str(NODE_SCRIPT)],
        cwd=str(NODE_RUNNER_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        log.write(line)
        if on_progress and line.startswith("N2E_PROGRESS\t"):
            try:
                payload = json.loads(line.split("\t", 1)[1])
                on_progress(
                    int(payload["current"]),
                    int(payload["total"]),
                    str(payload.get("query", "")),
                    str(payload.get("email", "")),
                    str(payload.get("status", "")),
                )
            except Exception:
                pass
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"name2email_platform.cjs завершився з кодом {rc}")


def _run_playwright_pipeline(
    *,
    chrome_profile_dir: str | None,
    wait_for_login: bool,
    on_progress: Callable[[int, int, str, str, str], None] | None,
    log: io.StringIO,
) -> None:
    _ensure_windows_playwright_event_loop()
    from contextlib import redirect_stderr, redirect_stdout

    from gmail_name2email_client import Name2EmailClient  # noqa: E402

    with redirect_stdout(log), redirect_stderr(log):
        client = Name2EmailClient(
            chrome_profile_dir=chrome_profile_dir,
            wait_for_login=wait_for_login,
            on_progress=on_progress,
        )
        client.run()


def run_name2email_client(
    file_bytes: bytes,
    *,
    chrome_profile_dir: str | None = None,
    wait_for_login: bool = True,
    on_progress: Callable[[int, int, str, str, str], None] | None = None,
) -> tuple[bytes, str]:
    """
    Записує CSV як Input.csv у тимчасову папку.
    Якщо встановлені Node.js і npm у node_runner: як у name2email.py з Name2Email-PythonMixed-Windows —
    лише піднімається Chrome (CDP), далі пошук робить Node+Puppeteer (без Playwright).
    Інакше — повний цикл через Playwright.
    """
    import tempfile

    if not name2emails_supported_platform():
        raise RuntimeError(
            "Автозапуск Chrome з репозиторію розрахований на локальний macOS або Windows "
            "з установленим Google Chrome. На типовому хмарному Linux-хостингу ця вкладка недоступна."
        )

    log = io.StringIO()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "Input.csv").write_bytes(file_bytes)

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            if _node_runner_ready():
                try:
                    _run_node_puppeteer_pipeline(
                        tmp_path,
                        chrome_profile_dir=chrome_profile_dir,
                        wait_for_login=wait_for_login,
                        on_progress=on_progress,
                        log=log,
                    )
                except Exception as e:
                    log.write(f"\n--- Node+Puppeteer: {e}. Відкат на Playwright. ---\n")
                    _run_playwright_pipeline(
                        chrome_profile_dir=chrome_profile_dir,
                        wait_for_login=wait_for_login,
                        on_progress=on_progress,
                        log=log,
                    )
            else:
                log.write(
                    f"\n--- Node runner недоступний. Встановіть Node.js і виконайте: "
                    f"cd \"{NODE_RUNNER_DIR}\" && npm install ---\n"
                    f"Використовується лише Playwright.\n"
                )
                _run_playwright_pipeline(
                    chrome_profile_dir=chrome_profile_dir,
                    wait_for_login=wait_for_login,
                    on_progress=on_progress,
                    log=log,
                )
        finally:
            os.chdir(old_cwd)

        out = tmp_path / "Output_With_Emails.csv"
        if not out.is_file():
            return b"", log.getvalue()
        return out.read_bytes(), log.getvalue()
