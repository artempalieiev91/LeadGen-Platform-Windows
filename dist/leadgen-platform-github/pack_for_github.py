"""
Зібрати копію проєкту без venv/node_modules/секретів — одна папка (і опційно .zip) для GitHub.

Запуск з кореня репозиторію:
  python pack_for_github.py
  python pack_for_github.py --zip

Вихід: dist/leadgen-platform-github/ та за потреби dist/leadgen-platform-github.zip
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
OUT_NAME = "leadgen-platform-github"

SKIP_DIR_NAMES = frozenset({
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    ".idea",
    ".vscode",
    ".playwright-browsers",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "dist",  # не копіювати попередній збір у себе
})

SKIP_FILE_NAMES = frozenset({".env", "secrets.toml"})
# Локальні артефакти Name2Email (не код)
SKIP_NAME2EMAIL_DATA = frozenset({"Input.csv", "Output_With_Emails.csv"})
SKIP_SUFFIXES = (".pyc", ".pyo")


def _skip_path(src: Path, base: Path) -> bool:
    try:
        rel = src.relative_to(base)
    except ValueError:
        return True
    for part in rel.parts:
        if part in SKIP_DIR_NAMES:
            return True
    name = src.name.lower()
    if name in SKIP_FILE_NAMES:
        return True
    if name.endswith(SKIP_SUFFIXES):
        return True
    if name == "puppeteer_needed.tmp.csv":
        return True
    if name in SKIP_NAME2EMAIL_DATA and "name2emails" in rel.parts:
        return True
    return False


def _copy_tree(src_dir: Path, dst_dir: Path, base: Path) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    for item in src_dir.iterdir():
        if _skip_path(item, base):
            continue
        target = dst_dir / item.name
        if item.is_dir():
            _copy_tree(item, target, base)
        else:
            shutil.copy2(item, target)


def _copy_root_files(out: Path) -> None:
    for name in (".gitignore", "README.md", "requirements.txt", "streamlit_app.py", "pack_for_github.py"):
        src = ROOT / name
        if src.is_file():
            shutil.copy2(src, out / name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Збірка чистої копії для GitHub.")
    parser.add_argument("--zip", action="store_true", help="Додатково створити dist/*.zip")
    args = parser.parse_args()

    out = DIST / OUT_NAME
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    _copy_root_files(out)

    st = ROOT / ".streamlit"
    if st.is_dir():
        _copy_tree(st, out / ".streamlit", ROOT)

    for folder in ("data", "services", "tabs"):
        src = ROOT / folder
        if src.is_dir():
            _copy_tree(src, out / folder, ROOT)

    for vsub in ("mathcurls", "name2emails"):
        src = ROOT / "vendor" / vsub
        if src.is_dir():
            _copy_tree(src, out / "vendor" / vsub, ROOT)

    print(f"Готово: {out}")
    print("Файл .streamlit/secrets.toml не копіюється (ім'я в списку пропуску).")

    if args.zip:
        zpath = DIST / f"{OUT_NAME}.zip"
        if zpath.exists():
            zpath.unlink()
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in out.rglob("*"):
                if f.is_file():
                    arc = f.relative_to(out.parent)
                    zf.write(f, arc.as_posix())
        print(f"ZIP: {zpath}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
