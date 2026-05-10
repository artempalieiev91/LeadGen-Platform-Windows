"""Злиття CSV контактів (ліди) із CSV компаніями для збагачення генерації outreach."""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from urllib.parse import urlparse


def norm_header_key(h: str) -> str:
    return re.sub(r"\s+", " ", str(h or "").strip().lower())


def parse_csv_bytes(data: bytes) -> tuple[list[str], list[dict]]:
    bio = io.StringIO(data.decode("utf-8-sig"))
    reader = csv.DictReader(bio)
    fn = list(reader.fieldnames or [])
    rows = list(reader)
    return fn, rows


_COL_APOLLO = (
    "apollo account id",
    "apollo_account_id",
    "account id apollo",
)
_COL_WEBSITE_LEAD = ("website", "company website", "web site", "primary website", "url")
_COL_WEBSITE_COMP = (
    "website",
    "company website",
    "primary domain",
    "company domain",
    "domain",
    "website url",
)
_COL_CO_LEAD = ("company name for emails", "company name", "account name")
_COL_CO_COMP = ("company name", "organization name", "account name", "organization")


def pick_column(fieldnames: list[str], candidates: tuple[str, ...]) -> str | None:
    fh = {norm_header_key(x): x for x in fieldnames}
    for c in candidates:
        nc = norm_header_key(c)
        if nc in fh:
            return fh[nc]
    return None


def domain_from_cell(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    low = s.strip().lower()
    if "://" not in low and not low.startswith(("http", "//")):
        host = low.split("/")[0].split("?")[0]
        if "." in host:
            netloc = host
        else:
            low_full = "https://" + low
            try:
                netloc = urlparse(low_full).netloc or ""
            except Exception:
                return ""
    else:
        try:
            netloc = urlparse(low).netloc or urlparse(low).path.split("/")[0]
        except Exception:
            return ""
    netloc = netloc.strip().lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc.partition(":")[0]


def normalize_company_join_key(raw: str) -> str:
    s = re.sub(r"\s+", " ", str(raw or "").strip().lower())
    s = re.sub(
        r"\s+(gmbh|srl\.?|plc|corp\.?|corporation|ltd\.?|limited|inc\.?|ag|sa|bv|nv|spa|kg|kk)\.?\s*$",
        "",
        s,
        flags=re.I,
    )
    return s.strip()


def normalize_apollo_id(raw: str) -> str:
    return re.sub(r"\s+", "", (raw or "").strip().lower())


@dataclass
class MergeOutcome:
    fieldnames: list[str]
    rows: list[dict]
    matched: int
    total_leads: int
    company_rows: int
    resolved: str


def merged_fieldnames(lead_fn: list[str], company_fn: list[str]) -> list[str]:
    out = list(lead_fn)
    seen = set(lead_fn)
    for h in company_fn:
        if h not in seen:
            out.append(h)
            seen.add(h)
    return out


def merge_leads_with_companies(
    lead_fn: list[str],
    lead_rows: list[dict],
    company_fn: list[str],
    company_rows: list[dict],
    *,
    join_on: str,
) -> MergeOutcome:
    """
    join_on: ``apollo_account_id`` | ``website_domain`` | ``company_name``.
    При знайденій компанії поля таблиці лидів домінантні (поверх рядка компанії).
    """
    if not lead_fn:
        raise ValueError("У файлі лидів немає заголовків.")
    join_on = join_on.strip().lower()

    if join_on == "apollo_account_id":
        lc = pick_column(lead_fn, _COL_APOLLO)
        cc = pick_column(company_fn, _COL_APOLLO)
        if not lc or not cc:
            raise ValueError(
                "Для злиття потрібна колонка «Apollo Account Id» (або схожа) **в обох** файлах. "
                f"Знайдено: ліди={'так (' + lc + ')' if lc else 'ні'}, компанії={'так (' + cc + ')' if cc else 'ні'}."
            )
        lk = lc
        ck = cc

        def norm_k(row: dict, key: str) -> str:
            return normalize_apollo_id(str(row.get(key) or ""))

    elif join_on == "website_domain":
        lc = pick_column(lead_fn, _COL_WEBSITE_LEAD)
        cc = pick_column(company_fn, _COL_WEBSITE_COMP)
        if not lc or not cc:
            raise ValueError(
                "Для злиття за доменом потрібен **Website** (або domain) у обох файлах. "
                f"Ліди: {lc or '—'}, компанії: {cc or '—'}."
            )
        lk = lc
        ck = cc

        def norm_k(row: dict, key: str) -> str:
            return domain_from_cell(str(row.get(key) or ""))

    elif join_on == "company_name":
        lc = pick_column(lead_fn, _COL_CO_LEAD)
        cc = pick_column(company_fn, _COL_CO_COMP)
        if not lc or not cc:
            raise ValueError(
                "Для злиття за назвою потрібні колонки **Company Name** / **Company Name for Emails** у лидах і назва компанії у файлі компаній."
            )
        lk = lc
        ck = cc

        def norm_k(row: dict, key: str) -> str:
            return normalize_company_join_key(str(row.get(key) or ""))

    else:
        raise ValueError(f"Невідомий режим join: {join_on}")

    lookup: dict[str, dict] = {}
    for cr in company_rows:
        nk = norm_k(cr, ck)
        if nk:
            lookup[nk] = cr

    out_fn = merged_fieldnames(lead_fn, company_fn)
    merged_rows: list[dict] = []
    matched = 0
    for lr in lead_rows:
        nk = norm_k(lr, lk)
        comp = lookup.get(nk) if nk else None
        if nk and comp:
            matched += 1
        row = dict(comp) if comp else {}
        row.update(lr)
        merged_rows.append(row)

    resolved = (
        f"Злиття «{join_on}»: ключ у лидах `{lk}`, у компаніях `{ck}` ({len(lookup)} унікальних ключів у таблиці компаній)."
    )
    return MergeOutcome(
        fieldnames=out_fn,
        rows=merged_rows,
        matched=matched,
        total_leads=len(lead_rows),
        company_rows=len(company_rows),
        resolved=resolved,
    )
