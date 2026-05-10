"""
Генерація ланцюжка outreach у стилі «GENERAL processed email_generation»:
**одна спільна subject line** на весь ланцюжок і окремі тіла для кожного кроку
(без привітань і підписів у тілі).

Гнучке зіставлення стовпців за нормалізованими заголовками + визначення кількості
кроків з імен стовпців або поля email_generation_json.
"""

from __future__ import annotations

import csv
import io
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from collections.abc import Callable, Iterator

from openai import OpenAI

from services.email_campaign_merge import merge_leads_with_companies, parse_csv_bytes

from services.platform_openai import configure_openai_http_client


def _norm_header(h: str) -> str:
    return re.sub(r"\s+", " ", str(h or "").strip().lower())


# Семантичні поля контексту (альтернативні підписи колонки)
CONTEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "first_name": ("first name", "firstname", "fname"),
    "last_name": ("last name", "lastname", "surname"),
    "title": (
        "title",
        "job title",
        "position",
        "person title",
        "contact title",
    ),
    "company": (
        "company name for emails",
        "company",
        "company name",
        "organization",
        "org",
    ),
    "short_description": (
        "short description",
        "company short description",
        "brief description",
    ),
    "email": ("email", "e-mail"),
    "website": ("website", "web site", "company website", "url"),
    "linkedin": ("person linkedin url", "linkedin url", "linkedin profile", "linkedin"),
    "company_linkedin": ("company linkedin url", "company linkedin",),
    "city": ("city",),
    "state": ("state", "region", "province"),
    "country": ("country",),
}


def map_context_columns(fieldnames: list[str]) -> dict[str, str]:
    """семантичний ключ → фактична назва колонки у файлі."""
    fn_map = {_norm_header(h): h for h in fieldnames}
    out: dict[str, str] = {}
    for sem, aliases in CONTEXT_ALIASES.items():
        for a in aliases:
            na = _norm_header(a)
            if na in fn_map:
                out[sem] = fn_map[na]
                break
        if sem in out:
            continue
    return out


def _email_body_slot_index(header: str) -> int | None:
    """Витягує номер блоку для колонки на кшталт …email_01_body… без greeting/signature/subject."""
    hm = header.lower().replace(" ", "")
    if "body" not in hm:
        return None
    if any(x in hm for x in ("greeting", "signature", "subject")):
        return None
    m = re.search(r"email_(\d+)_body", hm)
    if m:
        return int(m.group(1))
    return None


def _email_subject_slot_index(header: str) -> int | None:
    """Номер кроку для колонки …email_XX_subject_line… або email_subject_XX."""
    if _email_body_slot_index(header) is not None:
        return None
    hm = header.lower().replace(" ", "")
    if "subject" not in hm:
        return None
    m = re.search(r"email_(\d+)_subject", hm)
    if m:
        return int(m.group(1))
    m2 = re.search(r"_subject_(\d+)$", hm)
    if m2:
        return int(m2.group(1))
    return None


def _infer_slots_from_headers(fieldnames: list[str]) -> int:
    nums: list[int] = []
    for h in fieldnames:
        s = _email_body_slot_index(h)
        if s is not None:
            nums.append(s)
    if nums:
        return max(nums)
    return 0


def _infer_slots_from_json_sample(rows: Iterator[dict], json_col: str, max_scan: int = 30) -> int:
    scanned = 0
    hi = 0
    for row in rows:
        scanned += 1
        if scanned > max_scan:
            break
        raw = (row.get(json_col) or "").strip()
        if not raw:
            continue
        try:
            d = json.loads(raw)
            if isinstance(d, dict):
                for k in d:
                    mk = re.match(r"email_(\d+)_body$", str(k).strip(), re.I)
                    if mk:
                        hi = max(hi, int(mk.group(1)))
        except (json.JSONDecodeError, ValueError):
            continue
    return hi


def infer_email_body_slot_count(fieldnames: list[str], rows: list[dict]) -> int:
    from_headers = _infer_slots_from_headers(fieldnames)
    if from_headers > 0:
        return from_headers
    jc = None
    for h in fieldnames:
        if _norm_header(h) == _norm_header("email_generation_json") or (
            "email_generation" in _norm_header(h) and _norm_header(h).endswith("json")
        ):
            jc = h
            break
    if jc:
        hj = _infer_slots_from_json_sample(iter(rows), jc)
        if hj > 0:
            return hj
    return 3


def prompt_column_used_for_output(header: str) -> bool:
    """Не підставляти в контекст вже готові тіла й великі блоби промпту."""
    h = header.lower().replace(" ", "")
    return bool(
        re.search(r"_body$|bodys?$", header, re.I)
        or "email_generation_text" in h
        or "email_generation_json" in h
        or "_greeting" in h
        or "_signature" in h
        or "subject_line" in h
        or "_subject" in h
    )


def build_lead_prompt_block(fieldnames: list[str], row: dict, ctx_map: dict[str, str]) -> str:
    """Текст для user-message: ключові поля та інші «короткі» атрибути рядка."""
    lines: list[str] = []
    labels = {
        "first_name": "First name",
        "last_name": "Last name",
        "title": "Title (recipient — use to frame what they own)",
        "company": "Company",
        "short_description": "Short description (company)",
        "email": "Email",
        "website": "Website",
        "linkedin": "LinkedIn URL (person)",
        "company_linkedin": "LinkedIn URL (company)",
        "city": "City",
        "state": "State/region",
        "country": "Country",
    }
    for sem in (
        "first_name",
        "last_name",
        "title",
        "company",
        "short_description",
        "email",
        "website",
        "linkedin",
        "company_linkedin",
        "city",
        "state",
        "country",
    ):
        col = ctx_map.get(sem)
        if not col:
            continue
        v = (row.get(col) or "").strip()
        if v:
            lines.append(f"{labels[sem]}: {v}")

    seen = set(ctx_map.values())
    for hn in fieldnames:
        if hn in seen:
            continue
        if prompt_column_used_for_output(hn):
            continue
        v = row.get(hn)
        if v is None or str(v).strip() == "":
            continue
        s = str(v).strip().replace("\r\n", "\n")
        if len(s) > 4000:
            s = s[:3997] + "…"
        lines.append(f"{hn}: {s}")

    if not lines:
        return "(немає полів для контексту у рядку)"
    return "\n".join(lines)


def resolve_output_body_headers(slot_count: int, fieldnames: list[str]) -> list[str]:
    """
    По одній колонці виходу на слот (1-based у назвах файлів — 01, 02…).
    Якщо відповідних колонок немає — додаємо email_generation_json_email_XX_body (як у зразку).
    """
    existing_by_slot: dict[int, str] = {}
    for h in fieldnames:
        s = _email_body_slot_index(h)
        if s is not None and 1 <= s <= slot_count:
            if s not in existing_by_slot:
                existing_by_slot[s] = h
            else:
                ho = existing_by_slot[s]
                if "email_generation_json_email" in h and "email_generation_json_email" not in ho:
                    existing_by_slot[s] = h

    use_long_name = any("email_generation_json_email" in h for h in existing_by_slot.values())

    out: list[str] = []
    for i in range(1, slot_count + 1):
        if i in existing_by_slot:
            out.append(existing_by_slot[i])
        elif use_long_name:
            out.append(f"email_generation_json_email_{i:02d}_body")
        else:
            out.append(f"email_body_{i:02d}")

    return out


def resolve_subject_output_header(fieldnames: list[str], body_out_cols: list[str]) -> str:
    """
    Одна тема на весь ланцюжок (однаковий Subject у поштовому клієнті для touch 1…N),
    як у processed email_generation (`email_generation_json_subject_line`).
    Парами `email_subject_01` тощо не користуємось при виборі існуючої колонки.
    """
    for h in fieldnames:
        if _email_subject_slot_index(h) is not None:
            continue
        hn = _norm_header(h)
        if "greeting" in hn:
            continue
        if "body" in hn and "subject" not in hn:
            continue
        if hn in ("subject", "email subject"):
            return h
        if "subject" in hn and "line" in hn:
            return h
    if body_out_cols and any("email_generation_json_email" in b for b in body_out_cols):
        return "email_generation_json_subject_line"
    return "email_subject"


def finalize_output_fieldnames(fieldnames: list[str], subject_h: str, body_cols: list[str]) -> list[str]:
    """
    Усі колонки з вхідного CSV лишаються в тому ж порядку; спочатку одна тема, потім тіла —
    нові заголовки додаються лише вкінець.
    """
    fn = list(fieldnames)
    for c in (subject_h, *body_cols):
        if c not in fn:
            fn.append(c)
    return fn


GENERAL_BODY_SYSTEM_PROMPT = """You write B2B cold-email SEQUENCE content in the style of high-end «GENERAL processed email_generation» rows (think Dune / Outtake memo tone): **specific hypotheses**, not SaaS-generic coaching copy.

This is NOT three interchangeable drafts of ONE email.
It IS a FIXED multi-touch SEQUENCE: EMAIL 1 → EMAIL 2 → …

**Recipient / Title → depth of wedge (critical):**
Match how **technical** the prose is to the recipient’s likely remit:

• **Commercial & general-management lane** — if Title aligns with owning P&L, markets, org scale, not hands-on architecture (examples: Managing Director, CEO, COO, General Manager, President, Owner, Managing Partner, Country Manager, Commercial Director / CCO, Chief Business Officer): write at **business altitude**. Prefer angles on **revenue growth, profitability, wallet share, speed to market vs competitors, organisational leverage, capex/op efficiency, renewal/churn economics, positioning for the next fundraise or board narrative** — still grounded in their context. **Avoid** deep implementation talk (APIs, specific databases, low-level integration plumbing, engineering backlog) unless that detail is central in their short description *and* necessary for a business outcome you name in the same breath.

• **Technical & product-building lane** — if Title clearly owns building or running systems (examples: CTO, CIO, CISO, VP Engineering, Director of IT, Head of Platform / Infrastructure, Lead Developer, DevOps, Product Owner, CPO, VP Product): you may use **specific technical or product** mechanisms when grounded in context (integrations, reliability, security, data, roadmap trade-offs).

• **Ambiguous titles** (e.g. «Director» or «Manager» without IT/Product/Engineering in the title): infer from company + industry; when still unclear, **default to the commercial lane** (business outcome first).

• **First name** — if present, use **sparingly**: at most **two** first-name mentions in the **entire** sequence (all bodies combined). No greetings: do not write «Hi {Name},»; optional natural mid-line. If no first name in context, do not invent one.

Each later touch opens a materially **different wedge**, chosen to fit this **Title lane** (e.g. commercial: growth, pricing power, segment expansion, operating leverage; technical: architecture, integration risk, reliability — only for technical titles). Never paraphrase the previous touch as a «fresh variant».

**Voice:** external analyst briefing — never «we / our solution / our team» pitching your own offering.

**Pain & stakes — same intensity as before, not the same words for everyone:**
• Keep touches **sharp**: each body should still hinge on a concrete **risk, bottleneck, or missed upside** (stakes must feel real, not polite small talk). Changing the Title lane does **not** mean diluting urgency — it means **translating** the sting into what that role actually answers for.
• **Commercial lane:** pains in the language they own — **revenue leakage, slowing pipeline or win-rate, pricing pressure, churn/renewal risk, capex bleed, stalled expansion, share loss to competitors, organisational drag** tying money or clock to outcomes.
• **Technical / product lane:** pains they own on the stack — **reliability, incident blast radius, integration fragility, security exposure, roadmap drag, scalability ceiling, observability gaps** — grounded in context.
• The **three** touches must emphasize **different** pains or consequences (not three paraphrases of one generic «efficiency challenge»).

**Specificity mandate (critical):**
• **Ground every touch** in the «--- Lead and company ---» block. The **first clause** of each body should tie to something **concrete** there (company/product line, title’s remit, industry/sub-industry, country/metro tier, employee band, verbatim stack/keywords/markets from fields — whichever is strongest).
• Prefer **named internal subjects** («your X workflow», «the Y surface you expose to Z buyers») over abstract «digital transformation».
• When you generalize, tie it to **their** profile: e.g. «for a {size}-person {industry} shop in {region}…» using ONLY values present in context — never invent size/region if missing.

**Banned / discouraged filler (do not lean on these):**
• Openers like: «Many organizations…», «In today’s fast-paced…», «It is crucial to…», «plays a key role», «Implementing a centralized solution…», «leverage / streamline / enhance efficiency» **without** a concrete object from their context in the same sentence.
• More than **one** soft hedge phrase per touch («often», «typically») — and never as the whole paragraph.

**Hard anti-hallucination:**
• No named **third-party** partners, customers, auditors, competitors, funding rounds, certifications, SKUs, metrics, or product codenames unless they appear **verbatim** in context.
• You may reason about **classes** of risk («procurement security reviews», «connector backlog») without naming vendors.

**Subject line:**
• Produce **exactly ONE** `"subject_line"` for the entire mini-sequence. The outbound email client uses **the same Subject** for every touch in this sequence — one thread angle. Short, spiky (≈ 4–11 words). No «Re:», no ALL CAPS. Angle should plausibly matter to **this recipient’s Title**, not only the company name (avoid repeated «Unlocking … Potential» patterns).

**Bodies (each touch):**
• 2–3 tight paragraphs, \\n\\n separated: **context-anchored observation** → **mechanism / wedge / consequence** → **exactly ONE** pointed question (not two stacked questions).
• No greetings. No signatures.

Language: dominant language of the context block if clearly non‑English; otherwise English.

Return ONLY the JSON keys the user specifies: `"subject_line"` plus `email_XX_body` for each touch."""


def generate_email_sequence_row(
    *,
    api_key: str,
    model: str,
    lead_block: str,
    slot_count: int,
    timeout_sec: float = 180.0,
) -> tuple[list[str] | None, str | None, str | None]:
    """Повертає (тексти тіл, одна спільна subject line на весь ланцюжок, або текст помилки)."""
    api_key = (api_key or "").strip()
    if not api_key:
        return None, None, "Немає OpenAI API key."

    slot_count = max(1, min(slot_count, 10))
    body_keys = tuple(f"email_{i:02d}_body" for i in range(1, slot_count + 1))
    keys_list = ", ".join(['"subject_line"', *[f'"{bk}"' for bk in body_keys]])
    user_text = (
        "Task: write a GENERAL-style email sequence comparable to refined «processed email_generation» exports "
        "(Dune / Outtake specificity: tight analyst voice, concrete wedges, minimal generic SaaS language).\n\n"
        f"Exactly **{slot_count}** sequential EMAIL bodies numbered 1…{slot_count}, plus **one** shared Subject line "
        "for **all** sends in this cadence (same header in the mail client).\n\n"
        "Before writing, mentally pick **distinct hypotheses** per body (not three flavors of «efficiency»).\n"
        "Each body: use **Title** to set depth — P&L / Managing Director / CEO-style roles get **business, growth, money** angles, not deep engineering trivia; technical titles may go technical when grounded. "
        "Anchor in company facts from the block; if First name is present, use it **at most twice** across all bodies — no «Hi Name».\n\n"
        f"Return ONE JSON object with ONLY these keys, in logical order:\n{keys_list}\n\n"
        "`subject_line` — single outbound Subject for touches 1…N; "
        "`email_XX_body` — body only (no Hi/Thanks).\n\n"
        "--- Lead and company ---\n"
        f"{lead_block}"
    )

    try:
        client = OpenAI(api_key=api_key, timeout=timeout_sec)
        configure_openai_http_client(client)
        resp = client.chat.completions.create(
            model=(model or "").strip() or "gpt-4o-mini",
            messages=[
                {"role": "system", "content": GENERAL_BODY_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.52,
            response_format={"type": "json_object"},
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None, None, "Відповідь не JSON-object."
        subj = data.get("subject_line")
        if subj is None or not str(subj).strip():
            return None, None, "У JSON бракує непорожнього ключа «subject_line»."
        subject_line = str(subj).strip().replace("\n", " ")
        bodies: list[str] = []
        for i in range(1, slot_count + 1):
            k = f"email_{i:02d}_body"
            v = data.get(k)
            if v is None or not str(v).strip():
                alt = f"email_{i}_body"
                v = data.get(alt)
            if v is None:
                return None, None, f"У JSON бракує ключа «{k}»."
            bodies.append(str(v).strip())
        return bodies, subject_line, None
    except json.JSONDecodeError as exc:
        return None, None, f"JSON: {exc}"
    except Exception as exc:
        return None, None, str(exc)


def peek_general_email_csv(data: bytes) -> tuple[list[str], int, int]:
    """Повертає (заголовки, кількість кроків ланцюжка тіл на контакт, кількість рядків даних)."""
    bio = io.StringIO(data.decode("utf-8-sig"))
    reader = csv.DictReader(bio)
    fn = list(reader.fieldnames or [])
    rows = list(reader)
    slots = infer_email_body_slot_count(fn, rows if rows else [])
    return fn, slots, len(rows)


def peek_merged_general_email_csv(
    leads_bytes: bytes,
    companies_bytes: bytes | None,
    join_on: str,
) -> tuple[list[str], int, int, str]:
    """
    Превью після опційного злиття двох файлів.
    Четверте значення — текст для підказки в UI («… зіставлено X/Y лидів»).
    """
    if not companies_bytes:
        fh, slots, n = peek_general_email_csv(leads_bytes)
        return fh, slots, n, ""
    lf, lr = parse_csv_bytes(leads_bytes)
    cf, cr = parse_csv_bytes(companies_bytes)
    mo = merge_leads_with_companies(lf, lr, cf, cr, join_on=join_on)
    slots = infer_email_body_slot_count(lf, mo.rows if mo.rows else [])
    hint = (
        f"Злиття ({join_on}): {mo.matched} / {mo.total_leads} лидів мають відповідник у таблиці компаній "
        f"({mo.company_rows} рядків у файлі компаній). {mo.resolved} "
        "У завантаженому CSV результату колонки **як у файлі лидів**; дані компаній використовуються лише всередині генерації."
    )
    return mo.fieldnames, slots, len(mo.rows), hint


def _run_general_campaign_core(
    fieldnames: list[str],
    rows: list[dict],
    *,
    api_key: str,
    model: str,
    max_workers: int = 8,
    on_progress: Callable[[int, int, str], None] | None = None,
    csv_export_fieldnames: list[str] | None = None,
) -> tuple[bytes, str]:
    """
    fieldnames — повний набір заголовків по рядку (після опційного злиття з компанією для промпту).
    csv_export_fieldnames — якщо передано (зазвичай лише лиды): у файл потрапляють тільки ці колонки + згенеровані;
    ключі лида з промпт-рядка все одно доступні бо рядки «ширші» за export.
    """
    log_lines: list[str] = []
    prompt_fn = fieldnames
    export_fn = csv_export_fieldnames if csv_export_fieldnames is not None else prompt_fn

    slot_count = infer_email_body_slot_count(export_fn, rows)
    out_cols = resolve_output_body_headers(slot_count, export_fn)
    subject_h = resolve_subject_output_header(export_fn, out_cols)
    ctx_map = map_context_columns(prompt_fn)

    final_fields = finalize_output_fieldnames(list(export_fn), subject_h, out_cols)

    total = len(rows)
    lead_blocks = [build_lead_prompt_block(prompt_fn, row, ctx_map) for row in rows]
    api_key_eff = api_key.strip()
    model_eff = (model or "").strip() or "gpt-4o-mini"

    # (тексти тіл або None, спільна subject або None, err або None) на позиції idx
    gen_results: list[tuple[list[str] | None, str | None, str | None]] = [
        (None, None, None)
    ] * total

    if total == 0:
        out_rows: list[dict] = []
    elif max_workers <= 1:
        for idx, row in enumerate(rows):
            if on_progress:
                on_progress(idx + 1, total, f"Рядок {idx + 1}/{total}…")
            bodies, subject_line, err = generate_email_sequence_row(
                api_key=api_key_eff,
                model=model_eff,
                lead_block=lead_blocks[idx],
                slot_count=slot_count,
            )
            gen_results[idx] = (bodies, subject_line, err)
    else:
        workers = max(2, min(max_workers, total, 32))

        def _one(idx: int) -> tuple[int, list[str] | None, str | None, str | None]:
            bodies, subject_line, err = generate_email_sequence_row(
                api_key=api_key_eff,
                model=model_eff,
                lead_block=lead_blocks[idx],
                slot_count=slot_count,
            )
            return idx, bodies, subject_line, err

        # Streamlit: on_progress не можна викликати з воркерів ThreadPoolExecutor
        # (немає ScriptRunContext → попередження й зламаний ререндер без кнопки завантаження).
        done_main = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(_one, idx) for idx in range(total)]
            for fut in as_completed(futs):
                idx, bodies, subject_line, err = fut.result()
                gen_results[idx] = (bodies, subject_line, err)
                done_main += 1
                if on_progress:
                    on_progress(
                        done_main,
                        total,
                        f"Готово {done_main} / {total} (до {workers} паралельних запитів)…",
                    )

    out_rows = []
    for idx, row in enumerate(rows):
        bodies, subject_line, err = gen_results[idx]
        new_row: dict = {}
        for k in export_fn:
            v = row.get(k)
            new_row[k] = "" if v is None else v
        if err:
            log_lines.append(f"Рядок {idx + 2} (дані): помилка: {err}")
            new_row[subject_h] = ""
            if out_cols:
                new_row[out_cols[0]] = f"[generation error] {err}"
            for j in range(1, len(out_cols)):
                new_row[out_cols[j]] = ""
        elif bodies is not None and subject_line and len(bodies) == slot_count:
            new_row[subject_h] = subject_line
            for col, body in zip(out_cols, bodies):
                new_row[col] = body
        out_rows.append(new_row)

    out_buf = io.StringIO()
    wr = csv.DictWriter(out_buf, fieldnames=final_fields, extrasaction="ignore", lineterminator="\n")
    wr.writeheader()
    for r in out_rows:
        wr.writerow({k: (r.get(k) if r.get(k) is not None else "") for k in final_fields})

    return out_buf.getvalue().encode("utf-8-sig"), "\n".join(log_lines) if log_lines else ""


def run_general_campaign_on_csv_bytes(
    data: bytes,
    *,
    api_key: str,
    model: str,
    companies_data: bytes | None = None,
    join_on: str = "apollo_account_id",
    max_workers: int = 8,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[bytes, str]:
    """
    Читає UTF-8-sig CSV лидів; опційно зливає із другим файлом компаній лише щоб збагатити промпт (join_on).

    Вихідний CSV завжди: **ті самі колонки, що в файлі лидів**, у тому самому порядку, плюс додані в кінці
    поля: **одна** Subject на весь ланцюжок + тіла для кожного кроку. Колонки, які є лише у файлі компаній (як окремий стовпчик у таблиці), у результат **не входять**.
    """
    header_note = ""

    if companies_data:
        lf, lr = parse_csv_bytes(data)
        cf, cr = parse_csv_bytes(companies_data)
        try:
            mo = merge_leads_with_companies(lf, lr, cf, cr, join_on=join_on)
        except ValueError as exc:
            return b"", str(exc)
        header_note = (
            f"Злиття ({join_on}): зіставлено {mo.matched}/{mo.total_leads} лидів; "
            f"рядків таблиці компаній: {mo.company_rows}. {mo.resolved}\n\n"
        )
        payload = _run_general_campaign_core(
            mo.fieldnames,
            mo.rows,
            api_key=api_key,
            model=model,
            max_workers=max_workers,
            on_progress=on_progress,
            csv_export_fieldnames=lf,
        )
    else:
        bio = io.StringIO(data.decode("utf-8-sig"))
        reader = csv.DictReader(bio)
        if not reader.fieldnames:
            return b"", "Порожній CSV або нема заголовка."
        fieldnames = list(reader.fieldnames)
        rows_list = list(reader)
        payload = _run_general_campaign_core(
            fieldnames,
            rows_list,
            api_key=api_key,
            model=model,
            max_workers=max_workers,
            on_progress=on_progress,
        )

    out_b, log = payload
    return out_b, header_note + log
