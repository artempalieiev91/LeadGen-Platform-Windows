"""Email Text Generation — кампанія General: один або два CSV → злиття для промпту → 1× Subject + ланцюжок тіл."""

from __future__ import annotations

import streamlit as st

from services.email_text_general_campaign import (
    peek_merged_general_email_csv,
    run_general_campaign_on_csv_bytes,
)
from services.keep_awake import prevent_idle_sleep
from services.platform_openai import openai_api_key_effective

_SESSION_CAMPAIGN = "email_gen_campaign_choice"
_SESSION_UPLOAD_SIG = "email_gen_upload_sig"


@st.dialog("Кампанія")
def _dialog_pick_campaign() -> None:
    st.markdown("Оберіть, для якої кампанії готуєте тексти.")
    choice = st.selectbox(
        "Кампанія",
        options=["General"],
        index=0,
        key="_email_campaign_dialog_choice",
        help="Надалі сюди додасться решта кампаній.",
    )
    if st.button("Готово", type="primary", use_container_width=True):
        st.session_state[_SESSION_CAMPAIGN] = choice
        st.rerun()


def render_email_text_generation() -> None:
    st.subheader("Email Text Generation")

    if _SESSION_CAMPAIGN not in st.session_state:
        st.session_state[_SESSION_CAMPAIGN] = "General"

    hc1, hc2 = st.columns([1, 2])
    with hc1:
        if st.button("Обрати кампанію…", key="email_gen_open_campaign_dialog"):
            _dialog_pick_campaign()
    with hc2:
        st.markdown(f"**Кампанія:** `{st.session_state[_SESSION_CAMPAIGN]}`")

    if st.session_state[_SESSION_CAMPAIGN] != "General":
        st.info("Ця кампанія ще не підключена. Наразі доступна лише **General**.")
        return

    st.caption(
        "**Ліди** (обовʼязково): структура виходу **1:1 як у цьому CSV** (ті самі колонки й порядок). "
        "**Компанії** (опційно) потрібні лише щоб збагатити промпт (наприклад Short Description із другого файлу); **окремих стовпчиків із файлу компаній у завантаженому результаті не буде**. "
        "Два CSV склеюються за ключем нижче; у **кінці** файлу лида зʼявляється **Subject на ланцюжок** і **тіла покроково**; нахил тексту залежить від **Title** (наприклад MD/CEO — бізнес і гроші, а не дрібна техніка; IT/CTO — можна глибше в продукт і системи), стиль GENERAL / processed email_generation (без Hi/підписів у тілі)."
    )

    if "email_gen_parallel_workers" not in st.session_state:
        st.session_state["email_gen_parallel_workers"] = 8
    if "email_gen_model_input" not in st.session_state:
        st.session_state["email_gen_model_input"] = "gpt-4o-mini"

    with st.expander("Модель OpenAI і швидкість"):
        st.text_input(
            "Модель",
            key="email_gen_model_input",
            help="Наприклад gpt-4o-mini або gpt-4o.",
        )
        st.number_input(
            "Паралельні запити до API (одночасно)",
            min_value=1,
            max_value=32,
            key="email_gen_parallel_workers",
            help=(
                "Раніше все йшло **по одному** контакту — тому 300 рядків могли займати години. "
                "Значення 8–16 зазвичай дає зручний компроміс між швидкістю і лімітами OpenAI (429). "
                "Якщо отримуєте помилки rate limit — зменшіть до 3–5."
            ),
        )

    u1, u2 = st.columns(2)
    with u1:
        uploaded_leads = st.file_uploader(
            "1) CSV контактів (ліди)",
            type=["csv"],
            key="email_gen_csv_leads",
        )
    with u2:
        uploaded_companies = st.file_uploader(
            "2) CSV компаній (опційно; наприклад Short Description)",
            type=["csv"],
            key="email_gen_csv_companies",
        )

    if uploaded_leads is None:
        st.session_state.pop("email_gen_out_csv", None)
        st.session_state.pop("email_gen_log", None)
        st.session_state.pop(_SESSION_UPLOAD_SIG, None)
        return

    mode_labels = {
        "apollo_account_id": "Apollo Account Id — поле є в обох файлах",
        "website_domain": "Website / домен (наприклад zofre.de)",
        "company_name": "Назва компанії (нормалізація юрформ)",
    }
    pick = st.radio(
        "Як склеїти файл лидів із файлом компаній (якщо компанію завантажено):",
        options=list(mode_labels.keys()),
        format_func=lambda k: mode_labels[k],
        horizontal=True,
        key="email_gen_join_mode_pick",
        help=(
            "Спочатку береться рядок компанії за ключем, поверх накладається рядок ліду "
            "(поля контакту переважають при однаковій назві колонки). "
            "Якщо компанію для ключа не знайдено — у промпті лишаться лище дані з лиду."
        ),
    )
    join_on = pick

    raw_leads = uploaded_leads.getvalue()
    comp_bytes = uploaded_companies.getvalue() if uploaded_companies else None
    lead_nm = uploaded_leads.name or ""
    co_nm = (uploaded_companies.name or "") if uploaded_companies else ""

    if not comp_bytes:
        st.caption("Другий файл не обрано — працюємо лише з таблицею лидів (без злиття).")

    sig = f"L:{len(raw_leads)}:{lead_nm}|C:{len(comp_bytes or b'')}|{co_nm}|J:{join_on}"
    if st.session_state.get(_SESSION_UPLOAD_SIG) != sig:
        st.session_state.pop("email_gen_out_csv", None)
        st.session_state.pop("email_gen_log", None)
        st.session_state[_SESSION_UPLOAD_SIG] = sig

    try:
        _headers, n_slots, n_rows, merge_hint = peek_merged_general_email_csv(raw_leads, comp_bytes, join_on)
    except ValueError as exc:
        st.error(str(exc))
        return
    except Exception as exc:
        st.error(f"Не вдалося прочитати CSV: {exc}")
        return

    if merge_hint:
        st.success(
            f"**Ліди** ({lead_nm}) + **Компанії** ({co_nm}): **{n_rows:,}** рядків; "
            f"**1× Subject** (усі кроки) + **{n_slots}** тіл без привітань/підписів у тексті листа · UTF-8-sig."
        )
        st.info(merge_hint)
    else:
        st.success(
            f"Файл **{lead_nm}**: **{n_rows:,}** контактів; **1× Subject** + **{n_slots}** тіл без привітань/підписів у тілі. UTF-8-sig."
        )

    if n_rows > 150:
        st.warning(
            f"Багато рядків ({n_rows:,}). У розгортці **«Модель OpenAI і швидкість»** збільшіть "
            "**паралельні запити** (наприклад 8–12) — інакше обробка йде по одному контакту і дуже довго. "
            "При 429 / rate limit від OpenAI зменште паралельність."
        )

    api_key = openai_api_key_effective()
    prog = st.progress(0)
    lbl = st.empty()

    run = st.button("Згенерувати тіла листів", type="primary", key="email_gen_run_general")

    if not run:
        prog.progress(0)
        lbl.caption("")
        if st.session_state.get("email_gen_out_csv"):
            st.success(f"Результат уже готовий (**{n_rows}** рядків). Завантажте CSV нижче.")
            st.download_button(
                "Завантажити той самий CSV лидів + колонки з текстами",
                data=st.session_state["email_gen_out_csv"],
                file_name=_out_filename(lead_nm),
                mime="text/csv",
                key="email_gen_dl_last",
                type="secondary",
            )
        return

    if not api_key.strip():
        st.error("Додайте OpenAI API key на головній сторінці або через Secrets.")
        prog.progress(0)
        lbl.caption("")
        return

    mdl = (st.session_state.get("email_gen_model_input") or "gpt-4o-mini").strip()

    mw = int(st.session_state.get("email_gen_parallel_workers") or 8)
    mw = max(1, min(mw, 32))

    def _prog(cur: int, total: int, msg: str) -> None:
        if total > 0:
            prog.progress(min(cur / total, 1.0))
        lbl.caption(msg)

    log_text = ""
    try:
        with prevent_idle_sleep():
            out_csv, log_text = run_general_campaign_on_csv_bytes(
                raw_leads,
                api_key=api_key,
                model=mdl,
                companies_data=comp_bytes if comp_bytes else None,
                join_on=join_on,
                max_workers=mw,
                on_progress=_prog,
            )
    except Exception as exc:
        prog.progress(0)
        lbl.caption("")
        st.error(str(exc))
        return

    if not out_csv:
        prog.progress(0)
        lbl.caption("")
        st.error(log_text or "Пустий результат.")
        return

    st.session_state["email_gen_out_csv"] = out_csv
    st.session_state["email_gen_log"] = log_text

    prog.progress(1.0)
    lbl.caption("Готово.")
    st.success(f"Згенеровано **{n_rows}** рядків. Натисніть кнопку завантаження нижче.")

    if log_text.strip():
        with st.expander("Журнал / попередження"):
            st.code(log_text, language="text")

    st.download_button(
        "Завантажити той самий CSV лидів + колонки з текстами",
        data=out_csv,
        file_name=_out_filename(lead_nm),
        mime="text/csv",
        key="email_gen_dl_fresh",
    )


def _out_filename(upload_name: str) -> str:
    """Імʼя зберігає основу файлу лидів; суфікс лише позначає додані поля (не «інший» експорт)."""
    raw = upload_name.strip() if upload_name else "leads.csv"
    stem = raw.rsplit("/", 1)[-1].removesuffix(".csv").removesuffix(".CSV")
    if not stem:
        stem = "leads"
    return f"{stem}_email_text_added.csv"
