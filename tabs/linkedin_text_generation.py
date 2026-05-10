"""Вкладка генерації тексту для повідомлень / постів під LinkedIn."""

from __future__ import annotations

import streamlit as st

from services.copy_generation import generate_marketing_copy
from services.platform_openai import openai_api_key_effective


def render_linkedin_text_generation() -> None:
    st.subheader("LinkedIn Text Generation")

    st.markdown(
        """
**Призначення.** Чернетка короткого повідомлення для LinkedIn (InMail або connect note) або поста за вашим брифом.  
Ключ **OpenAI** — на головній сторінці або `openai_api_key` у Secrets.
        """
    )

    if "linkedin_txt_model" not in st.session_state:
        st.session_state["linkedin_txt_model"] = "gpt-4o-mini"

    model = st.text_input(
        "Модель OpenAI",
        key="linkedin_txt_model",
        help="Наприклад: gpt-4o-mini, gpt-4o.",
    )

    kind = st.radio(
        "Формат",
        options=["Повідомлення (InMail / note)", "Пост у стрічці"],
        key="linkedin_txt_kind",
        horizontal=True,
    )

    audience = st.text_input("Аудиторія / кому пишемо", key="linkedin_txt_audience", placeholder="HRD у IT, фаундер SaaS…")
    hook = st.text_area(
        "Головна думка / hook",
        key="linkedin_txt_hook",
        height=90,
        placeholder="Що саме хочете донести за 1–2 речення…",
    )
    context = st.text_area(
        "Деталі, тон, мова, обмеження по символах",
        key="linkedin_txt_context",
        height=160,
        placeholder="LinkedIn connect note часто до 300 символів — вкажіть ліміт, якщо треба…",
    )

    if st.button("Згенерувати текст", type="primary", key="linkedin_txt_generate"):
        key = openai_api_key_effective()
        fmt = "коротке персональне повідомлення (InMail або invitation note)" if "Повідомлення" in kind else "пост для стрічки LinkedIn"
        user_block = "\n".join(
            [
                f"Формат: {fmt}",
                f"Аудиторія: {audience.strip() or '—'}",
                "",
                "Головна думка:",
                hook.strip() or "—",
                "",
                "Деталі та обмеження:",
                context.strip() or "—",
            ]
        )
        system = (
            "Ти допомагаєш писати тексти для LinkedIn. Дотримуйся лімітів і мови з запиту користувача. "
            "Не вигадуй метрик і фактів без підстави. Уникай кліше на кшталт «Hope this finds you well» без потреби."
        )
        with st.spinner("Запит до OpenAI…"):
            text, err = generate_marketing_copy(
                api_key=key,
                model=model,
                system_prompt=system,
                user_content=user_block,
            )
        if err:
            st.error(err)
            return
        st.session_state["linkedin_txt_output"] = text

    if "linkedin_txt_output" not in st.session_state:
        st.session_state["linkedin_txt_output"] = ""

    st.markdown("##### Результат")
    st.text_area("Текст", key="linkedin_txt_output", height=260)
