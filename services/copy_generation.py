"""Спільний виклик Chat Completions для вкладок генерації тексту (email / LinkedIn)."""

from __future__ import annotations

from openai import OpenAI

from services.platform_openai import configure_openai_http_client


def generate_marketing_copy(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout_sec: float = 120.0,
) -> tuple[str | None, str | None]:
    """Повертає (текст відповіді або None, повідомлення про помилку або None)."""
    k = (api_key or "").strip()
    if not k:
        return None, "Немає API ключа OpenAI — введіть його на головній сторінці або в Secrets (`openai_api_key`)."
    m = (model or "").strip() or "gpt-4o-mini"
    try:
        client = OpenAI(api_key=k, timeout=timeout_sec)
        configure_openai_http_client(client)
        resp = client.chat.completions.create(
            model=m,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.65,
        )
        choice = resp.choices[0].message.content
        if not choice or not str(choice).strip():
            return None, "Модель повернула порожню відповідь."
        return str(choice).strip(), None
    except Exception as exc:
        return None, str(exc)
