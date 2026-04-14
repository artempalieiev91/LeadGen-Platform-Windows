# LeadGen Platform

Streamlit-застосунок: Research Validation, Sheets Preparation, MathcURLs, Name2Emails (локально на **Windows** або macOS).

## Швидкий старт (термінал)

### Windows (PowerShell або cmd)

```powershell
cd "C:\шлях\до\Streamlit Platform"
py -3 -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
copy .streamlit\secrets.toml.example .streamlit\secrets.toml
# Відредагуйте secrets.toml: [auth] username / password (обов’язково)
streamlit run streamlit_app.py
```

### macOS / Linux

```bash
cd "/шлях/до/Streamlit Platform"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Відредагуйте secrets.toml: [auth] username / password (обов’язково)
streamlit run streamlit_app.py
```

Відкриється браузер (за замовчуванням `http://localhost:8501`).

## PyCharm

1. **File → Open** — виберіть папку проєкту.
2. **Settings → Project → Python Interpreter** — додайте `.venv` (або створіть venv і виконайте `pip install -r requirements.txt` у терміналі PyCharm).
3. **Run → Edit Configurations → + → Python**:
   - **Script path:** `run_streamlit.py` (у корені проєкту).
   - **Working directory:** корінь проєкту.
4. Запуск: **Run** (▶) або **Shift+F10**.

Альтернатива без скрипта: **Module name** `streamlit`, **Parameters** `run streamlit_app.py`, режим **Run with Python module**.

У репозиторії є готова конфігурація **Streamlit LeadGen**: `.idea/runConfigurations/Streamlit_LeadGen.xml` — після відкриття проєкту виберіть її у списку Run і призначте інтерпретатор з `.venv`.

## Секрети

- Локально: файл `.streamlit/secrets.toml` (не комітиться). Приклад — `.streamlit/secrets.toml.example`.
- Потрібна секція **`[auth]`** з `username` і `password`, інакше вхід заблоковано.
- Для Telegram-сповіщень — `[telegram]` з `bot_token` (див. приклад).

## Name2Emails

Потрібен локальний **Google Chrome** на тому ж ПК, де запущено Streamlit. Підтримуються **Windows** і **macOS**; на типовому хмарному Linux-хостингу вкладка свідомо недоступна.

Якщо Chrome встановлено не в стандартну папку, задайте змінну середовища **`CHROME_PATH`** (повний шлях до `chrome.exe` на Windows або до виконуваного файлу Chrome на macOS).
