# Name2Email Node runner (Puppeteer + CDP)

Один раз у цій папці виконайте:

```bash
npm install
```

Потрібні **Node.js** (LTS) і доступ до інтернету для завантаження `puppeteer-core`, `csv-parse`, `csv-stringify`.

Платформа за замовчуванням використовує цей шлях (як [name2email.py](https://github.com/artempalieiev91/Name2Email-PythonMixed-Windows) у тому репо): **лише** піднімається Chrome з `--remote-debugging-port=9222`, далі пошук робить `name2email_platform.cjs` через **puppeteer.connect** — без Playwright перед Node (інакше можливе зависання на «Initializing Name2Email»).

Після відкриття «Написати» скрипт **чекає маркери готовності** Name2Email (як у `gmail_name2email_client.py`: «Save contacts», «Need to search in bulk?», «Зберегти контакти» тощо), до **900 с** (`NAME2EMAIL_LOGIN_WAIT_MS`). Якщо ~2 хв все ще «Initializing…», один раз закривається лист і відкривається знову. Якщо маркери так і не з’явились — помилка з поясненням (не продовжуємо «сліпо»).

Якщо `npm install` не зроблено, у журналі буде повідомлення й увімкнеться резервний режим лише через Playwright.
