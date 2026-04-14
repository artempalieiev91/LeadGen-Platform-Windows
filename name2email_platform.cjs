/**
 * Puppeteer + CDP (localhost:9222) — логіка з name2email-full-cleanlog-final.js,
 * але: повний CSV з заголовками, колонки з meta.json (як у gmail_name2email_client.py).
 */
"use strict";

const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer-core");
const { parse } = require("csv-parse/sync");
const { stringify } = require("csv-stringify/sync");

// Як у name2email-full-cleanlog-final.js (Name2Email-PythonMixed-Windows)
const BROWSER_URL = process.env.NAME2EMAIL_BROWSER_URL || "http://localhost:9222";
const INPUT = process.env.NAME2EMAIL_INPUT || "Input.csv";
const OUTPUT = process.env.NAME2EMAIL_OUTPUT || "Output_With_Emails.csv";
const META = process.env.NAME2EMAIL_META || "meta.json";

const POLL_MS = 200;
const POLL_MAX_MS = 45000;
const STABLE_NOT_FOUND = 4;
const TYPE_DELAY = 50;
// Як LOGIN_POLL_MAX_SEC у gmail_name2email_client.py (Windows 900 с)
const LOGIN_WAIT_MS = parseInt(process.env.NAME2EMAIL_LOGIN_WAIT_MS || "900000", 10);
const LOGIN_POLL_MS = 2000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function rndDelay(min = 1000, max = 2000) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function emitProgress(cur, total, query, email, status) {
  console.log(
    "N2E_PROGRESS\t" +
      JSON.stringify({
        current: cur,
        total: total,
        query: query || "",
        email: email || "",
        status: status || "",
      })
  );
}

async function isName2EmailSpinnerVisible(page) {
  return page.evaluate(() => {
    function isVisible(el) {
      if (!el || !el.getBoundingClientRect) return false;
      const st = window.getComputedStyle(el);
      if (st.display === "none" || st.visibility === "hidden") return false;
      if (Number.parseFloat(st.opacity || "1") === 0) return false;
      const r = el.getBoundingClientRect();
      return r.width >= 2 && r.height >= 2;
    }
    const exactSelectors = [
      "div.nametoemail-spinner",
      ".nametoemail-spinner",
      '[class*="nametoemail-spinner"]',
      '[class*="name2email-spinner"]',
    ];
    for (const sel of exactSelectors) {
      const el = document.querySelector(sel);
      if (el && isVisible(el)) return true;
    }
    const roots = document.querySelectorAll(
      '[class*="nametoemail"], [class*="name2email"], [class*="Name2Email"]'
    );
    for (const el of roots) {
      const c = String(el.className || "");
      if (!/(spinner|spinn|loader|-loading\b|is-loading|_loading\b)/i.test(c)) continue;
      if (isVisible(el)) return true;
    }
    let toEl = null;
    for (const sel of [
      'input[aria-label*="Кому"]',
      'input[aria-label*="To"]',
      'textarea[aria-label*="To"]',
      'input[name="to"]',
    ]) {
      const cand = document.querySelector(sel);
      if (cand && cand.getBoundingClientRect().width > 0) {
        toEl = cand;
        break;
      }
    }
    if (toEl) {
      const root =
        toEl.closest('[role="dialog"]') ||
        toEl.closest("[data-compose-id]") ||
        toEl.closest("form") ||
        document.body;
      const tr = toEl.getBoundingClientRect();
      for (const bar of root.querySelectorAll('[role="progressbar"]')) {
        if (!isVisible(bar)) continue;
        const br = bar.getBoundingClientRect();
        if (br.top < tr.top - 4) continue;
        if (br.top > tr.bottom + 140) continue;
        return true;
      }
      for (const svg of root.querySelectorAll("svg")) {
        if (!isVisible(svg)) continue;
        const sr = svg.getBoundingClientRect();
        if (sr.width < 14 || sr.width > 56 || sr.height < 14 || sr.height > 56) continue;
        if (sr.top < tr.bottom - 6) continue;
        if (sr.top > tr.bottom + 140) continue;
        const arc =
          svg.querySelector("circle[stroke-dasharray]") ||
          svg.querySelector("path[stroke-dasharray]");
        if (arc && arc.getAttribute("stroke")) return true;
      }
    }
    return false;
  });
}

function pageShowsNotFound(page) {
  return page.evaluate(() =>
    Array.from(document.querySelectorAll("div")).some(
      (el) => el.innerText && el.innerText.includes("Emails were not found.")
    )
  );
}

/** Як у name2email-full-cleanlog-final.js: лише поле одержувача, не рядок «Поиск в почте» (той теж role=combobox). */
const TO_FIELD_SELECTORS = [
  'input[aria-label*="Кому"]',
  'input[aria-label*="To"]',
  'textarea[aria-label*="To"]',
  'input[name="to"]',
  'textarea[name="to"]',
  'input[aria-label="To recipients"]',
  'input[aria-label*="Одержувач"]',
  'input[aria-label*="Получ"]',
  'input[aria-label*="Recipients"]',
  'div[role="combobox"][aria-label*="To"]',
  'div[role="combobox"][aria-label*="Кому"]',
  'div[contenteditable="true"][aria-label*="To"]',
  'div[contenteditable="true"][aria-label*="Кому"]',
];

async function findToField(page) {
  const handle = await page.evaluateHandle((sels) => {
    const dialogs = [...document.querySelectorAll('div[role="dialog"]')];
    for (let i = dialogs.length - 1; i >= 0; i--) {
      const d = dialogs[i];
      for (const sel of sels) {
        const node = d.querySelector(sel);
        if (!node) continue;
        const r = node.getBoundingClientRect();
        if (r.width > 0) return node;
      }
    }
    return null;
  }, TO_FIELD_SELECTORS);
  const element = handle.asElement();
  if (element) return element;
  await handle.dispose();
  return null;
}

async function ensureComposeReady(page) {
  let to = await findToField(page);
  if (to) return;
  await page.waitForSelector("div.T-I.T-I-KE.L3", { visible: true, timeout: 60000 }).catch(() => {});
  const composeBtn = await page.$("div.T-I.T-I-KE.L3");
  if (composeBtn) await composeBtn.click();
  await page.waitForFunction(
    () => {
      const dialogs = [...document.querySelectorAll('div[role="dialog"]')];
      const d = dialogs[dialogs.length - 1];
      if (!d) return false;
      for (const sel of TO_FIELD_SELECTORS) {
        const node = d.querySelector(sel);
        if (node) {
          const r = node.getBoundingClientRect();
          if (r.width > 0) return true;
        }
      }
      return false;
    },
    { timeout: 20000 }
  );
}

/** Ті самі маркери, що _ensure_name2email_logged_in у gmail_name2email_client.py — готовність розширення. */
const N2E_LOGGED_IN_MARKERS = [
  "Save contacts",
  "Need to search in bulk?",
  "Зберегти контакти",
  "Сохранить контакты",
];

async function pageHasName2EmailReadyMarkers(page) {
  return page.evaluate((markers) => {
    const t = document.body ? document.body.innerText : "";
    return markers.some((m) => t.includes(m));
  }, N2E_LOGGED_IN_MARKERS);
}

async function closeComposeDialog(page) {
  await page
    .evaluate(() => {
      const dialogs = [...document.querySelectorAll('div[role="dialog"]')];
      const d = dialogs[dialogs.length - 1];
      if (!d) return;
      const sels = [
        '[aria-label="Close"]',
        '[aria-label*="Close"]',
        '[aria-label*="Закрыть"]',
        '[aria-label*="закрити"]',
      ];
      for (const sel of sels) {
        const btn = d.querySelector(sel);
        if (btn) {
          btn.click();
          return;
        }
      }
    })
    .catch(() => {});
  await sleep(400);
  await page.keyboard.press("Escape").catch(() => {});
}

/**
 * Чекаємо появи маркерів авторизації Name2Email (не лише зникнення «Initializing…»).
 * Після ~120 с на екрані ініціалізації — один раз закриваємо лист і відкриваємо знову (як у підказці Python).
 */
async function waitForName2EmailExtensionReady(page) {
  const start = Date.now();
  let lastLog = 0;
  let composeRetryDone = false;

  while (Date.now() - start < LOGIN_WAIT_MS) {
    if (await pageHasName2EmailReadyMarkers(page)) {
      console.log("Name2Email: markers detected (extension ready).");
      return;
    }

    const elapsed = Date.now() - start;
    const initVisible = await page.evaluate(() =>
      String(document.body && document.body.innerText).includes("Initializing Name2Email")
    );

    if (!composeRetryDone && elapsed > 120000 && initVisible) {
      composeRetryDone = true;
      console.log(
        "Name2Email still shows «Initializing…» after 120s — closing compose and reopening once (same as manual fix)."
      );
      await closeComposeDialog(page);
      await sleep(2000);
      await ensureComposeReady(page);
    }

    if (Date.now() - lastLog > 45000) {
      lastLog = Date.now();
      console.log(
        `Waiting for Name2Email (Save contacts / Зберегти контакти / …) — ${Math.floor(
          (Date.now() - start) / 1000
        )}s / ${Math.floor(LOGIN_WAIT_MS / 1000)}s`
      );
    }

    await sleep(LOGIN_POLL_MS);
  }

  throw new Error(
    "Name2Email: за " +
      Math.floor(LOGIN_WAIT_MS / 1000) +
      " с не з’явились маркери готовності (Save contacts тощо). " +
      "Якщо вікно зависло на «Initializing Name2Email»: оновіть розширення, перевірте VPN/мережу, " +
      "увійдіть у Name2Email у цьому профілі Chrome; можна закрити лист і відкрити «Написати» знову — потім перезапустіть."
  );
}

async function processQuery(page, query) {
  const toField = await findToField(page);
  if (!toField) return { email: "", status: "skipped" };

  await toField.click({ clickCount: 3 });
  await page.keyboard.down("Control");
  await page.keyboard.press("a");
  await page.keyboard.up("Control");
  await page.keyboard.press("Backspace");
  await sleep(300);
  await toField.click();
  await page.evaluate((el) => el.focus(), toField);
  await sleep(200);
  await toField.type(query, { delay: TYPE_DELAY });

  const spaceIndex = query.indexOf(" ");
  if (spaceIndex !== -1) {
    await page.keyboard.press("Home");
    for (let j = 0; j <= spaceIndex; j++) await page.keyboard.press("ArrowRight");
    await page.keyboard.press("Backspace");
    await page.keyboard.press(" ");
  }

  let foundEmail = "";
  const maxTries = Math.ceil(POLL_MAX_MS / POLL_MS);
  let stableNotFound = 0;

  for (let t = 0; t < maxTries; t++) {
    const pill = await page.$("div.M9 span[email]");
    if (pill) {
      const email = await page.evaluate((el) => el.getAttribute("email"), pill);
      if (email) {
        foundEmail = email.trim();
        break;
      }
    }
    if (await isName2EmailSpinnerVisible(page)) {
      stableNotFound = 0;
      await sleep(POLL_MS);
      continue;
    }
    if (await pageShowsNotFound(page)) {
      stableNotFound += 1;
      if (stableNotFound >= STABLE_NOT_FOUND) break;
    } else {
      stableNotFound = 0;
    }
    await sleep(POLL_MS);
  }

  if (!foundEmail) {
    const finalPill = await page.$("div.M9 span[email]");
    if (finalPill) {
      const email = await page.evaluate((el) => el.getAttribute("email"), finalPill);
      if (email) foundEmail = email.trim();
    }
  }

  return { email: foundEmail, status: foundEmail ? "found" : "not_found" };
}

function collectTodo(rows, emailCol, qCol, ph) {
  const phL = (ph || "").trim().toLowerCase();
  const todo = [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const q = String(row[qCol] || "").trim();
    if (!q || ["query", "research emails", "emails research"].includes(q.toLowerCase())) continue;
    const em = String(row[emailCol] || "").trim();
    if (!em) {
      todo.push(i);
      continue;
    }
    if (em.toLowerCase() === phL) continue;
  }
  return todo;
}

(async () => {
  const metaRaw = fs.readFileSync(META, "utf-8");
  const meta = JSON.parse(metaRaw);
  const { email_col: emailCol, query_col: qCol, fieldnames, email_not_found_placeholder: ph } =
    meta;

  const buf = fs.readFileSync(INPUT, "utf-8");
  const rows = parse(buf, {
    columns: true,
    bom: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  });

  const todo = collectTodo(rows, emailCol, qCol, ph);
  emitProgress(0, Math.max(todo.length, 1), "", "", "init");

  const projectRow = (r) => {
    const o = {};
    for (const k of fieldnames) o[k] = r[k] != null ? r[k] : "";
    return o;
  };

  if (todo.length === 0) {
    const out = stringify(rows.map(projectRow), { header: true, columns: fieldnames });
    fs.writeFileSync(OUTPUT, "\ufeff" + out, "utf-8");
    console.log("Nothing to do — wrote CSV with headers.");
    process.exit(0);
  }

  console.log(`Connecting to Chrome at ${BROWSER_URL}`);
  const browser = await puppeteer.connect({ browserURL: BROWSER_URL });
  let pages = await browser.pages();
  let page = pages.find((p) => p.url().includes("mail.google.com"));
  if (!page) {
    page = await browser.newPage();
    await page.goto("https://mail.google.com", { waitUntil: "domcontentloaded" });
  }
  await ensureComposeReady(page);
  await waitForName2EmailExtensionReady(page);

  let done = 0;
  for (const rowIndex of todo) {
    const row = rows[rowIndex];
    const query = String(row[qCol] || "").trim();
    done += 1;
    console.log(`Row ${rowIndex + 2} query: ${query}`);
    const { email, status } = await processQuery(page, query);
    emitProgress(done, todo.length, query, email, status);
    if (status === "found" && email) row[emailCol] = email;
    else if (status === "not_found") row[emailCol] = ph || "email not found";

    const out = stringify(rows.map(projectRow), { header: true, columns: fieldnames });
    fs.writeFileSync(OUTPUT, "\ufeff" + out, "utf-8");

    if (done < todo.length) await sleep(rndDelay());
  }

  console.log(`Done. Saved: ${OUTPUT}`);
  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
