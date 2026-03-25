#!/usr/bin/env node
const { chromium } = require("playwright-core");

function getArg(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] ?? fallback;
}

function toInt(v, fallback) {
  const n = parseInt(String(v), 10);
  if (Number.isNaN(n)) return fallback;
  return n;
}

function toBool(v, fallback) {
  if (v === "") return fallback;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(s)) return true;
  if (["0", "false", "no", "off"].includes(s)) return false;
  return fallback;
}

(async () => {
  const url = getArg("--url");
  if (!url) {
    throw new Error("missing --url");
  }

  const headless = toBool(getArg("--headless", "true"), true);
  const timeoutMs = toInt(getArg("--timeout-ms", "60000"), 60000);
  const waitAfterLoadMs = toInt(getArg("--wait-after-load-ms", "3000"), 3000);
  const challengeWaitMs = toInt(getArg("--challenge-wait-ms", "20000"), 20000);
  const maxReloads = toInt(getArg("--max-reloads", "2"), 2);
  const manualWaitSec = toInt(getArg("--manual-wait-seconds", "0"), 0);
  const userDataDir = getArg("--user-data-dir", "");
  const browserPath = getArg("--browser-path", "");

  const ua =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

  const launchOpts = {
    headless,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-default-browser-check",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--window-size=1366,900",
    ],
    timeout: timeoutMs,
  };
  const disableSandbox =
    String(process.env.PLAYWRIGHT_DISABLE_SANDBOX || "true").toLowerCase() !== "false";
  if (disableSandbox) {
    launchOpts.args.push("--no-sandbox", "--disable-setuid-sandbox");
  }
  if (process.env.PLAYWRIGHT_EXTRA_ARGS) {
    process.env.PLAYWRIGHT_EXTRA_ARGS
      .split(/\s+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .forEach((arg) => launchOpts.args.push(arg));
  }
  if (browserPath) {
    launchOpts.executablePath = browserPath;
  }

  let browser = null;
  let context = null;
  try {
    if (userDataDir) {
      context = await chromium.launchPersistentContext(userDataDir, {
        ...launchOpts,
        userAgent: ua,
        locale: "zh-CN",
        extraHTTPHeaders: {
          "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
          Referer: "https://www.price.com.hk/",
        },
      });
    } else {
      browser = await chromium.launch(launchOpts);
      context = await browser.newContext({
        userAgent: ua,
        locale: "zh-CN",
        extraHTTPHeaders: {
          "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
          Referer: "https://www.price.com.hk/",
        },
      });
    }

    const page = context.pages().length > 0 ? context.pages()[0] : await context.newPage();
    let lastHTML = "";
    let title = "";
    let finalURL = "";
    for (let i = 0; i <= maxReloads; i += 1) {
      if (i === 0) {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      } else {
        await page.reload({ waitUntil: "domcontentloaded", timeout: timeoutMs });
      }
      if (waitAfterLoadMs > 0) {
        await page.waitForTimeout(waitAfterLoadMs);
      }
      const waitUntil = Date.now() + challengeWaitMs;
      while (Date.now() < waitUntil) {
        title = (await page.title()) || "";
        lastHTML = await page.content();
        finalURL = page.url();
        const lowerTitle = title.toLowerCase();
        const lowerHTML = lastHTML.toLowerCase();
        const challenged =
          lowerTitle.includes("just a moment") ||
          lowerHTML.includes("cf-mitigated") ||
          lowerHTML.includes("cf-challenge") ||
          (lowerHTML.includes("cloudflare") && lowerHTML.includes("checking your browser"));
        if (!challenged) {
          break;
        }
        await page.waitForTimeout(1500);
      }
      const lowerTitle = title.toLowerCase();
      const lowerHTML = lastHTML.toLowerCase();
      const stillChallenged =
        lowerTitle.includes("just a moment") ||
        lowerHTML.includes("cf-mitigated") ||
        lowerHTML.includes("cf-challenge") ||
        (lowerHTML.includes("cloudflare") && lowerHTML.includes("checking your browser"));
      if (!stillChallenged) {
        break;
      }
    }
    if (!headless && manualWaitSec > 0) {
      await page.waitForTimeout(manualWaitSec * 1000);
    }
    const html = await page.content();
    title = await page.title();
    finalURL = page.url();

    process.stdout.write(
      JSON.stringify({
        html,
        title,
        url: finalURL,
      })
    );
  } finally {
    if (context) {
      await context.close().catch(() => {});
    }
    if (browser) {
      await browser.close().catch(() => {});
    }
  }
})().catch((err) => {
  const msg = err && err.stack ? err.stack : String(err);
  process.stderr.write(msg);
  process.exit(1);
});
