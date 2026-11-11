/**
 * DeepSeek Dual Mode — Host half.
 *
 * Responsibilities:
 *  - Own the persistent DeepSeek web-chat browser (real chat.deepseek.com UI via CDP).
 *  - Persist web-chat sessions to the profile data directory (JSON, atomic writes).
 *  - Expose a small JSON/SSE HTTP surface under /dual-mode/ for the Client half.
 *  - Bridge instructions and execution summaries to real Harness sessions through
 *    the official sessionController / workspaceRegistry / workspaceChanges services.
 *  - Build deterministic (no API cost) task extractions and secret-masked payloads.
 *
 * No Harness database is touched directly and no DeepSeek API key is used.
 */
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const PLUGIN_ID = 'dsh-dual-mode';
const ROUTE = '/dual-mode';
const DEFAULT_PORT = 9410;
const DEFAULT_QUESTION = '请帮我分析以上内容，指出潜在问题，并给出你的建议。暂时不要修改代码。';

/* ------------------------------------------------------------------ */
/* small utilities                                                     */
/* ------------------------------------------------------------------ */

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function profileDirectory() {
  if (process.env.DSH_PROFILE_DIR) return process.env.DSH_PROFILE_DIR;
  // The desktop host passes the active profile as a positional argument. Prefer the
  // argument that actually looks like a profile (contains cordis.patch.yml) so the
  // plugin stores its data next to the profile it is running in.
  for (const candidate of process.argv.slice(1)) {
    try {
      if (!candidate || !fs.statSync(candidate).isDirectory()) continue;
      if (fs.existsSync(path.join(candidate, 'cordis.patch.yml'))) return candidate;
    } catch {
      /* not a directory */
    }
  }
  if (process.env.DSH_HOME) return path.join(process.env.DSH_HOME, 'profiles', process.env.DSH_PROFILE || 'desktop');
  return path.join(os.homedir(), '.dsh');
}

function dataDirectory() {
  return path.join(profileDirectory(), 'dual-mode');
}

function readJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function writeJsonAtomic(file, value) {
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2), 'utf8');
  fs.renameSync(tmp, file);
}

function readBody(req, limit = 6 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new Error('request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (error) {
        reject(new Error(`invalid JSON body: ${error.message}`));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, value) {
  if (res.headersSent) return;
  const body = JSON.stringify(value);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

function sendError(res, error) {
  sendJson(res, 200, { ok: false, error: error instanceof Error ? error.message : String(error) });
}

/* ------------------------------------------------------------------ */
/* secret masking                                                      */
/* ------------------------------------------------------------------ */

const SECRET_RULES = [
  { re: /\bsk-[A-Za-z0-9_-]{10,}\b/g, label: '[已脱敏:KEY]' },
  { re: /\b(?:ghp|gho|ghs|ghu)_[A-Za-z0-9]{20,}\b/g, label: '[已脱敏:TOKEN]' },
  { re: /\bAKIA[0-9A-Z]{16}\b/g, label: '[已脱敏:AWS]' },
  { re: /\b(Bearer\s+)[A-Za-z0-9._~+/-]{8,}=*/gi, label: '$1[已脱敏]' },
  { re: /((?:api[_-]?key|access[_-]?token|secret|password|passwd|pwd)\s*[:=]\s*)["']?[^\s"',;]{6,}/gi, label: '$1[已脱敏]' },
  { re: /\b([A-Z][A-Z0-9_]{2,}_(?:KEY|TOKEN|SECRET|PASSWORD))\s*=\s*\S+/g, label: '$1=[已脱敏]' },
  { re: /\b(?:-----BEGIN [A-Z ]*PRIVATE KEY-----)[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----)/g, label: '[已脱敏:PRIVATE KEY]' },
];

function maskSecrets(text) {
  let out = String(text ?? '');
  for (const rule of SECRET_RULES) out = out.replace(rule.re, rule.label);
  return out;
}

/* ------------------------------------------------------------------ */
/* persistent web-chat store                                           */
/* ------------------------------------------------------------------ */

class ChatStore {
  constructor(dir, log = () => {}) {
    this.dir = dir;
    this.file = path.join(dir, 'webchats.json');
    this.log = log;
    const loaded = readJson(this.file, { version: 1, chats: [] });
    this.chats = Array.isArray(loaded?.chats) ? loaded.chats : [];
    this.dirty = false;
    this.timer = null;

    // A reply that was still streaming when Harness or the browser went away can
    // never resume; mark it clearly instead of leaving a spinner forever.
    let repaired = 0;
    for (const chat of this.chats) {
      for (const message of chat.messages || []) {
        if (message.status === 'streaming') {
          message.status = 'error';
          message.error = '上次生成未正常结束（页面关闭或连接中断）';
          repaired += 1;
        }
      }
    }
    if (repaired) this.saveNow();
  }

  list() {
    return [...this.chats].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
  }

  get(id) {
    return this.chats.find((chat) => chat.id === id);
  }

  create({ title, workspaceId = null } = {}) {
    const now = Date.now();
    const chat = {
      id: `web-${randomUUID()}`,
      webSessionId: null,
      title: title || '新的网页对话',
      messages: [],
      createdAt: now,
      updatedAt: now,
      workspaceId,
      connection: { state: 'unknown', checkedAt: 0, error: null },
      tasks: [],
    };
    this.chats.push(chat);
    this.saveNow();
    return chat;
  }

  update(id, patch) {
    const chat = this.get(id);
    if (!chat) throw new Error(`web chat not found: ${id}`);
    Object.assign(chat, patch, { updatedAt: Date.now() });
    this.scheduleSave();
    return chat;
  }

  remove(id) {
    const index = this.chats.findIndex((chat) => chat.id === id);
    if (index < 0) throw new Error(`web chat not found: ${id}`);
    const [chat] = this.chats.splice(index, 1);
    this.saveNow();
    return chat;
  }

  appendMessage(id, message) {
    const chat = this.get(id);
    if (!chat) throw new Error(`web chat not found: ${id}`);
    chat.messages.push(message);
    chat.updatedAt = Date.now();
    this.scheduleSave();
    return message;
  }

  updateMessage(chatId, messageId, patch) {
    const chat = this.get(chatId);
    const message = chat?.messages.find((m) => m.id === messageId);
    if (!message) return undefined;
    Object.assign(message, patch);
    chat.updatedAt = Date.now();
    this.scheduleSave();
    return message;
  }

  addTask(chatId, task) {
    const chat = this.get(chatId);
    if (!chat) throw new Error(`web chat not found: ${chatId}`);
    chat.tasks.unshift(task);
    chat.updatedAt = Date.now();
    this.scheduleSave();
    return task;
  }

  scheduleSave() {
    this.dirty = true;
    if (this.timer !== null) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      if (this.dirty) this.saveNow();
    }, 800);
    this.timer.unref?.();
  }

  saveNow() {
    this.dirty = false;
    try {
      writeJsonAtomic(this.file, { version: 1, chats: this.chats });
    } catch (error) {
      this.log(`dual-mode: failed to persist web chats: ${error.message}`);
    }
  }

  dispose() {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    if (this.dirty) this.saveNow();
  }
}

/* ------------------------------------------------------------------ */
/* CDP page client                                                     */
/* ------------------------------------------------------------------ */

class CdpPage {
  constructor(wsUrl, logger) {
    this.wsUrl = wsUrl;
    this.logger = logger;
    this.ws = null;
    this.nextId = 0;
    this.pending = new Map();
    this.closed = false;
  }

  async open() {
    this.ws = new WebSocket(this.wsUrl);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('CDP websocket timeout')), 15000);
      this.ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
      this.ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error('CDP websocket error')); }, { once: true });
    });
    this.ws.addEventListener('message', (event) => {
      let message;
      try {
        message = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data));
      } catch {
        return;
      }
      const entry = this.pending.get(message.id);
      if (!entry) return;
      this.pending.delete(message.id);
      if (message.error) entry.reject(new Error(message.error.message || 'CDP command failed'));
      else entry.resolve(message.result);
    });
    this.ws.addEventListener('close', () => {
      this.closed = true;
      for (const entry of this.pending.values()) entry.reject(new Error('CDP connection closed'));
      this.pending.clear();
    });
  }

  send(method, params = {}) {
    if (this.closed) return Promise.reject(new Error('CDP connection closed'));
    const id = ++this.nextId;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      setTimeout(() => {
        if (this.pending.delete(id)) reject(new Error(`CDP command timed out: ${method}`));
      }, 30000).unref?.();
    });
  }

  async evaluate(expression) {
    const result = await this.send('Runtime.evaluate', {
      expression,
      awaitPromise: true,
      returnByValue: true,
      userGesture: true,
    });
    if (result.exceptionDetails) {
      const detail = result.exceptionDetails.exception?.description || result.exceptionDetails.text;
      throw new Error(`page script failed: ${detail}`);
    }
    return result.result?.value;
  }

  async click(x, y) {
    await this.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
    await this.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
  }

  close() {
    this.closed = true;
    try { this.ws?.close(); } catch { /* ignore */ }
  }
}

/* ------------------------------------------------------------------ */
/* DeepSeek web adapter (real chat.deepseek.com UI)                    */
/* ------------------------------------------------------------------ */

const PAGE_HELPERS = String.raw`(() => {
  const q = (s) => document.querySelector(s);
  const qa = (s) => [...document.querySelectorAll(s)];
  const composer = () => q('textarea');
  const assistantNodes = () => qa('.ds-message > .ds-markdown');
  const isVisible = (el) => !!el && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
  return {
    href: location.href,
    title: document.title,
    hasComposer: !!composer(),
    signIn: /\/sign_in/.test(location.href),
    loginText: /扫码登录|密码登录|发送验证码/.test(document.body.innerText || ''),
    messageCount: assistantNodes().length,
    toggles: qa('.ds-toggle-button').map((t) => ({
      label: (t.innerText || '').trim(),
      selected: (t.className || '').toString().includes('ds-toggle-button--selected'),
    })),
    newChat: qa('[role="button"], a, span').some((e) => (e.textContent || '').trim() === '开启新对话'),
    stop: qa('.ds-button').some((b) => /停止|stop|中止/i.test(((b.getAttribute('aria-label') || '') + ' ' + (b.innerText || '') + ' ' + (b.className || '')))),
  };
})()`;

const READ_STATE = String.raw`(() => {
  const nodes = [...document.querySelectorAll('.ds-message > .ds-markdown')];
  const last = nodes[nodes.length - 1];
  const stop = [...document.querySelectorAll('.ds-button')].some((b) => /停止|stop|中止/i.test(((b.getAttribute('aria-label') || '') + ' ' + (b.innerText || '') + ' ' + (b.className || ''))));
  return {
    count: nodes.length,
    text: last ? (last.innerText || '') : '',
    stop,
    href: location.href,
    userCount: document.querySelectorAll('.ds-message > div.fbb737a4').length,
  };
})()`;

class WebChatAdapter {
  constructor({ dir, port, browserPath, headless, log }) {
    this.dir = dir;
    this.profileDir = path.join(dir, 'chrome-profile');
    this.port = port;
    this.browserPath = browserPath;
    this.log = log;
    this.page = null;
    this.pageCheckedAt = 0;
    this.launching = null;
    this.lastLaunchError = null;
    this.child = null;
    const settings = this.readSettings();
    this.headless = typeof headless === 'boolean' ? headless : settings.headless !== false;
  }

  readSettings() {
    try {
      return JSON.parse(fs.readFileSync(path.join(this.dir, 'settings.json'), 'utf8'));
    } catch { return {}; }
  }

  writeSettings(patch) {
    try {
      const next = { ...this.readSettings(), ...patch };
      fs.mkdirSync(this.dir, { recursive: true });
      fs.writeFileSync(path.join(this.dir, 'settings.json'), JSON.stringify(next, null, 2));
    } catch (error) {
      this.log(`dual-mode: settings write failed ${error.message}`);
    }
  }

  static detectBrowser() {
    const candidates = [
      'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
      'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
      'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
      'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
    ];
    return candidates.find((candidate) => fs.existsSync(candidate)) || null;
  }

  async cdp(pathname, init) {
    const response = await fetch(`http://127.0.0.1:${this.port}${pathname}`, init);
    if (!response.ok) throw new Error(`CDP HTTP ${response.status}`);
    return response.json();
  }

  async ensureBrowser() {
    try {
      await this.cdp('/json/version');
      return true;
    } catch { /* not running */ }
    if (this.launching) return this.launching;
    this.launching = (async () => {
      const executable = this.browserPath || WebChatAdapter.detectBrowser();
      if (!executable) throw new Error('未找到 Chrome/Edge，可在插件配置中填写 browserPath');
      fs.mkdirSync(this.profileDir, { recursive: true });
      this.log(`dual-mode: launching ${this.headless ? 'headless' : 'visible'} browser ${executable}`);
      const args = [
        `--remote-debugging-port=${this.port}`,
        `--user-data-dir=${this.profileDir}`,
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-features=OptimizationHints',
      ];
      if (this.headless) {
        args.push(
          '--headless=new',
          '--disable-gpu',
          '--window-size=1280,900',
          '--mute-audio',
          '--disable-background-timer-throttling',
          '--disable-backgrounding-occluded-windows',
          '--disable-renderer-backgrounding',
          '--disable-features=CalculateNativeWinOcclusion',
        );
      }
      args.push('https://chat.deepseek.com/');
      const child = spawn(executable, args, { detached: true, stdio: 'ignore' });
      child.unref();
      this.child = child;
      for (let i = 0; i < 120; i += 1) {
        try {
          await this.cdp('/json/version');
          return true;
        } catch {
          await sleep(250);
        }
      }
      throw new Error('浏览器启动超时（CDP 端口未就绪）');
    })().finally(() => { this.launching = null; });
    return this.launching;
  }

  async findPageTarget() {
    try {
      const targets = await this.cdp('/json/list');
      const pages = targets.filter((target) => target.type === 'page');
      const chatPage = pages.find((target) => target.url.includes('chat.deepseek.com'));
      if (chatPage) return chatPage;
      const blank = pages.find((target) => target.url === 'about:blank') || pages[0];
      if (!blank) return null;
      await fetch(`http://127.0.0.1:${this.port}/json/new?${encodeURIComponent('https://chat.deepseek.com/')}`, { method: 'PUT' }).catch(() => {});
      await sleep(600);
      const after = await this.cdp('/json/list');
      return after.filter((target) => target.type === 'page').find((target) => target.url.includes('chat.deepseek.com')) || null;
    } catch {
      return null;
    }
  }

  async connectPage() {
    if (this.page && !this.page.closed) return this.page;
    if (this.page && this.page.closed) this.page = null;
    const target = await this.findPageTarget();
    if (!target?.webSocketDebuggerUrl) return null;
    const page = new CdpPage(target.webSocketDebuggerUrl, this.log);
    await page.open();
    this.page = page;
    // Headless pages are treated as background tabs; without these the page gets
    // throttled or frozen and the streaming reply never arrives.
    await page.send('Emulation.setFocusEmulationEnabled', { enabled: true }).catch(() => {});
    await page.send('Page.setWebLifecycleState', { state: 'active' }).catch(() => {});
    return page;
  }

  async withPage() {
    await this.ensureBrowser();
    const page = await this.connectPage();
    if (!page) throw new Error('无法连接到浏览器页面');
    return page;
  }

  async status() {
    // Never launch a browser just to poll status: report whether the CDP
    // endpoint is reachable and only inspect the page when it is.
    try {
      await this.cdp('/json/version');
    } catch {
      return { state: 'disconnected', error: null, mode: this.headless ? 'headless' : 'visible', at: Date.now() };
    }
    let page;
    try {
      page = await this.connectPage();
    } catch (error) {
      this.lastLaunchError = error.message;
      return { state: 'disconnected', error: error.message, mode: this.headless ? 'headless' : 'visible', at: Date.now() };
    }
    if (!page) return { state: 'launching', error: null, mode: this.headless ? 'headless' : 'visible', at: Date.now() };
    try {
      const info = await page.evaluate(PAGE_HELPERS);
      let state = 'connected';
      if (info.signIn || (info.loginText && !info.hasComposer)) state = 'login-required';
      else if (!info.hasComposer) state = 'loading';
      return {
        state,
        error: null,
        url: info.href,
        title: info.title,
        toggles: info.toggles,
        mode: this.headless ? 'headless' : 'visible',
        at: Date.now(),
      };
    } catch (error) {
      return { state: 'disconnected', error: error.message, mode: this.headless ? 'headless' : 'visible', at: Date.now() };
    }
  }

  async stopBrowser() {
    const pid = this.child?.pid;
    this.child = null;
    this.page = null;
    try {
      if (pid) spawn('taskkill', ['/PID', String(pid), '/T', '/F'], { stdio: 'ignore', windowsHide: true });
    } catch { /* ignore */ }
    await sleep(800);
  }

  async setHeadless(enabled) {
    if (this.headless === enabled) return this.status();
    this.headless = enabled;
    this.writeSettings({ headless: enabled });
    await this.stopBrowser();
    await this.ensureBrowser();
    return this.status();
  }

  // Return to a stored web conversation so a reply can continue in the same
  // chat.deepseek.com thread after the browser was relaunched.
  async ensureSession(webSessionId) {
    const page = await this.withPage();
    if (!webSessionId) return page;
    let href = '';
    try { href = String(await page.evaluate('location.href')); } catch { /* ignore */ }
    if (href.includes(webSessionId)) return page;
    await page.send('Page.navigate', { url: `https://chat.deepseek.com/a/chat/s/${webSessionId}` });
    for (let i = 0; i < 40; i += 1) {
      await sleep(300);
      const info = await page.evaluate(PAGE_HELPERS).catch(() => null);
      if (info?.hasComposer) break;
    }
    return page;
  }

  async openLogin() {
    await this.ensureBrowser();
    const page = await this.connectPage();
    if (!page) throw new Error('浏览器未就绪');
    await page.send('Page.navigate', { url: 'https://chat.deepseek.com/' });
    await sleep(1200);
    try {
      await page.send('Page.bringToFront');
    } catch { /* ignore */ }
    return this.status();
  }

  async newChat() {
    const page = await this.withPage();
    const clicked = await page.evaluate(String.raw`(() => {
      const candidates = [...document.querySelectorAll('[role="button"], a, span')];
      const target = candidates.find((el) => (el.textContent || '').trim() === '开启新对话');
      if (!target) return false;
      (target.closest('[role="button"], a') || target).click();
      return true;
    })()`);
    if (!clicked) {
      await page.send('Page.navigate', { url: 'https://chat.deepseek.com/' });
    }
    await sleep(1200);
    return { ok: true };
  }

  async setToggles(enabled = {}) {
    const page = await this.withPage();
    return page.evaluate(String.raw`(() => {
      const wanted = ${JSON.stringify({ deepThink: !!enabled.deepThink, search: !!enabled.search })};
      const result = {};
      const toggles = [...document.querySelectorAll('.ds-toggle-button')];
      const apply = (label, want) => {
        const el = toggles.find((t) => (t.innerText || '').trim().includes(label));
        if (!el) { result[label] = 'unsupported'; return; }
        const on = (el.className || '').toString().includes('ds-toggle-button--selected');
        if (on !== want) el.click();
        result[label] = want;
      };
      apply('深度思考', wanted.deepThink);
      apply('搜索', wanted.search);
      return result;
    })()`).catch(() => ({}));
  }

  async fillAndSend(text) {
    const page = await this.withPage();
    const filled = await page.evaluate(String.raw`(() => {
      const el = document.querySelector('textarea');
      if (!el) return { ok: false, reason: 'composer not found' };
      el.focus();
      const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
      setter.call(el, ${JSON.stringify(text)});
      el.dispatchEvent(new Event('input', { bubbles: true }));
      return { ok: el.value.length > 0, value: el.value };
    })()`);
    if (!filled?.ok) throw new Error(`无法输入消息：${filled?.reason || '内容为空'}`);
    await sleep(120);
    const clicked = await page.evaluate(String.raw`(() => {
      const buttons = [...document.querySelectorAll('.ds-button--primary')];
      const target = buttons[buttons.length - 1];
      if (!target) return { ok: false };
      if ((target.className || '').toString().includes('ds-button--disabled')) return { ok: false, reason: 'send disabled' };
      target.click();
      return { ok: true };
    })()`);
    if (!clicked?.ok) {
      await page.send('Input.dispatchKeyEvent', {
        type: 'keyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, nativeVirtualKeyCode: 13, text: '\r', unmodifiedText: '\r',
      });
      await page.send('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, nativeVirtualKeyCode: 13 });
    }
    return { ok: true };
  }

  async sendStreaming(text, { onDelta, signal, toggles, webSessionId } = {}) {
    const page = await this.ensureSession(webSessionId);
    // A previous reply that never produced text leaves the composer waiting; a
    // reload clears that wedged state before we send a new message.
    const wedged = await page.evaluate(String.raw`(() => {
      const nodes = document.querySelectorAll('.ds-message');
      const last = nodes[nodes.length - 1];
      return !!last && !last.querySelector('.ds-markdown');
    })()`).catch(() => false);
    if (wedged) {
      await page.send('Page.reload', {}).catch(() => {});
      for (let i = 0; i < 30; i += 1) {
        await sleep(400);
        const info = await page.evaluate(PAGE_HELPERS).catch(() => null);
        if (info?.hasComposer) break;
      }
    }
    const before = await page.evaluate(READ_STATE);
    const baselineText = before.text || '';
    await this.setToggles(toggles || {});
    await this.fillAndSend(text);
    const startedAt = Date.now();
    let last = '';
    let stable = 0;
    let sawStop = false;
    while (!signal?.aborted) {
      await sleep(350);
      let state;
      try {
        state = await page.evaluate(READ_STATE);
      } catch (error) {
        throw new Error(`网页连接中断：${error.message}`);
      }
      if (state.stop) sawStop = true;
      const nextText = state.text || '';
      const appeared = state.count > before.count || (nextText.length > 0 && nextText !== baselineText);
      if (appeared) {
        if (nextText !== last) {
          last = nextText;
          stable = 0;
          onDelta?.({ text: last, done: false, webSessionId: sessionIdFromUrl(state.href) });
        } else {
          stable += 1;
        }
        if (last.length > 0 && stable >= 6) break;
        if (sawStop && stable >= 40) break;
      }
      if (Date.now() - startedAt > 180000) throw new Error('等待网页回复超时：网页端没有返回新的回复，请检查浏览器窗口中的网页状态后重试。');
    }
    onDelta?.({ text: last, done: true, webSessionId: sessionIdFromUrl((await page.evaluate(READ_STATE).catch(() => ({ href: '' }))).href) });
    return { text: last, webSessionId: await this.currentSessionId() };
  }

  async currentSessionId() {
    const page = await this.withPage();
    const href = await page.evaluate('location.href');
    return sessionIdFromUrl(href);
  }

  async stop() {
    const page = await this.withPage();
    const clicked = await page.evaluate(String.raw`(() => {
      const buttons = [...document.querySelectorAll('.ds-button')];
      const stop = buttons.find((b) => /停止|stop|中止/i.test(((b.getAttribute('aria-label') || '') + ' ' + (b.innerText || '') + ' ' + (b.className || ''))));
      if (stop) { stop.click(); return true; }
      const primary = [...document.querySelectorAll('.ds-button--primary')];
      const last = primary[primary.length - 1];
      if (last && !(last.className || '').toString().includes('ds-button--disabled')) { last.click(); return true; }
      return false;
    })()`);
    return { ok: !!clicked };
  }

  async regenerate() {
    const page = await this.withPage();
    const clicked = await page.evaluate(String.raw`(() => {
      const byLabel = [...document.querySelectorAll('[aria-label], [title]')].find((el) => /重新生成|Regenerate/i.test((el.getAttribute('aria-label') || '') + ' ' + (el.getAttribute('title') || '')));
      if (byLabel) { byLabel.click(); return true; }
      return false;
    })()`);
    return { ok: !!clicked };
  }

  dispose() {
    // Keep the browser (and the logged-in web tab) alive across plugin reloads:
    // only drop the CDP socket, never close the tab.
    try { this.page?.ws?.close?.(); } catch { /* ignore */ }
    this.page = null;
  }
}

function sessionIdFromUrl(href) {
  if (!href) return null;
  const match = /\/a\/chat\/s\/([A-Za-z0-9-]+)/.exec(href);
  return match ? match[1] : null;
}

/* ------------------------------------------------------------------ */
/* deterministic task extractor                                        */
/* ------------------------------------------------------------------ */

const FILE_RE = /(?:[A-Za-z]:[\\/])?(?:[\w.@-]+[\\/])*[\w.@-]+\.(?:tsx?|jsx?|mjs|cjs|py|go|rs|java|kt|json|ya?ml|toml|md|css|scss|html|vue|svelte|sql|sh|ps1)\b/g;

function sentences(text) {
  return String(text ?? '')
    .replace(/\r/g, '')
    .split(/\n+/)
    .flatMap((line) => line.split(/(?<=[。！？!?；;])/))
    .map((line) => line.replace(/^[\s>*#\-\d.、)）]+/, '').trim())
    .filter((line) => line.length >= 4 && line.length <= 400);
}

function uniq(list) {
  const seen = new Set();
  const out = [];
  for (const item of list) {
    const key = item.replace(/\s+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function extractTask(chat) {
  const userMessages = chat.messages.filter((m) => m.role === 'user' && (m.text || '').trim());
  const assistantMessages = chat.messages.filter((m) => m.role === 'assistant' && (m.text || '').trim());
  const userLines = userMessages.flatMap((m) => sentences(m.text));
  const lastAssistant = assistantMessages[assistantMessages.length - 1]?.text || '';

  const requirementRe = /需要|必须|实现|支持|添加|增加|修改|删除|修复|优化|重构|不要|不能|禁止|请|希望|要求|设计|开发|接入|替换/;
  const constraintRe = /必须|不得|不能|不要|禁止|仅|只能|限制|兼容|保持|避免|无需|默认/;
  const acceptanceRe = /验收|测试|期望|确保|通过|标准|成功|结果/;
  const openRe = /[?？]$|待确认|不确定|是否|讨论|问题|风险/;

  const requirements = uniq(userLines.filter((line) => requirementRe.test(line))).slice(0, 18);
  const constraints = uniq(userLines.filter((line) => constraintRe.test(line))).slice(0, 12);
  const acceptance = uniq(userLines.filter((line) => acceptanceRe.test(line))).slice(0, 10);
  const files = uniq((userMessages.map((m) => m.text).join('\n').match(FILE_RE) || []).map((f) => f.replace(/\\/g, '/'))).slice(0, 24);
  const openQuestions = uniq([
    ...userLines.filter((line) => openRe.test(line)),
    ...sentences(lastAssistant).filter((line) => openRe.test(line)),
  ]).slice(0, 8);

  const target = requirements[0]
    || userLines[userLines.length - 1]
    || chat.title
    || '根据网页端对话整理开发任务';

  const title = (chat.title || target).replace(/\s+/g, ' ').slice(0, 60);
  const lines = [];
  lines.push(`# ${title}`);
  lines.push('');
  lines.push(`> 来源：DeepSeek 网页聊天「${chat.title || '未命名'}」 · 由本地确定性规则整理，未经模型二次生成。`);
  lines.push('');
  lines.push('## 任务目标');
  lines.push(target);
  lines.push('');
  if (requirements.length) {
    lines.push('## 用户需求（原文要点）');
    for (const item of requirements) lines.push(`- ${item}`);
    lines.push('');
  }
  if (files.length) {
    lines.push('## 涉及的文件或模块');
    for (const file of files) lines.push(`- \`${file}\``);
    lines.push('');
  }
  if (constraints.length) {
    lines.push('## 约束条件');
    for (const item of constraints) lines.push(`- ${item}`);
    lines.push('');
  }
  if (acceptance.length) {
    lines.push('## 验收标准');
    for (const item of acceptance) lines.push(`- ${item}`);
    lines.push('');
  }
  if (openQuestions.length) {
    lines.push('## 尚未解决的问题');
    for (const item of openQuestions) lines.push(`- ${item}`);
    lines.push('');
  }
  if (lastAssistant) {
    lines.push('## 网页端讨论摘要（仅供参考，不等同于用户需求）');
    const summary = lastAssistant.replace(/\s+/g, ' ').slice(0, 700);
    lines.push(summary + (lastAssistant.length > 700 ? '…' : ''));
    lines.push('');
  }
  lines.push('## 重要上下文');
  lines.push(`- 网页会话共 ${chat.messages.length} 条消息，用户消息 ${userMessages.length} 条。`);
  if (files.length) lines.push('- 文件路径来自对话原文，执行前请先核对仓库实际结构。');
  lines.push('- 请先复述你对目标与约束的理解，再开始修改。');

  return { markdown: maskSecrets(lines.join('\n')), title, stats: { requirements: requirements.length, files: files.length, constraints: constraints.length } };
}

function rawConversation(chat, limit = 24000) {
  const lines = [];
  for (const message of chat.messages) {
    const role = message.role === 'user' ? '用户' : 'DeepSeek 网页模型';
    lines.push(`### ${role}`);
    lines.push(message.text || '');
    lines.push('');
  }
  const text = lines.join('\n');
  return text.length > limit ? `${text.slice(0, limit)}\n\n…（原始对话已截断）` : text;
}

function composeHarnessMessage({ sourceTitle, text, context, question }) {
  const masked = maskSecrets(text);
  const blocks = [];
  blocks.push(`**来源：** ${sourceTitle || 'Harness 会话'}`);
  blocks.push('');
  blocks.push('**选中内容：**');
  blocks.push('');
  blocks.push(masked.trim());
  blocks.push('');
  if (context && context.trim()) {
    blocks.push('**背景上下文：**');
    blocks.push('');
    blocks.push(maskSecrets(context).trim());
    blocks.push('');
  }
  blocks.push('**我的问题：**');
  blocks.push('');
  blocks.push(String(question || '').trim() || DEFAULT_QUESTION);
  const markdown = blocks.join('\n');
  return { markdown, characters: markdown.length, defaultQuestion: DEFAULT_QUESTION };
}

/* ------------------------------------------------------------------ */
/* Harness bridge                                                      */
/* ------------------------------------------------------------------ */

class HarnessBridge {
  constructor(ctx, log) {
    this.ctx = ctx;
    this.log = log;
    this.titleCache = new Map();
  }

  controller() {
    const controller = this.ctx.get('sessionController');
    if (!controller) throw new Error('sessionController 服务不可用');
    return controller;
  }

  registry() {
    return this.ctx.get('workspaceRegistry') || null;
  }

  workspaces() {
    const registry = this.registry();
    if (!registry) return [];
    return registry.list().map((workspace) => ({
      id: workspace.id,
      title: workspace.title || path.basename(workspace.path || ''),
      path: workspace.path,
      sessionIds: [...(workspace.sessionIds || [])],
      updatedAt: workspace.updatedAt || 0,
    }));
  }

  async titleOf(sessionId) {
    const cached = this.titleCache.get(sessionId);
    if (cached && Date.now() - cached.at < 15000) return cached.title;
    let title = null;
    try {
      const projections = await this.controller().projections({ sessionId }, new AbortController().signal);
      if (projections?.kind === 'sequenced' && typeof projections.values?.title === 'string') title = projections.values.title;
      else if (typeof projections?.values?.title === 'string') title = projections.values.title;
    } catch { /* best effort */ }
    this.titleCache.set(sessionId, { title, at: Date.now() });
    return title;
  }

  async sessions() {
    const controller = this.controller();
    const page = await controller.list({}, new AbortController().signal);
    const items = Array.isArray(page?.items) ? page.items : [];
    const workspaces = this.workspaces();
    const workspaceFor = (cwd) => workspaces.find((w) => w.path && cwd && (cwd === w.path || cwd.startsWith(w.path))) || null;
    const result = [];
    for (const item of items.slice(0, 80)) {
      const workspace = workspaceFor(item.cwd);
      const title = (await this.titleOf(item.sessionId)) || (item.blank ? '空白会话' : String(item.sessionId).slice(0, 12));
      result.push({
        sessionId: item.sessionId,
        title,
        running: !!item.running,
        blank: !!item.blank,
        updatedAt: item.updatedAt || 0,
        cwd: item.cwd || null,
        workspaceId: workspace?.id || null,
        workspaceTitle: workspace?.title || null,
      });
    }
    return result;
  }

  async send({ sessionId, workspaceId, text, requestId }) {
    const controller = this.controller();
    let targetSessionId = sessionId || null;
    if (!targetSessionId) {
      const created = await controller.create({ workspaceId: workspaceId || undefined });
      targetSessionId = created?.sessionId;
      if (!targetSessionId) throw new Error('新建 Harness 会话失败');
    }
    const id = requestId || randomUUID();
    await controller.prompt({
      requestId: id,
      sessionId: targetSessionId,
      mode: 'queue',
      content: [{ type: 'text', text }],
    }, AbortSignal.timeout(30000));
    this.titleCache.delete(targetSessionId);
    return { sessionId: targetSessionId, requestId: id, accepted: true };
  }

  async state(sessionId) {
    try {
      return await this.controller().state({ sessionId }, new AbortController().signal);
    } catch {
      return null;
    }
  }

  async result(sessionId) {
    const controller = this.controller();
    const state = await this.state(sessionId).catch(() => null);
    const page = await controller.events({
      address: { kind: 'session', sessionId },
      maxMessages: 160,
    }, new AbortController().signal);
    const records = Array.isArray(page?.records) ? page.records : [];
    const events = records.map((record) => record.event || record).filter((event) => event && event.type);

    let turnStart = 0;
    for (let i = 0; i < events.length; i += 1) if (events[i].type === 'turn/start') turnStart = i;
    const turn = events.slice(turnStart);

    const assistantTexts = [];
    const tools = [];
    const errors = [];
    const openQuestions = [];
    let changeSeq = null;
    for (const event of turn) {
      if (event.type === 'assistant/message') {
        const content = event.data?.message?.content;
        if (Array.isArray(content)) {
          const text = content.filter((part) => part?.type === 'text').map((part) => part.text).join('\n').trim();
          if (text) assistantTexts.push(text);
        }
      } else if (event.type === 'tool/call') {
        const name = event.data?.name || event.data?.toolName || event.data?.tool || 'tool';
        tools.push(String(name));
      } else if (event.type === 'tool/result') {
        const message = event.data?.message;
        const content = message?.content;
        let text = '';
        if (Array.isArray(content)) text = content.filter((part) => part?.type === 'text').map((part) => part.text).join('\n');
        if (event.data?.error !== undefined || message?.isError === true) errors.push((text || '工具执行失败').slice(0, 500));
        else if (text && /测试|test|failed|失败|error/i.test(text)) {
          const line = text.split('\n').find((l) => /测试|test|failed|失败|error/i.test(l));
          if (line) errors.push(line.slice(0, 300));
        }
      } else if (event.type === 'workspace/changes') {
        changeSeq = event.seq;
      }
    }

    let files = [];
    if (changeSeq != null) {
      try {
        const changes = this.ctx.get('workspaceChanges');
        const summary = changes?.summary?.(sessionId, changeSeq);
        if (summary?.files) files = summary.files.map((file) => ({ path: file.path, display: file.display, added: file.added, deleted: file.deleted }));
      } catch { /* best effort */ }
    }

    const assistantText = assistantTexts.join('\n\n').trim();
    for (const line of sentences(assistantText)) {
      if (/[?？]$|需要确认|待确认|建议先/.test(line) && line.length > 8) openQuestions.push(line);
    }

    return {
      sessionId,
      title: (await this.titleOf(sessionId)) || String(sessionId).slice(0, 12),
      running: !!(state?.running),
      active: !!state?.active,
      queueLength: Array.isArray(state?.queue) ? state.queue.length : 0,
      assistantText,
      tools: uniq(tools).slice(0, 20),
      errors: uniq(errors).slice(0, 8),
      files: files.slice(0, 40),
      openQuestions: uniq(openQuestions).slice(0, 6),
      updatedAt: Date.now(),
    };
  }
}

/* ------------------------------------------------------------------ */
/* HTTP router                                                         */
/* ------------------------------------------------------------------ */

class Router {
  constructor({ store, adapter, bridge, log }) {
    this.store = store;
    this.adapter = adapter;
    this.bridge = bridge;
    this.log = log;
    this.streams = new Map();
  }

  async handle(req, res) {
    const url = new URL(req.url || '/', 'http://127.0.0.1');
    const route = url.pathname.startsWith(ROUTE)
      ? (url.pathname.slice(ROUTE.length) || '/')
      : url.pathname;
    try {
      if (req.method === 'GET' && route === '/status') {
        return sendJson(res, 200, { ok: true, ...(await this.adapter.status()) });
      }
      if (req.method !== 'POST') return sendJson(res, 405, { ok: false, error: 'method not allowed' });
      const body = await readBody(req);

      switch (route) {
        case '/bootstrap': {
          const status = await this.adapter.status();
          return sendJson(res, 200, {
            ok: true,
            chats: this.store.list(),
            workspaces: this.bridge.workspaces(),
            web: status,
            defaultQuestion: DEFAULT_QUESTION,
          });
        }
        case '/web/status': return sendJson(res, 200, { ok: true, web: await this.adapter.status() });
        case '/web/connect': return sendJson(res, 200, { ok: true, web: await this.adapter.openLogin() });
        case '/web/show': {
          const web = await this.adapter.setHeadless(false);
          return sendJson(res, 200, { ok: true, web: web.state === 'connected' ? await this.adapter.openLogin() : web });
        }
        case '/web/hide': return sendJson(res, 200, { ok: true, web: await this.adapter.setHeadless(true) });
        case '/web/new': {
          await this.adapter.newChat();
          return sendJson(res, 200, { ok: true, web: await this.adapter.status() });
        }

        case '/chats/list': return sendJson(res, 200, { ok: true, chats: this.store.list() });
        case '/chats/create': {
          const chat = this.store.create({ title: body.title, workspaceId: body.workspaceId || null });
          return sendJson(res, 200, { ok: true, chat });
        }
        case '/chats/get': {
          const chat = this.store.get(body.id);
          if (!chat) throw new Error('会话不存在');
          return sendJson(res, 200, { ok: true, chat });
        }
        case '/chats/rename': {
          const chat = this.store.update(body.id, { title: String(body.title || '').slice(0, 120) || '未命名对话' });
          return sendJson(res, 200, { ok: true, chat });
        }
        case '/chats/delete': {
          this.store.remove(body.id);
          return sendJson(res, 200, { ok: true });
        }
        case '/chats/origin': {
          const chat = this.store.update(body.id, {
            origin: {
              harnessSessionId: body.harnessSessionId || null,
              messageId: body.messageId || null,
              at: Date.now(),
            },
          });
          return sendJson(res, 200, { ok: true, chat });
        }

        case '/chat/send': return await this.streamSend(req, res, body);
        case '/chat/stop': {
          const controller = this.streams.get(body.chatId);
          if (controller) controller.abort();
          const result = await this.adapter.stop().catch((error) => ({ ok: false, error: error.message }));
          return sendJson(res, 200, { ok: true, stopped: true, web: result });
        }
        case '/chat/regenerate': {
          const result = await this.adapter.regenerate();
          return sendJson(res, 200, { ok: true, ...result });
        }

        case '/extract': {
          const chat = this.store.get(body.chatId);
          if (!chat) throw new Error('会话不存在');
          const extracted = extractTask(chat);
          let markdown = extracted.markdown;
          if (body.includeRaw) {
            markdown += `\n\n---\n\n## 附：原始网页对话\n\n${rawConversation(chat)}`;
          }
          return sendJson(res, 200, { ok: true, markdown, title: extracted.title, stats: extracted.stats });
        }

        case '/harness/workspaces': return sendJson(res, 200, { ok: true, workspaces: this.bridge.workspaces() });
        case '/harness/sessions': {
          const sessions = await this.bridge.sessions();
          const filtered = body.workspaceId ? sessions.filter((s) => s.workspaceId === body.workspaceId || !s.workspaceId) : sessions;
          return sendJson(res, 200, { ok: true, sessions: filtered });
        }
        case '/harness/send': {
          const chat = this.store.get(body.chatId);
          if (!chat) throw new Error('网页会话不存在');
          const text = String(body.markdown || '').trim();
          if (!text) throw new Error('任务内容为空');
          const sent = await this.bridge.send({
            sessionId: body.sessionId || null,
            workspaceId: body.workspaceId || null,
            text,
            requestId: body.requestId || undefined,
          });
          const task = {
            taskId: randomUUID(),
            requestId: sent.requestId,
            harnessSessionId: sent.sessionId,
            sessionTitle: (await this.bridge.titleOf(sent.sessionId)) || String(sent.sessionId).slice(0, 12),
            status: 'sent',
            sentAt: Date.now(),
            lastSyncAt: Date.now(),
            messagePreview: text.slice(0, 160),
          };
          this.store.addTask(chat.id, task);
          return sendJson(res, 200, { ok: true, task });
        }
        case '/harness/result': {
          const result = await this.bridge.result(body.sessionId);
          const chat = body.chatId ? this.store.get(body.chatId) : null;
          if (chat) {
            for (const task of chat.tasks) {
              if (task.harnessSessionId === body.sessionId) task.lastSyncAt = Date.now();
            }
            this.store.scheduleSave();
          }
          return sendJson(res, 200, { ok: true, result });
        }
        case '/compose': {
          const composed = composeHarnessMessage({
            sourceTitle: body.sourceTitle || 'Harness 会话',
            text: body.text || '',
            context: body.context || '',
            question: body.question || '',
          });
          return sendJson(res, 200, { ok: true, ...composed });
        }
        default:
          return sendJson(res, 404, { ok: false, error: `unknown route ${route}` });
      }
    } catch (error) {
      this.log(`dual-mode route ${route} failed: ${error?.stack || error}`);
      return sendError(res, error);
    }
  }

  async streamSend(req, res, body) {
    const chat = this.store.get(body.chatId);
    if (!chat) throw new Error('网页会话不存在');
    const text = String(body.text || '').trim();
    if (!text) throw new Error('消息内容为空');

    const userMessage = { id: `m-${randomUUID()}`, role: 'user', text, time: Date.now(), status: 'sent' };
    this.store.appendMessage(chat.id, userMessage);
    const assistantMessage = { id: `m-${randomUUID()}`, role: 'assistant', text: '', time: Date.now(), status: 'streaming', error: null };
    this.store.appendMessage(chat.id, assistantMessage);
    if (chat.messages.filter((m) => m.role === 'user').length === 1) chat.title = text.replace(/\s+/g, ' ').slice(0, 30);

    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    const emit = (payload) => {
      if (res.writableEnded) return;
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };
    emit({ type: 'accepted', chatId: chat.id, userMessageId: userMessage.id, assistantMessageId: assistantMessage.id, title: chat.title });

    const controller = new AbortController();
    this.streams.set(chat.id, controller);
    req.on('close', () => controller.abort());
    let lastSave = 0;
    try {
      const status = await this.adapter.status();
      if (status.state !== 'connected') {
        throw new Error(status.state === 'login-required'
          ? '网页端尚未登录：请点击「显示浏览器」在弹出的 Chrome 窗口完成登录后重试。'
          : `网页端未就绪（${status.state}${status.error ? `：${status.error}` : ''}）`);
      }
      const { text: finalText, webSessionId } = await this.adapter.sendStreaming(text, {
        webSessionId: chat.webSessionId,
        toggles: body.toggles || {},
        signal: controller.signal,
        onDelta: ({ text: partial, done, webSessionId: sessionId }) => {
          this.store.updateMessage(chat.id, assistantMessage.id, { text: partial });
          if (sessionId && sessionId !== chat.webSessionId) this.store.update(chat.id, { webSessionId: sessionId });
          emit({ type: 'delta', text: partial, done: !!done });
          if (Date.now() - lastSave > 1200) {
            lastSave = Date.now();
            this.store.saveNow();
          }
        },
      });
      this.store.updateMessage(chat.id, assistantMessage.id, { text: finalText, status: 'done' });
      this.store.update(chat.id, { webSessionId: webSessionId || chat.webSessionId, connection: { state: 'connected', checkedAt: Date.now(), error: null } });
      this.store.saveNow();
      emit({ type: 'done', text: finalText, webSessionId });
    } catch (error) {
      const aborted = controller.signal.aborted;
      const message = aborted ? '已停止生成' : (error instanceof Error ? error.message : String(error));
      const current = this.store.get(chat.id)?.messages.find((m) => m.id === assistantMessage.id);
      this.store.updateMessage(chat.id, assistantMessage.id, {
        status: aborted ? 'stopped' : 'error',
        error: aborted ? null : message,
        text: current?.text || (aborted ? (current?.text || '') : ''),
      });
      this.store.update(chat.id, { connection: { state: aborted ? 'connected' : 'error', checkedAt: Date.now(), error: aborted ? null : message } });
      this.store.saveNow();
      emit({ type: 'error', message, aborted });
    } finally {
      this.streams.delete(chat.id);
      if (!res.writableEnded) res.end();
    }
  }

  dispose() {
    for (const controller of this.streams.values()) controller.abort();
    this.streams.clear();
  }
}

/* ------------------------------------------------------------------ */
/* plugin entry                                                        */
/* ------------------------------------------------------------------ */

export const name = PLUGIN_ID;
export const inject = ['webServer'];

export function apply(ctx, config = {}) {
  const log = (message) => {
    try { ctx.logger?.info?.(message); } catch { /* ignore */ }
  };
  const dir = config.dataDir || dataDirectory();
  fs.mkdirSync(dir, { recursive: true });
  const store = new ChatStore(dir, log);
  const adapter = new WebChatAdapter({
    dir,
    port: Number(config.debuggingPort) || DEFAULT_PORT,
    browserPath: config.browserPath || null,
    log,
  });
  const bridge = new HarnessBridge(ctx, log);
  const router = new Router({ store, adapter, bridge, log });

  ctx.effect(() => {
    const disposeRoute = ctx.webServer.register({
      kind: 'prefix',
      path: ROUTE,
      handler: (req, res) => { void router.handle(req, res); },
    });
    return () => {
      router.dispose();
      store.dispose();
      adapter.dispose();
      try { disposeRoute?.(); } catch { /* ignore */ }
    };
  });

  log(`dual-mode: mounted, data dir ${dir}`);
}
