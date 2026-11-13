# DeepSeek Dual Mode

A DeepSeek Harness plugin that adds a ChatGPT-desktop-style product switcher to the
top-left brand area:

- **DeepSeek Harness** — the normal Harness agent UI (untouched).
- **DeepSeek Chat** — a separate workspace that drives your own logged-in
  [chat.deepseek.com](https://chat.deepseek.com) session in a dedicated, persistent Chrome
  profile and talks to the real web model. It never calls the DeepSeek API and never
  incurs API billing.

## Features

1. **Mode switcher** in the sidebar brand slot (`DeepSeek ▾`). The Harness UI, running
   tasks, scroll position and drafts are preserved because switching only changes the
   active main panel.
2. **Web Chats workspace** — a separate `main` panel with its own chat list, search,
   rename, delete, streaming replies, Markdown/code rendering, copy, stop and regenerate.
3. **提取指令 → Harness** — a deterministic extractor turns the current web conversation
   into an editable, structured Markdown task (goal, requirements, technical plan,
   files/modules, constraints, acceptance criteria, context, open questions). You pick an
   existing Harness session (searchable) or create a new one, then confirm.
4. **获取 Harness 执行结果** — pulls the latest execution summary (assistant output, tool
   calls, errors, changed files, open questions) and, after confirmation, sends it back to
   the web chat as your next message.
5. **在 Chat 模式询问** — right-click any Harness message (or use the hover chat button on
   assistant messages) to send it to a web chat. Selected text wins over the whole message;
   the content is composed as 来源 / 选中内容 / 背景上下文 / 我的问题 with secret masking and a
   character count before anything is sent.

## Requirements

- DeepSeek Harness with Web UI support (this plugin was built against the desktop profile).
- Google Chrome or Microsoft Edge installed (the adapter launches it with a dedicated
  `--user-data-dir`).
- A one-time manual login to chat.deepseek.com in the browser window the adapter opens.

## How the connection works

The plugin starts Chrome with remote debugging enabled and drives the **real web UI** over
the Chrome DevTools Protocol: it types into the page's composer, clicks the send button,
and reads the streamed Markdown reply from the DOM. Nothing is mocked and no DeepSeek API
key is used.

By default Chrome runs **headless** (`--headless=new`), so no browser window appears and
nothing has to stay open on screen. The browser is launched detached and is kept alive
across plugin reloads and panel switches; a reply that is streaming when the plugin is
reloaded keeps running in the same tab.

- Click the `静默模式` chip in the Chat panel header to switch to a visible window
  (`窗口模式`) — use this for the one-time login or to watch the real page.
- Click it again to go back to silent background mode. The choice is remembered in
  `<DSH_PROFILE_DIR>/dual-mode/settings.json`.
- If the browser process is gone, the next send relaunches it silently and navigates back
  to the stored `chat.deepseek.com/a/chat/s/<id>` thread, so the conversation continues in
  the same web chat.

Because the web UI is virtualized and DOM-driven, response completion is detected by
watching the reply text stabilise (about 2 s of no change), with a hard 3-minute timeout.
If a previous reply left the page with an empty trailing answer, the adapter reloads the
tab once before sending instead of hanging.

The adapter reuses one persistent Chrome profile. If that profile already has a
chat.deepseek.com login (for example a profile you logged into manually), copy it to
`<DSH_PROFILE_DIR>/dual-mode/chrome-profile` before enabling the plugin, or simply log in
once in the window the adapter opens.

## Files and data

- Web chats: `<DSH_PROFILE_DIR>/dual-mode/webchats.json` (atomic writes).
- Browser profile: `<DSH_PROFILE_DIR>/dual-mode/chrome-profile`.
- Harness sends go through the official `sessionController.prompt` API with `mode: 'queue'`,
  so a running Harness task is never interrupted.

## Route security

The plugin serves its own endpoints under `/dual-mode/` (chat store, browser control,
send/result bridge). Every request is admitted through the Harness connection service
(`ctx.connection.requestRejection`), so only same-origin requests from the authenticated
Harness web client are accepted:

- Host must be loopback (`127.0.0.1`, `localhost`, `[::1]`);
- `sec-fetch-site: cross-site` is rejected;
- a present `Origin` must match `Host`;
- no authenticated browser peer → `401`.

Cross-site or unauthenticated callers get `403` / `401` JSON instead of touching the chat
store or the automation browser. If the connection service is unavailable, the plugin falls
back to the same loopback + Origin fence.

## Limitations

- DSH has no message-level context-menu slot. The plugin listens for `contextmenu` inside
  `[data-conversation-content]` and shows its own small menu there (复制 + 在 Chat 模式询问);
  native menus everywhere else are untouched.
- Web-side model switching is only offered for the toggles actually present on the page
  (深度思考 / 智能搜索). The plugin does not invent model options.
- Disabling/uninstalling the plugin removes the switcher and the Chat panel; the Harness UI
  is untouched because native components are never patched.
- The Chat workspace is a `main` panel opened from the brand menu. It is not a real session
  folder: DSH has no folder in the session data model, so Web Chats are kept in the plugin's
  own JSON store and never mixed with Harness sessions.
- Automation relies on the current chat.deepseek.com DOM (class names such as
  `.ds-message`, `.ds-markdown`, `.ds-toggle-button`). If the site changes its markup the
  adapter reports a send/timeout error instead of faking a reply; the selectors live in one
  place (`READ_STATE`, `fillAndSend`, `setToggles` in `lib/index.js`).
