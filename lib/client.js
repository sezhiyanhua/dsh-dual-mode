window.__ModuleLoader__.load({
  id: 'dsh-dual-mode',
  factory: (require) => {
    const React = require('react');
    const h = React.createElement;
    const { useState, useEffect, useRef, useMemo, useSyncExternalStore } = React;

    const PANEL_ID = 'dual-mode-chat';
    const DEFAULT_QUESTION = '请帮我分析以上内容，指出潜在问题，并给出你的建议。暂时不要修改代码。';

    /* ---------------------------------------------------------------- */
    /* module store                                                      */
    /* ---------------------------------------------------------------- */

    let state = {
      booted: false,
      loading: true,
      error: null,
      chats: [],
      currentId: null,
      workspaces: [],
      sessions: [],
      web: { state: 'unknown' },
      busy: {},
      drafts: {},
      scroll: {},
      modal: null,
      toast: null,
      query: '',
      defaultQuestion: DEFAULT_QUESTION,
      language: 'zh',
    };
    const listeners = new Set();
    function getState() { return state; }
    function setState(patch) {
      state = Object.assign({}, state, patch);
      for (const listener of [...listeners]) listener();
    }
    function subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    }
    function useStore() { return useSyncExternalStore(subscribe, getState, getState); }

    let toastTimer = null;
    function toast(message) {
      setState({ toast: message });
      if (toastTimer) clearTimeout(toastTimer);
      toastTimer = setTimeout(() => setState({ toast: null }), 3200);
    }

    /* ---------------------------------------------------------------- */
    /* http helpers                                                      */
    /* ---------------------------------------------------------------- */

    async function api(pathname, body) {
      const init = {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body === undefined ? {} : body),
      };
      const response = await fetch(`/dual-mode${pathname}`, init);
      let data;
      try { data = await response.json(); } catch { data = { ok: false, error: `HTTP ${response.status}` }; }
      if (!response.ok || data.ok === false) throw new Error(data.error || `HTTP ${response.status}`);
      return data;
    }

    const activeStreams = new Map();
    const abortStreams = new Map();

    function patchChat(chatId, mutate) {
      setState({ chats: state.chats.map((chat) => (chat.id === chatId ? mutate({ ...chat, messages: [...chat.messages] }) : chat)) });
    }

    async function reloadChat(chatId) {
      try {
        const data = await api('/chats/get', { id: chatId });
        setState({ chats: state.chats.map((chat) => (chat.id === chatId ? data.chat : chat)) });
      } catch { /* ignore */ }
    }

    async function refreshChats() {
      try {
        const data = await api('/chats/list');
        setState({ chats: data.chats });
      } catch { /* ignore */ }
    }

    async function loadSessions(workspaceId) {
      try {
        const data = await api('/harness/sessions', { workspaceId: workspaceId || null });
        setState({ sessions: data.sessions });
      } catch { /* ignore */ }
    }

    function applyStreamEvent(chatId, event) {
      if (event.type === 'accepted') {
        patchChat(chatId, (chat) => {
          const messages = chat.messages.map((message) => {
            if (message.id === 'tmp-user') return { ...message, id: event.userMessageId };
            if (message.id === 'tmp-assistant') return { ...message, id: event.assistantMessageId };
            return message;
          });
          return { ...chat, title: event.title || chat.title, messages };
        });
        return;
      }
      if (event.type === 'delta') {
        patchChat(chatId, (chat) => {
          const messages = chat.messages.map((message) => (
            message.id === 'tmp-assistant' ? { ...message, text: event.text, status: 'streaming' } : message
          ));
          return { ...chat, messages };
        });
        return;
      }
      if (event.type === 'done') {
        patchChat(chatId, (chat) => {
          const messages = chat.messages.map((message) => (
            message.id === 'tmp-assistant' ? { ...message, text: event.text || message.text, status: 'done' } : message
          ));
          return { ...chat, messages, webSessionId: event.webSessionId || chat.webSessionId };
        });
        return;
      }
      if (event.type === 'error') {
        patchChat(chatId, (chat) => {
          const messages = chat.messages.map((message) => (
            message.id === 'tmp-assistant'
              ? { ...message, status: event.aborted ? 'stopped' : 'error', error: event.aborted ? null : event.message }
              : message
          ));
          return { ...chat, messages };
        });
      }
    }

    async function sendChat(chatId, text, toggles) {
      if (!text.trim() || state.busy[chatId]) return;
      setState({ busy: { ...state.busy, [chatId]: true } });
      patchChat(chatId, (chat) => ({
        ...chat,
        messages: [
          ...chat.messages,
          { id: 'tmp-user', role: 'user', text, time: Date.now(), status: 'sent' },
          { id: 'tmp-assistant', role: 'assistant', text: '', time: Date.now(), status: 'streaming', error: null },
        ],
      }));
      const controller = new AbortController();
      activeStreams.set(chatId, controller);
      const finish = () => {
        setState({ busy: { ...state.busy, [chatId]: false } });
        activeStreams.delete(chatId);
        void reloadChat(chatId);
      };
      try {
        const response = await fetch('/dual-mode/chat/send', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ chatId, text, toggles: toggles || {} }),
          signal: controller.signal,
        });
        if (!response.ok || !response.body) throw new Error(`网页聊天通道不可用（HTTP ${response.status}）`);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let index;
          while ((index = buffer.indexOf('\n\n')) >= 0) {
            const raw = buffer.slice(0, index);
            buffer = buffer.slice(index + 2);
            const line = raw.split('\n').find((item) => item.startsWith('data:'));
            if (!line) continue;
            try { applyStreamEvent(chatId, JSON.parse(line.slice(5).trim())); } catch { /* ignore malformed */ }
          }
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          patchChat(chatId, (chat) => ({
            ...chat,
            messages: chat.messages.map((message) => (
              message.id === 'tmp-assistant'
                ? { ...message, status: 'error', error: error instanceof Error ? error.message : String(error) }
                : message
            )),
          }));
        }
      } finally {
        finish();
      }
    }

    async function stopChat(chatId) {
      try { await api('/chat/stop', { chatId }); } catch { /* ignore */ }
      activeStreams.get(chatId)?.abort();
      patchChat(chatId, (chat) => ({
        ...chat,
        messages: chat.messages.map((message) => (
          message.status === 'streaming' ? { ...message, status: 'stopped' } : message
        )),
      }));
      setState({ busy: { ...state.busy, [chatId]: false } });
    }

    /* ---------------------------------------------------------------- */
    /* markdown rendering                                                */
    /* ---------------------------------------------------------------- */

    function inlineNodes(text, prefix) {
      const nodes = [];
      const pattern = /(`[^`\n]+`|\*\*[^*\n]+\*\*|\[[^\]\n]+\]\([^)\s]+\))/g;
      let last = 0;
      let match;
      let index = 0;
      while ((match = pattern.exec(text)) !== null) {
        if (match.index > last) nodes.push(text.slice(last, match.index));
        const token = match[0];
        const key = `${prefix}-i${index++}`;
        if (token.startsWith('`')) {
          nodes.push(h('code', { key, className: 'dsdm-inline-code' }, token.slice(1, -1)));
        } else if (token.startsWith('**')) {
          nodes.push(h('strong', { key }, token.slice(2, -2)));
        } else {
          const linkMatch = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(token);
          if (linkMatch) nodes.push(h('a', { key, href: linkMatch[2], target: '_blank', rel: 'noreferrer' }, linkMatch[1]));
        }
        last = match.index + token.length;
      }
      if (last < text.length) nodes.push(text.slice(last));
      return nodes;
    }

    function Markdown({ text }) {
      const blocks = useMemo(() => {
        const lines = String(text || '').replace(/\r/g, '').split('\n');
        const out = [];
        let paragraph = [];
        let list = null;
        let quote = [];
        const flushParagraph = () => {
          if (paragraph.length) { out.push({ kind: 'p', text: paragraph.join('\n') }); paragraph = []; }
        };
        const flushList = () => {
          if (list) { out.push(list); list = null; }
        };
        const flushQuote = () => {
          if (quote.length) { out.push({ kind: 'quote', text: quote.join('\n') }); quote = []; }
        };
        for (let i = 0; i < lines.length; i += 1) {
          const line = lines[i];
          if (/^```/.test(line)) {
            flushParagraph(); flushList(); flushQuote();
            const language = line.slice(3).trim();
            const body = [];
            i += 1;
            while (i < lines.length && !/^```/.test(lines[i])) { body.push(lines[i]); i += 1; }
            out.push({ kind: 'code', language, text: body.join('\n') });
            continue;
          }
          if (/^\s*$/.test(line)) { flushParagraph(); flushList(); flushQuote(); continue; }
          const heading = /^(#{1,4})\s+(.*)$/.exec(line);
          if (heading) {
            flushParagraph(); flushList(); flushQuote();
            out.push({ kind: 'h', level: heading[1].length, text: heading[2] });
            continue;
          }
          if (/^\s*[-*+]\s+/.test(line)) {
            flushParagraph(); flushQuote();
            if (!list) list = { kind: 'ul', items: [] };
            list.items.push(line.replace(/^\s*[-*+]\s+/, ''));
            continue;
          }
          if (/^\s*\d+[.)]\s+/.test(line)) {
            flushParagraph(); flushQuote();
            if (!list || list.kind !== 'ol') { flushList(); list = { kind: 'ol', items: [] }; }
            list.items.push(line.replace(/^\s*\d+[.)]\s+/, ''));
            continue;
          }
          if (/^\s*>\s?/.test(line)) {
            flushParagraph(); flushList();
            quote.push(line.replace(/^\s*>\s?/, ''));
            continue;
          }
          flushList(); flushQuote();
          paragraph.push(line);
        }
        flushParagraph(); flushList(); flushQuote();
        return out;
      }, [text]);

      return h('div', { className: 'dsdm-md' }, blocks.map((block, index) => {
        if (block.kind === 'code') {
          return h('div', { key: index, className: 'dsdm-code' },
            h('div', { className: 'dsdm-code-head' },
              h('span', null, block.language || 'code'),
              h('button', {
                type: 'button',
                className: 'dsdm-code-copy',
                onClick: () => { navigator.clipboard?.writeText(block.text); toast('代码已复制'); },
              }, '复制')),
            h('pre', null, h('code', null, block.text)));
        }
        if (block.kind === 'h') {
          return h(`h${Math.min(block.level + 2, 6)}`, { key: index, className: 'dsdm-h' }, inlineNodes(block.text, `h${index}`));
        }
        if (block.kind === 'ul' || block.kind === 'ol') {
          const Tag = block.kind === 'ul' ? 'ul' : 'ol';
          return h(Tag, { key: index, className: 'dsdm-list' },
            block.items.map((item, itemIndex) => h('li', { key: itemIndex }, inlineNodes(item, `l${index}-${itemIndex}`))));
        }
        if (block.kind === 'quote') {
          return h('blockquote', { key: index, className: 'dsdm-quote' }, inlineNodes(block.text, `q${index}`));
        }
        return h('p', { key: index, className: 'dsdm-p' },
          block.text.split('\n').flatMap((line, lineIndex) => [
            lineIndex > 0 ? h('br', { key: `br${lineIndex}` }) : null,
            ...inlineNodes(line, `p${index}-${lineIndex}`),
          ]).filter(Boolean));
      }));
    }

    /* ---------------------------------------------------------------- */
    /* tiny ui pieces                                                    */
    /* ---------------------------------------------------------------- */

    const IconChat = ({ size = 16 }) => h('svg', { width: size, height: size, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round', strokeLinejoin: 'round' },
      h('path', { d: 'M21 11.5a8.4 8.4 0 0 1-8.5 8.4 9 9 0 0 1-3.9-.9L3 21l1.9-5.3A8.4 8.4 0 1 1 21 11.5z' }));

    const IconChevron = () => h('svg', { width: 12, height: 12, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2.2, strokeLinecap: 'round', strokeLinejoin: 'round' },
      h('path', { d: 'M6 9l6 6 6-6' }));

    const IconSend = () => h('svg', { width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.9, strokeLinecap: 'round', strokeLinejoin: 'round' },
      h('path', { d: 'M4 12l16-8-6 16-3-6-7-2z' }));

    const IconStop = () => h('svg', { width: 14, height: 14, viewBox: '0 0 24 24', fill: 'currentColor' },
      h('rect', { x: 6, y: 6, width: 12, height: 12, rx: 2 }));

    const IconPlus = () => h('svg', { width: 15, height: 15, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round' },
      h('path', { d: 'M12 5v14M5 12h14' }));

    const IconTrash = () => h('svg', { width: 13, height: 13, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round' },
      h('path', { d: 'M4 7h16M9 7V5h6v2m-8 0l1 13h8l1-13' }));

    const IconPencil = () => h('svg', { width: 13, height: 13, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round' },
      h('path', { d: 'M4 20h4l10-10-4-4L4 16v4zM14 6l4 4' }));

    function timeLabel(value) {
      if (!value) return '';
      const date = new Date(value);
      const now = Date.now();
      if (now - value < 86400000) return date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
      return date.toLocaleDateString('zh-CN', { month: 'numeric', day: 'numeric' });
    }

    function webLabel(web) {
      const value = web?.state || 'unknown';
      if (value === 'connected') return { text: '网页已连接', tone: 'ok' };
      if (value === 'login-required') return { text: '需要登录', tone: 'warn' };
      if (value === 'loading' || value === 'launching') return { text: '网页加载中', tone: 'warn' };
      return { text: '网页未连接', tone: 'err' };
    }

    /* ---------------------------------------------------------------- */
    /* brand / mode switcher                                             */
    /* ---------------------------------------------------------------- */

    let modeMenuEl = null;
    function closeModeMenu() {
      if (modeMenuEl) { try { modeMenuEl.remove(); } catch { /* ignore */ } modeMenuEl = null; }
    }

    function openModeMenu(anchor, mode, onChoose) {
      closeModeMenu();
      const el = document.createElement('div');
      el.className = 'dsdm-menu';
      el.setAttribute('data-dsh-dual-mode', '1');
      el.style.position = 'fixed';
      el.style.left = `${Math.round(anchor.left)}px`;
      el.style.top = `${Math.round(anchor.top)}px`;
      el.style.zIndex = '4000';

      const makeItem = (label, sub, active, glyph, next) => {
        const row = document.createElement('button');
        row.type = 'button';
        row.className = `dsdm-menu-item${active ? ' is-active' : ''}`;
        const icon = document.createElement('span');
        icon.className = 'dsdm-menu-glyph';
        icon.textContent = glyph;
        const text = document.createElement('span');
        text.className = 'dsdm-menu-text';
        text.textContent = label;
        const hint = document.createElement('span');
        hint.className = 'dsdm-menu-sub';
        hint.textContent = sub;
        row.appendChild(icon); row.appendChild(text); row.appendChild(hint);
        row.addEventListener('mousedown', (event) => { event.preventDefault(); event.stopPropagation(); });
        row.addEventListener('click', (event) => { event.preventDefault(); event.stopPropagation(); closeModeMenu(); onChoose(next); });
        return row;
      };

      el.appendChild(makeItem('DeepSeek Chat', '网页账号', mode === 'chat', '◍', 'chat'));
      el.appendChild(makeItem('DeepSeek Harness', '本地 Agent', mode === 'harness', '◈', 'harness'));
      document.body.appendChild(el);
      modeMenuEl = el;

      const onDown = (event) => { if (!el.contains(event.target)) cleanup(); };
      const onKey = (event) => { if (event.key === 'Escape') cleanup(); };
      const cleanup = () => {
        closeModeMenu();
        document.removeEventListener('mousedown', onDown, true);
        document.removeEventListener('keydown', onKey, true);
      };
      setTimeout(() => {
        document.addEventListener('mousedown', onDown, true);
        document.addEventListener('keydown', onKey, true);
      }, 0);
    }

    function BrandModeSwitcher(props) {
      const store = useStore();
      const ref = useRef(null);
      const panelInfo = props.usePanelInfo;
      const activePanelId = panelInfo ? panelInfo((info) => info.activePanelId) : null;
      const mode = activePanelId === PANEL_ID ? 'chat' : 'harness';
      const layout = props.layout || null;

      const choose = (next) => {
        try {
          if (!layout) return;
          if (next === 'chat') layout.selectPanel(PANEL_ID);
          else layout.selectPanel(null);
        } catch (error) {
          toast(`切换失败：${error.message}`);
        }
      };

      return h('div', { className: 'dsdm-brand', ref },
        h('button', {
          type: 'button',
          className: 'dsdm-brand-button',
          title: '切换 DeepSeek 模式',
          onPointerDown: (event) => { event.stopPropagation(); },
          onMouseDown: (event) => { event.preventDefault(); event.stopPropagation(); },
          onClick: (event) => {
            event.preventDefault();
            event.stopPropagation();
            const rect = ref.current?.getBoundingClientRect();
            const anchor = rect ? { left: rect.left, top: rect.bottom + 6 } : { left: 12, top: 80 };
            openModeMenu(anchor, mode, choose);
          },
        },
        h('span', { className: 'dsdm-brand-name' }, 'deepseek'),
        h('span', { className: 'dsdm-brand-mode' }, mode === 'chat' ? 'Chat' : 'Harness'),
        h('span', { className: 'dsdm-brand-caret' }, h(IconChevron)))
      );
    }

    function PanelIcon({ active }) {
      return h('span', { className: `dsdm-panel-icon${active ? ' is-active' : ''}` }, h(IconChat, { size: 18 }));
    }

    /* ---------------------------------------------------------------- */
    /* chat workspace                                                    */
    /* ---------------------------------------------------------------- */

    function ChatWorkspace(props) {
      const store = useStore();
      const { chats, currentId, workspaces, sessions, busy, drafts, query, web, modal } = store;
      const current = chats.find((chat) => chat.id === currentId) || null;
      const scrollRef = useRef(null);
      const inputRef = useRef(null);
      const [sending, setSending] = useState(false);
      const [toggles, setToggles] = useState({ deepThink: false, search: false });

      useEffect(() => {
        if (!store.booted && !store.loading) return;
        const timer = setInterval(() => {
          api('/web/status').then((data) => setState({ web: data.web })).catch(() => {});
        }, 6000);
        return () => clearInterval(timer);
      }, [store.booted, store.loading]);


      useEffect(() => {
        const node = scrollRef.current;
        if (!node) return;
        const saved = state.scroll[currentId];
        node.scrollTop = typeof saved === 'number' ? saved : node.scrollHeight;
      }, [currentId]);

      useEffect(() => {
        const node = scrollRef.current;
        if (!node) return;
        const nearBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 220;
        if (nearBottom && current?.messages.some((message) => message.status === 'streaming')) {
          node.scrollTop = node.scrollHeight;
        }
      }, [current?.messages]);

      const lastCount = useRef(0);
      useEffect(() => {
        const count = current?.messages.length || 0;
        if (count > lastCount.current) {
          const node = scrollRef.current;
          if (node) node.scrollTop = node.scrollHeight;
        }
        lastCount.current = count;
      }, [current?.messages.length, currentId]);

      const draft = current ? (drafts[current.id] || '') : '';
      const setDraft = (value) => {
        if (!current) return;
        setState({ drafts: { ...state.drafts, [current.id]: value } });
      };

      const createChat = async () => {
        try {
          const data = await api('/chats/create', {});
          setState({ chats: [data.chat, ...state.chats], currentId: data.chat.id });
          inputRef.current?.focus();
        } catch (error) { toast(error.message); }
      };

      const deleteChat = async (chat) => {
        if (!window.confirm(`删除网页对话「${chat.title}」？`)) return;
        try {
          await api('/chats/delete', { id: chat.id });
          const rest = state.chats.filter((item) => item.id !== chat.id);
          setState({ chats: rest, currentId: rest[0]?.id || null });
        } catch (error) { toast(error.message); }
      };

      const renameChat = async (chat) => {
        const next = window.prompt('重命名网页对话', chat.title);
        if (!next) return;
        try {
          const data = await api('/chats/rename', { id: chat.id, title: next });
          setState({ chats: state.chats.map((item) => (item.id === chat.id ? data.chat : item)) });
        } catch (error) { toast(error.message); }
      };

      const submit = () => {
        if (!current) return;
        const text = (drafts[current.id] || '').trim();
        if (!text || busy[current.id]) return;
        setDraft('');
        void sendChat(current.id, text, toggles);
      };

      const onKeyDown = (event) => {
        if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent?.isComposing) {
          event.preventDefault();
          submit();
        }
      };

      const openExtract = () => {
        if (!current) return;
        setState({ modal: { type: 'extract', chatId: current.id, loading: true, error: null } });
        api('/extract', { chatId: current.id, includeRaw: false })
          .then((data) => setState({
            modal: {
              type: 'extract',
              chatId: current.id,
              loading: false,
              error: null,
              markdown: data.markdown,
              includeRaw: false,
              workspaceId: current.workspaceId || workspaces[0]?.id || '',
              sessionId: current.origin?.harnessSessionId || '',
              createNew: !current.origin?.harnessSessionId,
              search: '',
              sending: false,
              sent: null,
            },
          }))
          .catch((error) => setState({ modal: { type: 'extract', chatId: current.id, loading: false, error: error.message } }));
      };

      const openResult = () => {
        if (!current) return;
        const task = current.tasks?.[0];
        if (!task) { toast('这个网页对话还没有发送过 Harness 任务'); return; }
        void openResultModal(current, task.harnessSessionId);
      };

      const filteredChats = chats.filter((chat) => !query || chat.title.toLowerCase().includes(query.toLowerCase()));

      return h('div', { className: 'dsdm-root' },
        h('aside', { className: 'dsdm-side' },
          h('div', { className: 'dsdm-side-head' },
            h('div', { className: 'dsdm-side-title' }, 'Web Chats'),
            h('button', { type: 'button', className: 'dsdm-icon-button', title: '新建网页对话', onClick: createChat }, h(IconPlus))),
          h('div', { className: 'dsdm-search' },
            h('input', {
              value: query,
              placeholder: '搜索网页对话',
              onChange: (event) => setState({ query: event.target.value }),
            })),
          h('div', { className: 'dsdm-chat-list' },
            filteredChats.length === 0 && h('div', { className: 'dsdm-empty-small' }, '还没有网页对话'),
            filteredChats.map((chat) => h('div', {
              key: chat.id,
              className: `dsdm-chat-row${chat.id === currentId ? ' is-active' : ''}`,
              onClick: () => setState({ currentId: chat.id }),
            },
            h('div', { className: 'dsdm-chat-row-main' },
              h('div', { className: 'dsdm-chat-row-title' }, chat.title || '未命名对话'),
              h('div', { className: 'dsdm-chat-row-meta' },
                timeLabel(chat.updatedAt),
                chat.tasks?.length ? ` · ${chat.tasks.length} 个 Harness 任务` : '')),
            h('div', { className: 'dsdm-chat-row-actions' },
              h('button', { type: 'button', className: 'dsdm-icon-button', title: '重命名', onClick: (event) => { event.stopPropagation(); void renameChat(chat); } }, h(IconPencil)),
              h('button', { type: 'button', className: 'dsdm-icon-button', title: '删除', onClick: (event) => { event.stopPropagation(); void deleteChat(chat); } }, h(IconTrash)))))),
          h('div', { className: 'dsdm-side-section' }, 'Harness Sessions'),
          h('div', { className: 'dsdm-session-list' },
            sessions.slice(0, 20).map((session) => h('button', {
              key: session.sessionId,
              type: 'button',
              className: 'dsdm-session-row',
              title: session.cwd || session.sessionId,
              onClick: () => {
                try {
                  props.uiWorkspace?.openSession({ sessionId: session.sessionId });
                  props.layout?.selectPanel(null);
                } catch (error) { toast(error.message); }
              },
            },
            h('span', { className: 'dsdm-session-title' }, session.title || session.sessionId.slice(0, 10)),
            session.running && h('span', { className: 'dsdm-dot is-busy' }))))),

        h('section', { className: 'dsdm-main' },
          h('header', { className: 'dsdm-topbar' },
            h('button', {
              type: 'button',
              className: 'dsdm-ghost-button dsdm-back',
              title: '返回 DeepSeek Harness',
              onClick: () => { try { props.layout?.selectPanel(null); } catch (error) { toast(error.message); } },
            }, '← Harness'),
            h('div', { className: 'dsdm-topbar-title' }, current?.title || 'DeepSeek Chat'),
            h('div', { className: 'dsdm-topbar-right' },
              h('button', {
                type: 'button',
                className: `dsdm-status dsdm-status-${webLabel(web).tone}`,
                onClick: () => api('/web/connect').then((data) => setState({ web: data.web })).catch((error) => toast(error.message)),
                title: '打开 Chrome 窗口，使用自己的 DeepSeek 网页账号登录',
              },
              h('span', { className: 'dsdm-status-dot' }), webLabel(web).text),
              h('button', {
                type: 'button',
                className: 'dsdm-ghost-button',
                title: web?.mode === 'visible'
                  ? '当前在可见 Chrome 窗口中运行，点击切换为静默后台模式'
                  : '当前静默后台运行，点击显示浏览器窗口（用于登录或查看网页端）',
                onClick: () => {
                  const next = web?.mode === 'visible' ? '/web/hide' : '/web/show';
                  api(next).then((data) => setState({ web: data.web })).catch((error) => toast(error.message));
                },
              }, web?.mode === 'visible' ? '窗口模式' : '静默模式'),
              h('button', {
                type: 'button',
                className: 'dsdm-ghost-button',
                onClick: () => {
                  api('/web/new').then((data) => setState({ web: data.web })).then(() => refreshChats()).catch((error) => toast(error.message));
                },
              }, '网页新对话'))),

          !current && h('div', { className: 'dsdm-hero' },
            h('h1', null, 'DeepSeek Chat'),
            h('p', null, '使用你自己的 DeepSeek 网页账号对话。消息通过真实浏览器会话发送，不经过 Harness API，也不产生 API 费用。'),
            h('button', { type: 'button', className: 'dsdm-primary-button', onClick: createChat }, '新建网页对话')),

          current && h('div', { className: 'dsdm-conversation' },
            h('div', {
              className: 'dsdm-messages',
              ref: scrollRef,
              onScroll: (event) => {
                const node = event.currentTarget;
                setState({ scroll: { ...state.scroll, [current.id]: node.scrollTop } });
              },
            },
            current.messages.length === 0 && h('div', { className: 'dsdm-empty' },
              h('div', { className: 'dsdm-empty-title' }, '开始一段真实网页对话'),
              h('div', { className: 'dsdm-empty-hint' }, '连接的是 chat.deepseek.com 的网页界面，回复来自网页端模型。')),
            current.messages.map((message) => h(MessageRow, { key: message.id, message })),

            current.tasks?.length > 0 && h('div', { className: 'dsdm-task-cards' },
              current.tasks.map((task) => h('div', { key: task.taskId, className: 'dsdm-task-card' },
                h('div', { className: 'dsdm-task-head' },
                  h('span', { className: 'dsdm-task-title' }, `→ ${task.sessionTitle || task.harnessSessionId.slice(0, 10)}`),
                  h('span', { className: `dsdm-task-status is-${task.status}` }, '已发送')),
                h('div', { className: 'dsdm-task-meta' }, `发送于 ${timeLabel(task.sentAt)} · 最近同步 ${timeLabel(task.lastSyncAt)}`),
                h('div', { className: 'dsdm-task-actions' },
                  h('button', {
                    type: 'button',
                    className: 'dsdm-ghost-button',
                    onClick: () => { void openResultModal(current, task.harnessSessionId); },
                  }, '获取执行结果'),
                  h('button', {
                    type: 'button',
                    className: 'dsdm-ghost-button',
                    onClick: () => {
                      try {
                        props.uiWorkspace?.openSession({ sessionId: task.harnessSessionId });
                        props.layout?.selectPanel(null);
                      } catch (error) { toast(error.message); }
                    },
                  }, '打开 Harness 会话'))))))),

          current && h('div', { className: 'dsdm-composer-wrap' },
            h('div', { className: 'dsdm-composer-tools' },
              h('button', {
                type: 'button',
                className: `dsdm-chip${toggles.deepThink ? ' is-on' : ''}`,
                onClick: () => setToggles((value) => ({ ...value, deepThink: !value.deepThink })),
                title: '深度思考（以网页端实际支持为准）',
              }, '深度思考'),
              h('button', {
                type: 'button',
                className: `dsdm-chip${toggles.search ? ' is-on' : ''}`,
                onClick: () => setToggles((value) => ({ ...value, search: !value.search })),
                title: '联网搜索（以网页端实际支持为准）',
              }, '智能搜索'),
              h('span', { className: 'dsdm-grow' }),
              h('button', { type: 'button', className: 'dsdm-action-button', onClick: openExtract }, '提取指令 → Harness'),
              h('button', { type: 'button', className: 'dsdm-action-button', onClick: openResult }, '获取 Harness 执行结果')),
            h('div', { className: 'dsdm-composer' },
              h('textarea', {
                ref: inputRef,
                value: draft,
                rows: 3,
                placeholder: '给网页端 DeepSeek 发送消息（Enter 发送，Shift+Enter 换行）',
                onChange: (event) => setDraft(event.target.value),
                onKeyDown,
              }),
              h('div', { className: 'dsdm-composer-bottom' },
                h('span', { className: 'dsdm-counter' }, draft.length ? `${draft.length} 字符` : ''),
                busy[current.id]
                  ? h('button', { type: 'button', className: 'dsdm-send-button is-stop', onClick: () => { void stopChat(current.id); } }, h(IconStop), '停止')
                  : h('button', { type: 'button', className: 'dsdm-send-button', onClick: submit, disabled: !draft.trim() }, h(IconSend), '发送'))),
            current.connection?.error && h('div', { className: 'dsdm-banner is-error' },
              `网页连接异常：${current.connection.error}`,
              h('button', { type: 'button', className: 'dsdm-ghost-button', onClick: () => { void stopChat(current.id); } }, '重试'))
            )
          ),
        modal && h(ModalHost, { modal })
      );
    }

    function MessageRow({ message }) {
      const [copied, setCopied] = useState(false);
      const streaming = message.status === 'streaming';
      const copy = () => {
        navigator.clipboard?.writeText(message.text || '');
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      };
      return h('div', { className: `dsdm-message dsdm-message-${message.role}` },
        h('div', { className: 'dsdm-message-head' },
          h('span', { className: 'dsdm-message-role' }, message.role === 'user' ? '你' : 'DeepSeek 网页'),
          h('button', { type: 'button', className: 'dsdm-copy-button', onClick: copy }, copied ? '已复制' : '复制')),
        h('div', { className: 'dsdm-message-body' },
          message.role === 'assistant'
            ? h(Markdown, { text: message.text })
            : h('div', { className: 'dsdm-user-text' }, message.text),
          streaming && !message.text && h('div', { className: 'dsdm-typing' }, h('span'), h('span'), h('span')),
          streaming && message.text && h('span', { className: 'dsdm-caret' })),
        message.error && h('div', { className: 'dsdm-banner is-error' }, message.error),
        message.status === 'stopped' && h('div', { className: 'dsdm-note' }, '已停止生成'));
    }

    /* ---------------------------------------------------------------- */
    /* modals                                                            */
    /* ---------------------------------------------------------------- */

    function updateModal(patch) {
      setState({ modal: { ...state.modal, ...patch } });
    }

    async function openResultModal(chat, sessionId) {
      setState({ modal: { type: 'result', chatId: chat.id, sessionId, loading: true, error: null } });
      try {
        const data = await api('/harness/result', { sessionId, chatId: chat.id });
        setState({ modal: { type: 'result', chatId: chat.id, sessionId, loading: false, error: null, result: data.result, sent: false } });
      } catch (error) {
        setState({ modal: { type: 'result', chatId: chat.id, sessionId, loading: false, error: error.message } });
      }
    }

    function ModalHost({ modal }) {
      if (modal.type === 'extract') return h(ExtractModal, { modal });
      if (modal.type === 'result') return h(ResultModal, { modal });
      return null;
    }

    function closeModal() { setState({ modal: null }); }

    function ExtractModal({ modal }) {
      if (modal.loading) {
        return h('div', { className: 'dsdm-overlay' }, h('div', { className: 'dsdm-dialog' },
          h('div', { className: 'dsdm-dialog-head' }, h('h2', null, '提取指令 → Harness'), h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body' }, '正在整理当前网页对话…')));
      }
      if (modal.error) {
        return h('div', { className: 'dsdm-overlay' }, h('div', { className: 'dsdm-dialog' },
          h('div', { className: 'dsdm-dialog-head' }, h('h2', null, '提取指令 → Harness'), h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body' }, h('div', { className: 'dsdm-banner is-error' }, modal.error))));
      }
      const sessions = state.sessions.filter((session) => !modal.search || (session.title || '').toLowerCase().includes(modal.search.toLowerCase()));
      const send = async () => {
        if (modal.sending) return;
        updateModal({ sending: true, error: null });
        try {
          const data = await api('/harness/send', {
            chatId: modal.chatId,
            sessionId: modal.createNew ? null : (modal.sessionId || null),
            workspaceId: modal.workspaceId || null,
            markdown: modal.markdown,
          });
          updateModal({ sending: false, sent: data.task });
          await refreshChats();
          toast(`已发送到 Harness 会话：${data.task.sessionTitle}`);
        } catch (error) {
          updateModal({ sending: false, error: error.message });
        }
      };
      return h('div', { className: 'dsdm-overlay', onMouseDown: (event) => { if (event.target === event.currentTarget) closeModal(); } },
        h('div', { className: 'dsdm-dialog dsdm-dialog-wide' },
          h('div', { className: 'dsdm-dialog-head' },
            h('h2', null, '提取指令 → Harness'),
            h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body dsdm-extract-grid' },
            h('div', { className: 'dsdm-extract-main' },
              h('div', { className: 'dsdm-field-label' }, '任务指令（可编辑）'),
              h('textarea', {
                className: 'dsdm-extract-text',
                value: modal.markdown,
                onChange: (event) => updateModal({ markdown: event.target.value }),
              }),
              h('label', { className: 'dsdm-check' },
                h('input', {
                  type: 'checkbox',
                  checked: !!modal.includeRaw,
                  onChange: async (event) => {
                    const includeRaw = event.target.checked;
                    updateModal({ includeRaw });
                    try {
                      const data = await api('/extract', { chatId: modal.chatId, includeRaw });
                      updateModal({ markdown: data.markdown });
                    } catch (error) { updateModal({ error: error.message }); }
                  },
                }),
                '附带原始网页对话')),
            h('div', { className: 'dsdm-extract-side' },
              h('div', { className: 'dsdm-field-label' }, '目标工作区'),
              h('select', {
                className: 'dsdm-select',
                value: modal.workspaceId,
                onChange: (event) => updateModal({ workspaceId: event.target.value }),
              },
              state.workspaces.map((workspace) => h('option', { key: workspace.id, value: workspace.id }, workspace.title || workspace.path))),
              h('div', { className: 'dsdm-field-label' }, '目标 Harness 会话'),
              h('label', { className: 'dsdm-radio' },
                h('input', {
                  type: 'radio',
                  checked: !!modal.createNew,
                  onChange: () => updateModal({ createNew: true }),
                }),
                '创建新的 Harness 会话'),
              h('label', { className: 'dsdm-radio' },
                h('input', {
                  type: 'radio',
                  checked: !modal.createNew,
                  onChange: () => updateModal({ createNew: false }),
                }),
                '发送到现有会话'),
              !modal.createNew && h('div', { className: 'dsdm-session-pick' },
                h('input', {
                  className: 'dsdm-search-input',
                  placeholder: '搜索会话',
                  value: modal.search,
                  onChange: (event) => updateModal({ search: event.target.value }),
                }),
                h('div', { className: 'dsdm-session-pick-list' },
                  sessions.map((session) => h('button', {
                    key: session.sessionId,
                    type: 'button',
                    className: `dsdm-session-pick-row${modal.sessionId === session.sessionId ? ' is-active' : ''}`,
                    onClick: () => updateModal({ sessionId: session.sessionId }),
                  },
                  h('span', null, session.title || session.sessionId.slice(0, 10)),
                  session.running && h('span', { className: 'dsdm-dot is-busy' })))),
                modal.sessionId && h('div', { className: 'dsdm-hint' }, `目标：${(state.sessions.find((s) => s.sessionId === modal.sessionId)?.title) || modal.sessionId}`)))),
          h('div', { className: 'dsdm-dialog-foot' },
            modal.sent
              ? h('div', { className: 'dsdm-sent' },
                h('span', { className: 'dsdm-ok' }, `✓ 已发送到 ${modal.sent.sessionTitle}`),
                h('button', {
                  type: 'button',
                  className: 'dsdm-ghost-button',
                  onClick: () => {
                    try {
                      props?.uiWorkspace?.openSession({ sessionId: modal.sent.harnessSessionId });
                      closeModal();
                      window.__DSH_LAYOUT__?.selectPanel?.(null);
                    } catch { /* ignore */ }
                  },
                }, '跳转查看'))
              : h(React.Fragment, null,
                modal.error && h('span', { className: 'dsdm-error-text' }, modal.error),
                h('button', { type: 'button', className: 'dsdm-ghost-button', onClick: closeModal }, '取消'),
                h('button', {
                  type: 'button',
                  className: 'dsdm-primary-button',
                  disabled: !!modal.sending,
                  onClick: send,
                }, modal.sending ? '发送中…' : '确认发送')))));
    }

    function ResultModal({ modal }) {
      if (modal.loading) {
        return h('div', { className: 'dsdm-overlay' }, h('div', { className: 'dsdm-dialog' },
          h('div', { className: 'dsdm-dialog-head' }, h('h2', null, 'Harness 执行结果'), h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body' }, '正在读取 Harness 会话状态…')));
      }
      if (modal.error) {
        return h('div', { className: 'dsdm-overlay' }, h('div', { className: 'dsdm-dialog' },
          h('div', { className: 'dsdm-dialog-head' }, h('h2', null, 'Harness 执行结果'), h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body' }, h('div', { className: 'dsdm-banner is-error' }, modal.error))));
      }
      const result = modal.result;
      const summary = buildResultSummary(result);
      const sendBack = async () => {
        if (!window.confirm('将执行摘要发送到当前 DeepSeek 网页对话？发送后网页端模型可以看到这些内容。')) return;
        try {
          const chatId = modal.chatId;
          const chat = state.chats.find((item) => item.id === chatId);
          if (!chat) throw new Error('网页会话不存在');
          setState({ modal: null });
          setState({ currentId: chatId });
          await sendChat(chatId, summary, {});
          toast('已发送到网页对话');
        } catch (error) { toast(error.message); }
      };
      return h('div', { className: 'dsdm-overlay', onMouseDown: (event) => { if (event.target === event.currentTarget) closeModal(); } },
        h('div', { className: 'dsdm-dialog dsdm-dialog-wide' },
          h('div', { className: 'dsdm-dialog-head' },
            h('h2', null, `Harness 执行结果 · ${result.title}`),
            h('button', { type: 'button', className: 'dsdm-icon-button', onClick: closeModal }, '✕')),
          h('div', { className: 'dsdm-dialog-body' },
            h('div', { className: 'dsdm-result-status' },
              result.running ? '正在执行' : '当前空闲',
              result.queueLength ? ` · 队列 ${result.queueLength}` : ''),
            h('div', { className: 'dsdm-result-section' }, '已完成工作'),
            h('div', { className: 'dsdm-result-text' }, result.assistantText || '（暂无助手输出）'),
            result.files.length > 0 && h('div', null,
              h('div', { className: 'dsdm-result-section' }, '修改文件'),
              h('ul', { className: 'dsdm-result-list' }, result.files.map((file) => h('li', { key: file.path },
                h('code', null, file.display || file.path),
                `  +${file.added} −${file.deleted}`)))),
            result.errors.length > 0 && h('div', null,
              h('div', { className: 'dsdm-result-section' }, '错误 / 测试失败'),
              h('ul', { className: 'dsdm-result-list is-error' }, result.errors.map((item, index) => h('li', { key: index }, item)))),
            result.openQuestions.length > 0 && h('div', null,
              h('div', { className: 'dsdm-result-section' }, '需要进一步讨论'),
              h('ul', { className: 'dsdm-result-list' }, result.openQuestions.map((item, index) => h('li', { key: index }, item)))),
            h('div', { className: 'dsdm-hint' }, `工具调用：${result.tools.length ? result.tools.join('、') : '无'} · 最近同步 ${timeLabel(result.updatedAt)}`)),
          h('div', { className: 'dsdm-dialog-foot' },
            h('button', { type: 'button', className: 'dsdm-ghost-button', onClick: closeModal }, '关闭'),
            h('button', { type: 'button', className: 'dsdm-primary-button', onClick: sendBack }, '确认后发送到网页对话'))));
    }

    function buildResultSummary(result) {
      const lines = [];
      lines.push(`### Harness 执行摘要 · ${result.title}`);
      lines.push('');
      lines.push('**已完成工作：**');
      lines.push('');
      lines.push(result.assistantText || '（暂无助手输出）');
      if (result.files.length) {
        lines.push('');
        lines.push('**修改文件：**');
        for (const file of result.files) lines.push(`- \`${file.display || file.path}\` (+${file.added} −${file.deleted})`);
      }
      if (result.errors.length) {
        lines.push('');
        lines.push('**错误 / 测试失败：**');
        for (const error of result.errors) lines.push(`- ${error}`);
      }
      if (result.openQuestions.length) {
        lines.push('');
        lines.push('**需要进一步讨论：**');
        for (const question of result.openQuestions) lines.push(`- ${question}`);
      }
      lines.push('');
      lines.push('以上摘要由本地 Harness 会话生成，请帮我分析并给出下一步建议。');
      return lines.join('\n');
    }

    /* ---------------------------------------------------------------- */
    /* Harness → Chat inquiry (DOM dialog + message actions)             */
    /* ---------------------------------------------------------------- */

    function pickMessageRow(start, root) {
      let node = start;
      let candidate = null;
      while (node && node !== root) {
        const cls = typeof node.className === 'string' ? node.className : '';
        if (/(^|[\s_-])(message|bubble|markdown|prose|chat-node|content)([\s_-]|$)/i.test(cls) && (node.innerText || '').trim().length > 0) {
          candidate = node;
        }
        node = node.parentElement;
      }
      if (candidate) return candidate;
      let top = start;
      while (top && top.parentElement !== root) top = top.parentElement;
      return top && top !== root ? top : null;
    }

    function rowSelectionText(row) {
      const selection = window.getSelection();
      if (selection && selection.rangeCount > 0 && !selection.isCollapsed) {
        const range = selection.getRangeAt(0);
        if (row && row.contains(range.commonAncestorContainer)) return selection.toString().trim();
        return selection.toString().trim();
      }
      return (row?.innerText || '').trim();
    }

    function el(tag, attrs, ...children) {
      const node = document.createElement(tag);
      for (const [key, value] of Object.entries(attrs || {})) {
        if (key === 'class') node.className = value;
        else if (key === 'text') node.textContent = value;
        else if (key.startsWith('on')) node.addEventListener(key.slice(2).toLowerCase(), value);
        else node.setAttribute(key, value);
      }
      for (const child of children.flat()) {
        if (child == null) continue;
        node.append(typeof child === 'string' ? document.createTextNode(child) : child);
      }
      return node;
    }

    async function openInquire(payload) {
      if (!payload.text || !payload.text.trim()) { toast('没有可发送的内容'); return; }
      const chatsData = await api('/chats/list').catch(() => ({ chats: [] }));
      const chats = chatsData.chats || [];
      const overlay = el('div', { class: 'dsdm-overlay' });
      const dialog = el('div', { class: 'dsdm-dialog dsdm-dialog-wide' });
      const head = el('div', { class: 'dsdm-dialog-head' },
        el('h2', { text: '在 Chat 模式询问' }),
        el('button', { class: 'dsdm-icon-button', text: '✕', onclick: () => overlay.remove() }));
      const sourceLine = el('div', { class: 'dsdm-hint' }, '来源：', payload.sourceTitle || 'Harness 会话');
      const contentArea = el('textarea', { class: 'dsdm-extract-text', spellcheck: 'false' });
      contentArea.value = payload.text;
      const questionArea = el('textarea', { class: 'dsdm-question-input', rows: '2' });
      questionArea.value = payload.question || DEFAULT_QUESTION;
      const targetSelect = el('select', { class: 'dsdm-select' });
      for (const chat of chats) {
        const option = el('option', { value: chat.id }, chat.title || '未命名对话');
        targetSelect.append(option);
      }
      const newOption = el('option', { value: '__new__' }, '＋ 新建网页对话');
      targetSelect.append(newOption);
      if (payload.preferChatId && chats.some((chat) => chat.id === payload.preferChatId)) targetSelect.value = payload.preferChatId;
      const counter = el('span', { class: 'dsdm-counter' });
      const errorLine = el('span', { class: 'dsdm-error-text' });
      const sendButton = el('button', { class: 'dsdm-primary-button' }, '确认并发送');
      const cancelButton = el('button', { class: 'dsdm-ghost-button' }, '取消');

      const updateCounter = () => {
        counter.textContent = `${contentArea.value.length + questionArea.value.length} 字符`;
      };
      contentArea.addEventListener('input', updateCounter);
      questionArea.addEventListener('input', updateCounter);
      updateCounter();

      const body = el('div', { class: 'dsdm-dialog-body' },
        sourceLine,
        el('div', { class: 'dsdm-field-label' }, '选中内容（可编辑）'),
        contentArea,
        el('div', { class: 'dsdm-field-label' }, '目标网页会话'),
        targetSelect,
        el('div', { class: 'dsdm-field-label' }, '我的问题'),
        questionArea,
        el('div', { class: 'dsdm-hint' }, '发送前会自动脱敏常见的密钥、Token 与密码。内容会通过真实网页会话发送。'),
        counter);
      const foot = el('div', { class: 'dsdm-dialog-foot' }, errorLine, cancelButton, sendButton);

      cancelButton.addEventListener('click', () => overlay.remove());
      overlay.addEventListener('mousedown', (event) => { if (event.target === overlay) overlay.remove(); });
      sendButton.addEventListener('click', async () => {
        sendButton.disabled = true;
        sendButton.textContent = '发送中…';
        try {
          const composed = await api('/compose', {
            sourceTitle: payload.sourceTitle,
            text: contentArea.value,
            question: questionArea.value,
            context: payload.context || '',
          });
          let chatId = targetSelect.value;
          if (chatId === '__new__') {
            const created = await api('/chats/create', { title: (payload.sourceTitle || '网页对话').slice(0, 40) });
            chatId = created.chat.id;
          }
          await api('/chats/origin', {
            chatId,
            harnessSessionId: payload.harnessSessionId || null,
            messageId: payload.messageId || null,
          }).catch(() => {});
          overlay.remove();
          await refreshChats();
          setState({ currentId: chatId });
          try { window.__DSH_LAYOUT__?.selectPanel?.(PANEL_ID); } catch { /* ignore */ }
          await sendChat(chatId, composed.markdown, {});
        } catch (error) {
          errorLine.textContent = error.message;
          sendButton.disabled = false;
          sendButton.textContent = '确认并发送';
        }
      });

      dialog.append(head, body, foot);
      overlay.append(dialog);
      document.body.append(overlay);
      contentArea.focus();
    }

    function installContextMenu(ctx) {
      return () => {
        const onContextMenu = (event) => {
          if (!(event.target instanceof Element)) return;
          if (event.target.closest('input, textarea, [contenteditable="true"]')) return;
          const root = event.target.closest('[data-conversation-content]');
          if (!root) return;
          const row = pickMessageRow(event.target, root);
          if (!row) return;
          event.preventDefault();
          event.stopPropagation();
          const menu = el('div', { class: 'dsdm-context-menu' });
          const selection = window.getSelection();
          const hasSelection = selection && selection.rangeCount > 0 && !selection.isCollapsed;
          const copyText = rowSelectionText(row);
          const items = [
            { label: hasSelection ? '复制选中内容' : '复制消息', run: () => navigator.clipboard?.writeText(copyText) },
            { label: '在 Chat 模式询问', run: () => {
              close();
              void openInquire({
                text: copyText,
                sourceTitle: document.title,
                harnessSessionId: currentSessionIdFromDom(),
                messageId: null,
                preferChatId: state.modal?.preferChatId,
              });
            }, primary: true },
          ];
          const close = () => {
            menu.remove();
            document.removeEventListener('mousedown', onDown, true);
            document.removeEventListener('keydown', onKey, true);
          };
          const onDown = (event) => { if (!menu.contains(event.target)) close(); };
          const onKey = (event) => { if (event.key === 'Escape') close(); };
          for (const item of items) {
            const button = el('button', { class: `dsdm-menu-item${item.primary ? ' is-primary' : ''}` },
              item.primary ? el('span', { class: 'dsdm-menu-glyph', text: '◍' }) : null,
              el('span', { text: item.label }));
            button.addEventListener('click', () => { item.run(); });
            menu.append(button);
          }
          menu.style.left = `${Math.min(event.clientX, window.innerWidth - 240)}px`;
          menu.style.top = `${Math.min(event.clientY, window.innerHeight - 140)}px`;
          document.body.append(menu);
          document.addEventListener('mousedown', onDown, true);
          document.addEventListener('keydown', onKey, true);
        };
        document.addEventListener('contextmenu', onContextMenu, true);
        return () => document.removeEventListener('contextmenu', onContextMenu, true);
      };
    }

    function currentSessionIdFromDom() {
      const content = document.querySelector('[data-conversation-content]');
      if (!content) return null;
      const id = content.getAttribute('data-conversation-session');
      return id || null;
    }

    function AssistantAskButton(props) {
      const ref = useRef(null);
      return h('button', {
        type: 'button',
        ref,
        className: 'dsdm-msg-action',
        title: '在 Chat 模式询问',
        onClick: () => {
          const root = document.querySelector('[data-conversation-content]');
          const row = root ? pickMessageRow(ref.current, root) : null;
          void openInquire({
            text: rowSelectionText(row),
            sourceTitle: document.title,
            harnessSessionId: currentSessionIdFromDom(),
            messageId: props.messageId || null,
          });
        },
      }, h(IconChat, { size: 14 }), h('span', null, '在 Chat 模式询问'));
    }

    /* ---------------------------------------------------------------- */
    /* styles                                                            */
    /* ---------------------------------------------------------------- */

    const CSS = `
.dsdm-brand{position:relative;display:flex;align-items:center;min-width:0}
.dsdm-brand-button{display:flex;align-items:center;gap:6px;background:transparent;border:0;color:var(--dsw-alias-label-primary);cursor:pointer;padding:4px 6px;border-radius:8px;font:inherit;max-width:100%}
.dsdm-brand-button:hover{background:var(--dsw-alias-bg-layer-2)}
.dsdm-brand-name{font-weight:600;font-size:15px;letter-spacing:-.02em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dsdm-brand-mode{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--dsw-alias-label-secondary);line-height:16px}
.dsdm-brand-caret{display:inline-flex;color:var(--dsw-alias-label-secondary);opacity:0;transition:opacity .12s}
.dsdm-brand-button:hover .dsdm-brand-caret{opacity:.7}
.dsdm-back{flex:0 0 auto}
.dsdm-menu{position:absolute;top:calc(100% + 6px);left:0;z-index:4000;min-width:262px;padding:6px;background:var(--dsw-alias-bg-overlay,#fff);border:1px solid var(--dsw-alias-border-l1);border-radius:12px;box-shadow:0 12px 32px rgba(0,0,0,.18)}
.dsdm-menu-item{display:flex;align-items:center;gap:8px;width:100%;padding:8px 10px;border:0;border-radius:8px;background:transparent;color:var(--dsw-alias-label-primary);cursor:pointer;font:inherit;text-align:left}
.dsdm-menu-item:hover{background:var(--dsw-alias-bg-layer-2)}
.dsdm-menu-item.is-active{background:var(--dsw-alias-bg-layer-2)}
.dsdm-menu-item.is-primary{color:var(--dsw-alias-brand-primary);font-weight:600}
.dsdm-menu-text{flex:1}
.dsdm-menu-sub{font-size:11px;color:var(--dsw-alias-label-secondary)}
.dsdm-menu-glyph{width:15px;text-align:center;color:var(--dsw-alias-label-secondary)}
.dsdm-panel-icon{display:inline-flex;color:var(--dsw-alias-label-secondary)}
.dsdm-panel-icon.is-active{color:var(--dsw-alias-brand-primary)}
.dsdm-root{display:flex;height:100%;min-height:0;background:var(--dsw-alias-bg-base);color:var(--dsw-alias-label-primary);font-size:13px;font-family:inherit}
.dsdm-side{width:252px;flex:0 0 252px;border-right:1px solid var(--dsw-alias-border-l1);display:flex;flex-direction:column;min-height:0;background:var(--dsw-alias-bg-layer-1)}
.dsdm-side-head{display:flex;align-items:center;justify-content:space-between;padding:14px 12px 8px}
.dsdm-side-title{font-weight:650;font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:var(--dsw-alias-label-secondary)}
.dsdm-search{padding:0 12px 8px}
.dsdm-search input{width:100%;box-sizing:border-box;padding:6px 8px;border-radius:8px;border:1px solid var(--dsw-alias-border-l1);background:var(--dsw-alias-bg-base);color:inherit;font:inherit;outline:none}
.dsdm-search input:focus{border-color:var(--dsw-alias-brand-primary)}
.dsdm-chat-list{flex:1;overflow:auto;padding:0 8px}
.dsdm-chat-row{display:flex;align-items:center;gap:4px;padding:8px;border-radius:8px;cursor:pointer}
.dsdm-chat-row:hover{background:var(--dsw-alias-bg-layer-2)}
.dsdm-chat-row.is-active{background:var(--dsw-alias-bg-layer-2);box-shadow:inset 2px 0 0 var(--dsw-alias-brand-primary)}
.dsdm-chat-row-main{flex:1;min-width:0}
.dsdm-chat-row-title{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:550}
.dsdm-chat-row-meta{font-size:11px;color:var(--dsw-alias-label-secondary);margin-top:2px}
.dsdm-chat-row-actions{display:none;gap:2px}
.dsdm-chat-row:hover .dsdm-chat-row-actions{display:flex}
.dsdm-icon-button{border:0;background:transparent;color:var(--dsw-alias-label-secondary);cursor:pointer;border-radius:6px;padding:4px;display:inline-flex;align-items:center;justify-content:center;font:inherit}
.dsdm-icon-button:hover{background:var(--dsw-alias-bg-layer-2);color:var(--dsw-alias-label-primary)}
.dsdm-side-section{padding:12px 14px 6px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--dsw-alias-label-secondary);border-top:1px solid var(--dsw-alias-border-l1)}
.dsdm-session-list{max-height:34%;overflow:auto;padding:0 8px 10px}
.dsdm-session-row{display:flex;align-items:center;gap:6px;width:100%;border:0;background:transparent;color:inherit;padding:6px 8px;border-radius:7px;cursor:pointer;font:inherit;text-align:left}
.dsdm-session-row:hover{background:var(--dsw-alias-bg-layer-2)}
.dsdm-session-title{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}
.dsdm-dot{width:6px;height:6px;border-radius:50%;background:var(--dsw-alias-state-idle-primary)}
.dsdm-dot.is-busy{background:var(--dsw-alias-state-warn-primary);animation:dsdm-pulse 1.4s infinite}
@keyframes dsdm-pulse{0%,100%{opacity:1}50%{opacity:.35}}
.dsdm-main{flex:1;display:flex;flex-direction:column;min-width:0;min-height:0}
.dsdm-topbar{display:flex;align-items:center;gap:10px;padding:10px 16px;border-bottom:1px solid var(--dsw-alias-border-l1);min-height:44px}
.dsdm-topbar-title{font-weight:600;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dsdm-topbar-right{display:flex;align-items:center;gap:8px}
.dsdm-status{display:inline-flex;align-items:center;gap:6px;font:inherit;font-size:11px;padding:4px 9px;border-radius:999px;cursor:pointer;border:1px solid var(--dsw-alias-border-l1);background:var(--dsw-alias-bg-layer-1);color:var(--dsw-alias-label-secondary)}
.dsdm-status-dot{width:6px;height:6px;border-radius:50%;background:var(--dsw-alias-state-idle-primary)}
.dsdm-status-ok .dsdm-status-dot{background:var(--dsw-alias-state-success-primary)}
.dsdm-status-warn .dsdm-status-dot{background:var(--dsw-alias-state-warn-primary)}
.dsdm-status-err .dsdm-status-dot{background:var(--dsw-alias-state-error-primary)}
.dsdm-ghost-button{border:1px solid var(--dsw-alias-border-l1);background:transparent;color:var(--dsw-alias-label-secondary);border-radius:8px;padding:5px 10px;font:inherit;font-size:12px;cursor:pointer}
.dsdm-ghost-button:hover{color:var(--dsw-alias-label-primary);border-color:var(--dsw-alias-border-l2)}
.dsdm-primary-button{border:0;background:var(--dsw-alias-brand-primary);color:#fff;border-radius:8px;padding:7px 14px;font:inherit;font-size:12.5px;font-weight:600;cursor:pointer}
.dsdm-primary-button:disabled{opacity:.5;cursor:default}
.dsdm-hero{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;padding:32px;text-align:center;color:var(--dsw-alias-label-secondary)}
.dsdm-hero h1{font-size:24px;margin:0;color:var(--dsw-alias-label-primary)}
.dsdm-hero p{max-width:520px;line-height:1.7;margin:0 0 8px}
.dsdm-conversation{flex:1;display:flex;flex-direction:column;min-height:0}
.dsdm-messages{flex:1;overflow:auto;padding:18px 22px 8px;scroll-behavior:smooth}
.dsdm-empty{text-align:center;color:var(--dsw-alias-label-secondary);padding:56px 0}
.dsdm-empty-title{font-size:15px;color:var(--dsw-alias-label-primary);margin-bottom:6px}
.dsdm-empty-hint{font-size:12px}
.dsdm-empty-small{padding:14px;color:var(--dsw-alias-label-secondary);font-size:12px}
.dsdm-message{margin:0 0 18px;max-width:min(760px,100%)}
.dsdm-message-user{margin-left:auto}
.dsdm-message-head{display:flex;align-items:center;gap:8px;margin-bottom:4px}
.dsdm-message-role{font-size:11px;font-weight:650;letter-spacing:.04em;color:var(--dsw-alias-label-secondary)}
.dsdm-message-user .dsdm-message-head{justify-content:flex-end}
.dsdm-copy-button{border:0;background:transparent;color:var(--dsw-alias-label-secondary);font-size:11px;cursor:pointer;opacity:0;transition:opacity .15s}
.dsdm-message:hover .dsdm-copy-button{opacity:1}
.dsdm-message-body{background:var(--dsw-alias-bg-layer-1);border:1px solid var(--dsw-alias-border-l1);border-radius:12px;padding:10px 14px;line-height:1.7;overflow-wrap:anywhere}
.dsdm-message-user .dsdm-message-body{background:var(--dsw-alias-bg-layer-2)}
.dsdm-user-text{white-space:pre-wrap}
.dsdm-md p{margin:0 0 8px}
.dsdm-md p:last-child{margin-bottom:0}
.dsdm-h{font-size:14px;margin:10px 0 6px}
.dsdm-list{margin:4px 0 8px;padding-left:20px}
.dsdm-quote{border-left:3px solid var(--dsw-alias-border-l2);margin:6px 0;padding:2px 10px;color:var(--dsw-alias-label-secondary)}
.dsdm-inline-code{background:var(--dsw-alias-bg-base);border:1px solid var(--dsw-alias-border-l1);border-radius:5px;padding:0 4px;font-size:12px}
.dsdm-code{border:1px solid var(--dsw-alias-border-l1);border-radius:10px;overflow:hidden;margin:8px 0;background:var(--dsw-alias-bg-base)}
.dsdm-code-head{display:flex;align-items:center;justify-content:space-between;padding:6px 10px;font-size:11px;color:var(--dsw-alias-label-secondary);border-bottom:1px solid var(--dsw-alias-border-l1)}
.dsdm-code-copy{border:0;background:transparent;color:inherit;cursor:pointer;font-size:11px}
.dsdm-code pre{margin:0;padding:10px 12px;overflow:auto;font-size:12px;line-height:1.6}
.dsdm-typing{display:flex;gap:4px;padding:4px 0}
.dsdm-typing span{width:5px;height:5px;border-radius:50%;background:var(--dsw-alias-label-secondary);animation:dsdm-bounce 1.2s infinite}
.dsdm-typing span:nth-child(2){animation-delay:.15s}
.dsdm-typing span:nth-child(3){animation-delay:.3s}
@keyframes dsdm-bounce{0%,60%,100%{transform:translateY(0);opacity:.4}30%{transform:translateY(-3px);opacity:1}}
.dsdm-caret{display:inline-block;width:7px;height:14px;background:var(--dsw-alias-brand-primary);vertical-align:-2px;animation:dsdm-blink 1s steps(2) infinite}
@keyframes dsdm-blink{50%{opacity:0}}
.dsdm-note{font-size:11px;color:var(--dsw-alias-label-secondary);margin-top:4px}
.dsdm-banner{margin:8px 0;padding:7px 10px;border-radius:8px;font-size:12px;display:flex;align-items:center;gap:10px;justify-content:space-between}
.dsdm-banner.is-error{background:color-mix(in srgb,var(--dsw-alias-state-error-primary) 12%,transparent);color:var(--dsw-alias-state-error-primary);border:1px solid color-mix(in srgb,var(--dsw-alias-state-error-primary) 35%,transparent)}
.dsdm-task-cards{display:flex;flex-direction:column;gap:8px;padding:0 22px 8px}
.dsdm-task-card{border:1px solid var(--dsw-alias-border-l1);border-radius:10px;padding:8px 12px;background:var(--dsw-alias-bg-layer-1)}
.dsdm-task-head{display:flex;align-items:center;gap:8px}
.dsdm-task-title{flex:1;font-size:12.5px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dsdm-task-status{font-size:10.5px;padding:1px 7px;border-radius:999px;background:var(--dsw-alias-bg-layer-2);color:var(--dsw-alias-label-secondary)}
.dsdm-task-status.is-sent{color:var(--dsw-alias-state-success-primary)}
.dsdm-task-meta{font-size:11px;color:var(--dsw-alias-label-secondary);margin:3px 0 6px}
.dsdm-task-actions{display:flex;gap:6px}
.dsdm-composer-wrap{padding:0 22px 14px;flex:0 0 auto}
.dsdm-composer-tools{display:flex;align-items:center;gap:8px;padding:6px 0}
.dsdm-grow{flex:1}
.dsdm-chip{border:1px solid var(--dsw-alias-border-l1);background:transparent;color:var(--dsw-alias-label-secondary);border-radius:999px;padding:3px 10px;font:inherit;font-size:11.5px;cursor:pointer}
.dsdm-chip.is-on{color:var(--dsw-alias-brand-primary);border-color:var(--dsw-alias-brand-primary);background:color-mix(in srgb,var(--dsw-alias-brand-primary) 10%,transparent)}
.dsdm-action-button{border:1px solid var(--dsw-alias-border-l1);background:var(--dsw-alias-bg-layer-1);color:var(--dsw-alias-label-primary);border-radius:8px;padding:4px 10px;font:inherit;font-size:12px;cursor:pointer}
.dsdm-action-button:hover{border-color:var(--dsw-alias-brand-primary);color:var(--dsw-alias-brand-primary)}
.dsdm-composer{border:1px solid var(--dsw-alias-border-l1);border-radius:14px;background:var(--dsw-alias-bg-layer-1);padding:10px 12px}
.dsdm-composer:focus-within{border-color:var(--dsw-alias-brand-primary)}
.dsdm-composer textarea{width:100%;box-sizing:border-box;border:0;background:transparent;color:inherit;font:inherit;line-height:1.6;resize:none;outline:none;min-height:58px}
.dsdm-composer-bottom{display:flex;align-items:center;justify-content:flex-end;gap:10px}
.dsdm-counter{flex:1;font-size:11px;color:var(--dsw-alias-label-secondary)}
.dsdm-send-button{display:inline-flex;align-items:center;gap:6px;border:0;border-radius:9px;background:var(--dsw-alias-brand-primary);color:#fff;padding:7px 13px;font:inherit;font-size:12.5px;cursor:pointer}
.dsdm-send-button:disabled{opacity:.45;cursor:default}
.dsdm-send-button.is-stop{background:var(--dsw-alias-state-error-primary)}
.dsdm-overlay{position:fixed;inset:0;z-index:2000;background:rgba(0,0,0,.42);display:flex;align-items:center;justify-content:center;padding:24px}
.dsdm-dialog{width:min(520px,100%);max-height:86vh;display:flex;flex-direction:column;background:var(--dsw-alias-bg-base);color:var(--dsw-alias-label-primary);border:1px solid var(--dsw-alias-border-l1);border-radius:16px;box-shadow:0 24px 64px rgba(0,0,0,.32);overflow:hidden}
.dsdm-dialog-wide{width:min(920px,100%)}
.dsdm-dialog-head{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--dsw-alias-border-l1)}
.dsdm-dialog-head h2{margin:0;font-size:15px}
.dsdm-dialog-body{padding:16px 18px;overflow:auto;display:flex;flex-direction:column;gap:8px}
.dsdm-dialog-foot{display:flex;align-items:center;justify-content:flex-end;gap:10px;padding:12px 18px;border-top:1px solid var(--dsw-alias-border-l1)}
.dsdm-extract-grid{display:grid;grid-template-columns:minmax(0,1.4fr) 280px;gap:18px;align-items:start}
.dsdm-extract-main,.dsdm-extract-side{display:flex;flex-direction:column;gap:6px;min-width:0}
.dsdm-extract-text{width:100%;box-sizing:border-box;min-height:340px;max-height:52vh;resize:vertical;border:1px solid var(--dsw-alias-border-l1);border-radius:10px;padding:10px;background:var(--dsw-alias-bg-layer-1);color:inherit;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;line-height:1.6;outline:none}
.dsdm-question-input{width:100%;box-sizing:border-box;min-height:76px;border:1px solid var(--dsw-alias-border-l1);border-radius:10px;padding:8px 10px;background:var(--dsw-alias-bg-layer-1);color:inherit;font:inherit;font-size:12.5px;resize:vertical;outline:none}
.dsdm-field-label{font-size:11px;font-weight:650;letter-spacing:.04em;text-transform:uppercase;color:var(--dsw-alias-label-secondary);margin-top:6px}
.dsdm-select{width:100%;box-sizing:border-box;border:1px solid var(--dsw-alias-border-l1);border-radius:8px;background:var(--dsw-alias-bg-layer-1);color:inherit;font:inherit;padding:6px 8px;outline:none}
.dsdm-search-input{width:100%;box-sizing:border-box;border:1px solid var(--dsw-alias-border-l1);border-radius:8px;background:var(--dsw-alias-bg-base);color:inherit;font:inherit;padding:5px 8px;outline:none}
.dsdm-check,.dsdm-radio{display:flex;align-items:center;gap:7px;font-size:12px;color:var(--dsw-alias-label-secondary);cursor:pointer}
.dsdm-session-pick{display:flex;flex-direction:column;gap:6px;margin-top:2px}
.dsdm-session-pick-list{max-height:180px;overflow:auto;border:1px solid var(--dsw-alias-border-l1);border-radius:8px;padding:4px}
.dsdm-session-pick-row{display:flex;align-items:center;gap:6px;width:100%;border:0;background:transparent;color:inherit;font:inherit;font-size:12px;text-align:left;padding:5px 7px;border-radius:6px;cursor:pointer}
.dsdm-session-pick-row:hover{background:var(--dsw-alias-bg-layer-2)}
.dsdm-session-pick-row.is-active{background:color-mix(in srgb,var(--dsw-alias-brand-primary) 14%,transparent);color:var(--dsw-alias-brand-primary)}
.dsdm-hint{font-size:11px;color:var(--dsw-alias-label-secondary);line-height:1.5}
.dsdm-error-text{flex:1;font-size:12px;color:var(--dsw-alias-state-error-primary)}
.dsdm-ok{color:var(--dsw-alias-state-success-primary);font-size:12px}
.dsdm-sent{display:flex;align-items:center;gap:12px;flex:1}
.dsdm-result-status{font-size:12px;color:var(--dsw-alias-label-secondary)}
.dsdm-result-section{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--dsw-alias-label-secondary);margin:10px 0 4px}
.dsdm-result-text{white-space:pre-wrap;font-size:12.5px;line-height:1.65;max-height:260px;overflow:auto;background:var(--dsw-alias-bg-layer-1);border:1px solid var(--dsw-alias-border-l1);border-radius:8px;padding:10px}
.dsdm-result-list{margin:2px 0;padding-left:18px;font-size:12px;line-height:1.7}
.dsdm-result-list.is-error{color:var(--dsw-alias-state-error-primary)}
.dsdm-context-menu{position:fixed;z-index:2100;min-width:200px;padding:5px;background:var(--dsw-alias-bg-overlay,#fff);border:1px solid var(--dsw-alias-border-l1);border-radius:10px;box-shadow:0 12px 32px rgba(0,0,0,.22)}
.dsdm-msg-action{display:inline-flex;align-items:center;gap:5px;border:0;background:transparent;color:var(--dsw-alias-label-secondary);font:inherit;font-size:11.5px;padding:3px 7px;border-radius:6px;cursor:pointer}
.dsdm-msg-action:hover{background:var(--dsw-alias-bg-layer-2);color:var(--dsw-alias-brand-primary)}
@media (max-width: 860px){.dsdm-extract-grid{grid-template-columns:1fr}.dsdm-side{width:200px;flex-basis:200px}}
`;

    /* ---------------------------------------------------------------- */
    /* apply                                                             */
    /* ---------------------------------------------------------------- */

    function apply(ctx) {
      try {
        const styles = ctx.get('styles');
        if (styles?.insert) styles.insert(CSS);
        else {
          const styleEl = document.createElement('style');
          styleEl.setAttribute('data-dsh-dual-mode', '1');
          styleEl.textContent = CSS;
          document.head.appendChild(styleEl);
          ctx.effect(() => () => { try { styleEl.remove(); } catch { /* ignore */ } });
        }

      const layout = ctx.get('layout');
      const uiWorkspace = ctx.get('uiWorkspace');
      try { window.__DSH_LAYOUT__ = layout; } catch { /* ignore */ }

      ctx.effect(() => {
        const dispose = installContextMenu(ctx);
        return () => dispose();
      });

      ctx.slots.inject('sidebar.brand.name', () => ctx.slots.register(
        { name: 'sidebar.brand.name', locale: true, priority: -10 },
        (props) => h(BrandModeSwitcher, { ...props, layout }),
      ));

      ctx.slots.inject('sidebar.panellist', () => ctx.slots.register(
        { name: 'sidebar.panellist', id: PANEL_ID, order: 5, label: 'DeepSeek Chat' },
        PanelIcon,
      ));

      ctx.slots.inject('main', () => ctx.slots.register(
        { name: 'main', key: PANEL_ID },
        (props) => h(ChatWorkspace, { ...props, layout, uiWorkspace }),
      ));

      ctx.slots.inject('conversation.chat.assistant-actions', () => ctx.slots.register(
        { name: 'conversation.chat.assistant-actions', id: 'dual-mode-ask', order: 30, label: '在 Chat 模式询问' },
        AssistantAskButton,
      ));


      // Bootstrap data once the module is live.
      api('/bootstrap')
        .then(async (data) => {
          setState({
            booted: true,
            loading: false,
            chats: data.chats,
            workspaces: data.workspaces,
            web: data.web,
            currentId: data.chats[0]?.id || null,
            defaultQuestion: data.defaultQuestion || DEFAULT_QUESTION,
          });
          if (!data.chats.length) {
            try {
              const created = await api('/chats/create', {});
              setState({ chats: [created.chat], currentId: created.chat.id });
            } catch { /* ignore */ }
          }
          void loadSessions();
        })
        .catch((error) => setState({ booted: true, loading: false, error: error.message }));

        return { name: 'dsh-dual-mode' };
      } catch (error) {
        console.error('dsh-dual-mode apply failed', error);
        throw error;
      }
    }

    return {
      name: 'dsh-dual-mode',
      inject: ['slots', 'locale'],
      apply,
    };
  },
});
