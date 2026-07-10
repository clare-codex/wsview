# wsview

WebSocket 查看器，为 JSON-RPC 2.0 agent 应用调试而生。两种形态：

- **本地代理模式**：调试本地进程（Node/桌面 agent）的 WS 流量 —— 反向代理 + Web UI
- **Chrome 插件**：调试网页里的 WS 流量 —— DevTools 面板（见下文 [Chrome 插件](#chrome-插件)）

## 本地代理模式

```sh
npm install
npm start -- --target ws://localhost:8000    # agent 原本连接的服务端地址
```

然后：
1. 把 agent 的 WebSocket URL 改成 `ws://localhost:9800`（路径原样转发，如 `/rpc` → `<target>/rpc`；若 target 自带路径则按 target 为准）
2. 浏览器打开 **http://localhost:9801**

选项：`-p` 代理端口（9800）、`-u` UI 端口（9801）、`--ring` 内存事件上限（5000）。

## 功能

- 双向消息流实时展示，转发本身零改动（帧原样透传，含 subprotocol / 自定义 header）
- JSON-RPC 2.0 感知：request / response / notification / error 自动分类
- **请求-响应按 id 配对**，直接显示响应耗时；支持双向请求（server → client 也能配对）
- 按 method / id / payload 子串过滤，按消息类型、按连接筛选
- JSON 可折叠树形查看、长字符串折叠、copy raw、NDJSON 导出
- 多连接并发、断线重连、暂停 / 追尾滚动、j/k 键盘导航

## Demo

```sh
npm start -- --target ws://localhost:8890    # 终端 1
npm run demo                                 # 终端 2：模拟 server + agent
```

## Chrome 插件

抓当前页面创建的所有 WebSocket（MAIN world 包装 `WebSocket`，帧原样放行），UI 与代理模式同一套 viewer。

**安装**：`chrome://extensions` → 打开「开发者模式」→「加载已解压的扩展程序」→ 选 `extension/` 目录。

**使用**：在要调试的页面打开 DevTools → 「wsview」面板。页面加载起的所有 WS 连接、双向帧、JSON-RPC 配对/延迟都会出现；面板后开也能看到近期历史（后台 service worker 缓冲，休眠后丢弃属正常）。

限制：
- 看不到 Worker / Service Worker 里建的 WebSocket（只 patch 页面主世界）
- \>1MB 的帧只保留截断预览（分类/配对仍正常，meta 在截断前解析）

## 代码结构

- `src/server.ts` — 代理 + 事件缓冲 + UI feed（代理模式）
- `public/viewer.js` + `style.css` — **共享 viewer 核心**（分类/配对/渲染），改完跑 `npm run sync:ext` 同步到插件
- `public/app.js` — 网页版传输层（feed WebSocket）
- `extension/` — 插件：`inject.js`（MAIN world patch）→ `relay.js`（转发）→ `bg.js`（按 tab 缓冲/分发）→ `panel/`（DevTools 面板）
