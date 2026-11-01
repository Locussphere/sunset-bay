# Sunset Bay online (GitHub Pages + Render + Supabase)

Players, shared building, planted trees and text chat. No voice.

```
docs/index.html   the game (static, goes on GitHub Pages)
server/           realtime server (Node + WebSocket, goes on Render)
render.yaml       Render blueprint
```

## 1. Database (Supabase)
1. Create a free project at supabase.com.
2. Open the project's **Connect** / Database settings and copy the **pooler** connection string
   (Session or Transaction pooler). Replace `[YOUR-PASSWORD]` with your database password.
   Use the pooler string, not the "direct" one: the direct one needs IPv6.
3. Nothing else: the server creates its own table (`sb_state`) on first start.
   Any Postgres works instead (Neon, Render Postgres...). Just give the server its `DATABASE_URL`.

## 2. Server (Render)
1. Push this folder to a GitHub repo.
2. On render.com: **New > Blueprint** (or **New > Web Service**), pick the repo.
   If you make the service by hand: root directory `server`, build `npm install`, start `npm start`, plan Free.
3. Set the environment variable `DATABASE_URL` to the Supabase string.
4. Optional: `ALLOWED_ORIGINS=https://YOUR-NAME.github.io` so only your site can connect.
5. Deploy. Open `https://YOUR-SERVICE.onrender.com/` and you should see `{"ok":true,...}`.

Free Render services go to sleep when idle, so the first player after a quiet spell waits
about a minute. The game shows "Waking the server..." and keeps retrying.

## 3. Game (GitHub Pages)
1. In `docs/index.html` find `var SB_SERVER_URL='__SB_SERVER__';` and put your server there:
   `var SB_SERVER_URL='wss://YOUR-SERVICE.onrender.com';`
2. Commit and push. In the repo: **Settings > Pages > Deploy from a branch > main, folder /docs**.
3. Open `https://YOUR-NAME.github.io/REPO/`.

To try a server without editing the file, add `?server=wss://YOUR-SERVICE.onrender.com` to the URL once;
the browser remembers it.

## Run it on your own computer
```
cd server && npm install && npm start      # listens on :8080, saves to state.json without DATABASE_URL
```
Open `docs/index.html?server=ws://localhost:8080` in two browser windows.

## How it behaves
- Everyone builds anywhere: pieces, blocks and planted trees are shared and saved in Postgres.
- Inventory, quests, farming and your look stay in each player's browser.
- Chat: Enter (or the Chat button). 160 characters, one message every 0.7 seconds, last 60 lines kept.
- Limits in the server: 40 players, 4000 pieces, 9000 blocks, 800 planted trees, build-action rate limit.
- Not included: accounts, moderation tools, anti-cheat. Anyone can remove anything. Fine for friends;
  add logins and an owner/ban list before opening it to the public.
