"use strict";

const express   = require("express");
const http      = require("http");
const WebSocket = require("ws");
const axios     = require("axios");

// ──────────────────────────────────────────────────────────────────────────────
//  Configuration
// ──────────────────────────────────────────────────────────────────────────────
const PORT     = parseInt(process.env.GATEWAY_PORT || "3000", 10);
const REPLICAS = (process.env.REPLICAS || "").split(",").filter(Boolean);

// ──────────────────────────────────────────────────────────────────────────────
//  Express + HTTP server
// ──────────────────────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(express.json());

// CORS – allow browser to call /cluster-state from any origin
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ──────────────────────────────────────────────────────────────────────────────
//  Leader tracking
// ──────────────────────────────────────────────────────────────────────────────
let currentLeaderUrl = null;   // e.g. "http://replica2:4002"
let discovering      = false;

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11,23)}][Gateway] ${msg}`);
}

/**
 * Poll all replicas; return URL of the one that reports role==="leader".
 * Also accepts a hint from any follower's leaderId field.
 */
async function discoverLeader() {
  if (discovering) return currentLeaderUrl;
  discovering = true;

  const results = await Promise.allSettled(
    REPLICAS.map(url =>
      axios.get(`${url}/state`, { timeout: 400 }).then(r => ({ url, ...r.data }))
    )
  );

  let hint = null;
  for (const r of results) {
    if (r.status !== "fulfilled") continue;
    if (r.value.role === "leader") {
      currentLeaderUrl = r.value.url;
      log(`Leader = ${currentLeaderUrl}`);
      discovering = false;
      return currentLeaderUrl;
    }
    if (r.value.leader && !hint) hint = r.value.leader;
  }

  // No leader yet; try the hinted ID
  if (hint) {
    const hintUrl = REPLICAS.find(u => u.includes(hint));
    if (hintUrl) {
      try {
        const r = await axios.get(`${hintUrl}/state`, { timeout: 400 });
        if (r.data.role === "leader") {
          currentLeaderUrl = hintUrl;
          log(`Leader (via hint) = ${currentLeaderUrl}`);
          discovering = false;
          return currentLeaderUrl;
        }
      } catch { /* ignore */ }
    }
  }

  currentLeaderUrl = null;
  log("No leader found yet");
  discovering = false;
  return null;
}

// Continuous leader health-check every 300 ms
setInterval(async () => {
  if (!currentLeaderUrl) { await discoverLeader(); return; }
  try {
    const r = await axios.get(`${currentLeaderUrl}/state`, { timeout: 350 });
    if (r.data.role !== "leader") {
      log(`${currentLeaderUrl} is no longer leader – rediscovering`);
      currentLeaderUrl = null;
      await discoverLeader();
    }
  } catch {
    log(`Leader ${currentLeaderUrl} unreachable – rediscovering`);
    currentLeaderUrl = null;
    await discoverLeader();
  }
}, 300);

discoverLeader();

// ──────────────────────────────────────────────────────────────────────────────
//  Stroke forwarding  (with retry + redirect handling)
// ──────────────────────────────────────────────────────────────────────────────
async function sendStroke(stroke, attempt = 0) {
  if (attempt > 6) { log("Max retry exceeded for stroke"); return null; }

  if (!currentLeaderUrl) {
    await discoverLeader();
    if (!currentLeaderUrl) {
      await sleep(200);
      return sendStroke(stroke, attempt + 1);
    }
  }

  try {
    const res = await axios.post(`${currentLeaderUrl}/stroke`, stroke, { timeout: 1000 });
    if (res.data.redirect) {
      // Leader changed – update and retry
      const leaderUrl = REPLICAS.find(u => u.includes(res.data.leader));
      currentLeaderUrl = leaderUrl || null;
      return sendStroke(stroke, attempt + 1);
    }
    return res.data;
  } catch {
    log("Stroke POST failed – rediscovering leader");
    currentLeaderUrl = null;
    await discoverLeader();
    return sendStroke(stroke, attempt + 1);
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ──────────────────────────────────────────────────────────────────────────────
//  WebSocket server
// ──────────────────────────────────────────────────────────────────────────────
const wss     = new WebSocket.Server({ server });
const clients = new Set();

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

wss.on("connection", async (ws) => {
  clients.add(ws);
  log(`Client connected  (total=${clients.size})`);

  // Replay committed log so new client canvas is in sync
  try {
    const leader = currentLeaderUrl || await discoverLeader();
    if (leader) {
      const r = await axios.get(`${leader}/committed-log`, { timeout: 1000 });
      ws.send(JSON.stringify({ type: "init", entries: r.data.entries || [] }));
    } else {
      ws.send(JSON.stringify({ type: "init", entries: [] }));
    }
  } catch {
    ws.send(JSON.stringify({ type: "init", entries: [] }));
  }

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === "stroke") {
      const result = await sendStroke(msg.data);
      if (result && result.committed) {
        // Broadcast to ALL clients (including sender – canvas dedup is client-side)
        broadcast({ type: "stroke", data: msg.data });
      }
    }
  });

  ws.on("close", () => { clients.delete(ws); log(`Client disconnected (total=${clients.size})`); });
  ws.on("error", ()  => { clients.delete(ws); });
});

// ──────────────────────────────────────────────────────────────────────────────
//  HTTP endpoints
// ──────────────────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.get("/cluster-state", async (_req, res) => {
  const results = await Promise.allSettled(
    REPLICAS.map(url =>
      axios.get(`${url}/state`, { timeout: 500 }).then(r => r.data)
    )
  );
  res.json(results.map((r, i) =>
    r.status === "fulfilled"
      ? r.value
      : { id: `replica${i + 1}`, role: "down", term: "—", logLength: "—", commitIndex: "—" }
  ));
});

// ──────────────────────────────────────────────────────────────────────────────
//  Start
// ──────────────────────────────────────────────────────────────────────────────
server.listen(PORT, () => log(`Listening on :${PORT}`));
