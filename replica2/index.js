"use strict";

const express = require("express");
const axios   = require("axios");
const fs      = require("fs");
const path    = require("path");

const app = express();
app.use(express.json());

// ──────────────────────────────────────────────────────────────────────────────
//  Configuration  (injected via docker-compose environment variables)
// ──────────────────────────────────────────────────────────────────────────────
const REPLICA_ID = process.env.REPLICA_ID || "replica1";
const PORT       = parseInt(process.env.REPLICA_PORT || "4001", 10);
const PEERS      = (process.env.PEERS || "").split(",").filter(Boolean);

const HEARTBEAT_MS    = 150;
const ELECTION_MIN_MS = 500;
const ELECTION_MAX_MS = 800;
const RPC_TIMEOUT_MS  = 300;

// ──────────────────────────────────────────────────────────────────────────────
//  Disk persistence — committed log survives full cluster restarts
// ──────────────────────────────────────────────────────────────────────────────
const DATA_DIR = path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "log.json");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function persistLog() {
  try {
    const snapshot = {
      log         : raft.log.slice(0, raft.commitIndex + 1),
      commitIndex : raft.commitIndex,
      currentTerm : raft.currentTerm,
    };
    fs.writeFileSync(LOG_FILE, JSON.stringify(snapshot), "utf8");
  } catch (e) {
    console.error(`[${REPLICA_ID}] persistLog error:`, e.message);
  }
}

function loadPersistedLog() {
  try {
    if (!fs.existsSync(LOG_FILE)) return;
    const snapshot = JSON.parse(fs.readFileSync(LOG_FILE, "utf8"));
    if (Array.isArray(snapshot.log) && snapshot.log.length > 0) {
      raft.log         = snapshot.log;
      raft.commitIndex = snapshot.commitIndex ?? snapshot.log.length - 1;
      raft.currentTerm = snapshot.currentTerm ?? 0;
      console.log(`[${REPLICA_ID}] Restored ${raft.log.length} committed entries from disk (commitIndex=${raft.commitIndex})`);
    }
  } catch (e) {
    console.error(`[${REPLICA_ID}] loadPersistedLog error — starting fresh:`, e.message);
  }
}

// ──────────────────────────────────────────────────────────────────────────────
//  Persistent RAFT State
// ──────────────────────────────────────────────────────────────────────────────
const raft = {
  currentTerm : 0,
  votedFor    : null,
  log         : [],     // [ { index, term, stroke } ]

  role        : "follower",
  leaderId    : null,
  commitIndex : -1,
  lastApplied : -1,

  nextIndex   : {},
  matchIndex  : {},
  votes       : 0,

  // FIX #4: Commit lock — prevents concurrent commit races
  _committingIndex: -1,
};

let electionTimer  = null;
let heartbeatTimer = null;

// ──────────────────────────────────────────────────────────────────────────────
//  Logging helper
// ──────────────────────────────────────────────────────────────────────────────
function L(msg) {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`[${ts}][${REPLICA_ID}][${raft.role.toUpperCase()}][T${raft.currentTerm}] ${msg}`);
}

// ──────────────────────────────────────────────────────────────────────────────
//  Timer helpers
// ──────────────────────────────────────────────────────────────────────────────
function randomElectionTimeout() {
  return Math.floor(Math.random() * (ELECTION_MAX_MS - ELECTION_MIN_MS + 1)) + ELECTION_MIN_MS;
}

function resetElectionTimer() {
  clearTimeout(electionTimer);
  electionTimer = setTimeout(startElection, randomElectionTimeout());
}

function stopElectionTimer()  { clearTimeout(electionTimer);   electionTimer  = null; }
function stopHeartbeatTimer() { clearInterval(heartbeatTimer); heartbeatTimer = null; }

// ──────────────────────────────────────────────────────────────────────────────
//  State transitions
// ──────────────────────────────────────────────────────────────────────────────
function becomeFollower(term, leaderId = null) {
  L(`-> FOLLOWER  (term=${term}, leader=${leaderId || "?"})`);
  raft.role        = "follower";
  raft.currentTerm = term;
  raft.votedFor    = null;
  raft.leaderId    = leaderId;
  raft.votes       = 0;
  raft._committingIndex = -1;
  stopHeartbeatTimer();
  resetElectionTimer();
}

function becomeCandidate() {
  raft.role        = "candidate";
  raft.currentTerm += 1;
  raft.votedFor    = REPLICA_ID;
  raft.votes       = 1;
  raft.leaderId    = null;
  L(`-> CANDIDATE (term=${raft.currentTerm})`);
  resetElectionTimer();
}

function becomeLeader() {
  L(`-> LEADER`);
  raft.role     = "leader";
  raft.leaderId = REPLICA_ID;
  raft._committingIndex = -1;
  stopElectionTimer();

  for (const peer of PEERS) {
    raft.nextIndex[peer]  = raft.log.length;
    raft.matchIndex[peer] = -1;
  }

  sendHeartbeats();
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_MS);
}

// ──────────────────────────────────────────────────────────────────────────────
//  FIX #3: Higher-term step-down — always enforced, even on current leader
// ──────────────────────────────────────────────────────────────────────────────
function maybeStepDown(remoteTerm) {
  if (remoteTerm > raft.currentTerm) {
    L(`Higher term ${remoteTerm} received — stepping down`);
    becomeFollower(remoteTerm);
    return true;
  }
  return false;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Helper: last log metadata
// ──────────────────────────────────────────────────────────────────────────────
function lastLogMeta() {
  const idx  = raft.log.length - 1;
  const term = idx >= 0 ? raft.log[idx].term : -1;
  return { index: idx, term };
}

// ──────────────────────────────────────────────────────────────────────────────
//  FIX #3: Overwrite-prevention guard
//  Returns true if entries would clobber a committed slot with a different term
// ──────────────────────────────────────────────────────────────────────────────
function wouldOverwriteCommitted(entries, insertFromIndex) {
  for (let i = 0; i < entries.length; i++) {
    const logIdx = insertFromIndex + i;
    if (logIdx <= raft.commitIndex) {
      const existing = raft.log[logIdx];
      if (existing && existing.term !== entries[i].term) {
        L(`SAFETY VIOLATION blocked: attempt to overwrite committed entry at index ${logIdx}`);
        return true;
      }
    }
  }
  return false;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Election
// ──────────────────────────────────────────────────────────────────────────────
async function startElection() {
  becomeCandidate();

  const { index: lastLogIndex, term: lastLogTerm } = lastLogMeta();

  const voteRequests = PEERS.map(peer =>
    axios.post(`${peer}/request-vote`, {
      term        : raft.currentTerm,
      candidateId : REPLICA_ID,
      lastLogIndex,
      lastLogTerm,
    }, { timeout: RPC_TIMEOUT_MS })
    .then(r => r.data)
    .catch(() => ({ voteGranted: false, term: 0 }))
  );

  const results = await Promise.all(voteRequests);

  if (raft.role !== "candidate") return;

  for (const { voteGranted, term } of results) {
    if (maybeStepDown(term)) return;
    if (voteGranted) {
      raft.votes += 1;
      L(`Got vote — total ${raft.votes}`);
      if (raft.votes >= majority()) {
        becomeLeader();
        return;
      }
    }
  }
  L(`Split vote — will retry`);
}

function majority() {
  return Math.floor((PEERS.length + 1) / 2) + 1;
}

// ──────────────────────────────────────────────────────────────────────────────
//  Heartbeats  (leader -> followers)
// ──────────────────────────────────────────────────────────────────────────────
async function sendHeartbeats() {
  if (raft.role !== "leader") return;

  for (const peer of PEERS) {
    axios.post(`${peer}/heartbeat`, {
      term        : raft.currentTerm,
      leaderId    : REPLICA_ID,
      commitIndex : raft.commitIndex,
    }, { timeout: RPC_TIMEOUT_MS })
    .then(r => {
      if (maybeStepDown(r.data.term)) return;
      if (r.data.needsSync !== undefined) {
        syncFollower(peer, r.data.needsSync);
      }
    })
    .catch(() => { /* peer down */ });
  }
}

// ──────────────────────────────────────────────────────────────────────────────
//  FIX #4: Atomic-safe log replication with commit lock
// ──────────────────────────────────────────────────────────────────────────────
async function replicateEntry(entry) {
  // Prevent concurrent replication for the same index (race condition guard)
  if (raft._committingIndex >= entry.index) {
    L(`Duplicate replication attempt for index ${entry.index} — skipped`);
    return false;
  }
  raft._committingIndex = entry.index;

  let acks = 1;  // leader counts itself

  const reqs = PEERS.map(peer => {
    const prevIdx  = entry.index - 1;
    const prevTerm = prevIdx >= 0 && raft.log[prevIdx] ? raft.log[prevIdx].term : -1;

    return axios.post(`${peer}/append-entries`, {
      term        : raft.currentTerm,
      leaderId    : REPLICA_ID,
      prevLogIndex: prevIdx,
      prevLogTerm : prevTerm,
      entries     : [entry],
      leaderCommit: raft.commitIndex,
    }, { timeout: 500 })
    .then(r => {
      if (maybeStepDown(r.data.term)) return;
      if (r.data.success) {
        acks += 1;
        raft.matchIndex[peer] = entry.index;
        raft.nextIndex[peer]  = entry.index + 1;
      } else if (r.data.needsSync !== undefined) {
        syncFollower(peer, r.data.needsSync);
      }
    })
    .catch(() => { /* peer down */ });
  });

  await Promise.allSettled(reqs);

  // FIX #4: Must still be leader AND same term before committing (Raft §5.4.2)
  if (raft.role !== "leader" || raft.currentTerm !== entry.term) {
    L(`Term/role changed during replication of index ${entry.index} — not committing`);
    raft._committingIndex = -1;
    return false;
  }

  const committed = acks >= majority() && entry.term === raft.currentTerm;
  raft._committingIndex = -1;
  return committed;
}

// ──────────────────────────────────────────────────────────────────────────────
//  FIX #2: Proper incremental catch-up sync  (leader -> lagging follower)
//  Only sends COMMITTED entries from fromIndex onward — spec-accurate
//  FIX duplicate sync: track in-flight syncs per peer to avoid double-send
// ──────────────────────────────────────────────────────────────────────────────
const syncInFlight = new Set();  // peers currently being synced

async function syncFollower(peer, fromIndex) {
  // Deduplicate: if a sync is already in flight to this peer, skip
  if (syncInFlight.has(peer)) return;
  syncInFlight.add(peer);
  try {
    const safeFrom = Math.max(0, fromIndex);
    // Only send up to commitIndex (never uncommitted entries)
    const missing  = raft.log.slice(safeFrom, raft.commitIndex + 1);
    if (missing.length === 0) return;

    L(`Syncing ${peer} from index ${safeFrom} to ${safeFrom + missing.length - 1} (${missing.length} committed entries)`);

    const r = await axios.post(`${peer}/sync-log`, {
      term        : raft.currentTerm,
      leaderId    : REPLICA_ID,
      fromIndex   : safeFrom,
      entries     : missing,
      commitIndex : raft.commitIndex,
    }, { timeout: 1000 });

    // Higher term always wins — step down if peer has a newer term
    if (r.data && r.data.term) maybeStepDown(r.data.term);
  } catch { /* peer still down — retry on next heartbeat */ }
  finally { syncInFlight.delete(peer); }
}

// ──────────────────────────────────────────────────────────────────────────────
//  Routes – Health / Info
// ──────────────────────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => res.json({ status: "ok", id: REPLICA_ID }));

app.get("/state", (_req, res) => res.json({
  id         : REPLICA_ID,
  role       : raft.role,
  term       : raft.currentTerm,
  leader     : raft.leaderId,
  logLength  : raft.log.length,
  commitIndex: raft.commitIndex,
}));

app.get("/committed-log", (_req, res) => {
  res.json({ entries: raft.log.slice(0, raft.commitIndex + 1) });
});

// ──────────────────────────────────────────────────────────────────────────────
//  Routes – RAFT RPCs
// ──────────────────────────────────────────────────────────────────────────────

// ── /request-vote ─────────────────────────────────────────────────────────────
app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;
  L(`RequestVote from ${candidateId} term=${term}`);

  // FIX #3: Step down immediately if higher term (even from candidates)
  if (term > raft.currentTerm) becomeFollower(term);

  const { index: myLastIdx, term: myLastTerm } = lastLogMeta();

  // FIX #1: Strict log up-to-date check (Raft §5.4.1)
  const logOk =
    lastLogTerm > myLastTerm ||
    (lastLogTerm === myLastTerm && lastLogIndex >= myLastIdx);

  const grant =
    term >= raft.currentTerm &&
    (raft.votedFor === null || raft.votedFor === candidateId) &&
    logOk;

  if (grant) {
    raft.votedFor    = candidateId;
    raft.currentTerm = term;
    resetElectionTimer();
    L(`Voted YES for ${candidateId}`);
  } else {
    L(`Voted NO for ${candidateId} (logOk=${logOk}, votedFor=${raft.votedFor})`);
  }

  res.json({ term: raft.currentTerm, voteGranted: grant });
});

// ── /heartbeat ────────────────────────────────────────────────────────────────
app.post("/heartbeat", (req, res) => {
  const { term, leaderId, commitIndex } = req.body;

  if (term < raft.currentTerm) {
    return res.json({ term: raft.currentTerm, success: false });
  }

  if (term > raft.currentTerm || raft.role !== "follower") {
    becomeFollower(term, leaderId);
  } else {
    raft.leaderId    = leaderId;
    raft.currentTerm = term;
    resetElectionTimer();
  }

  // FIX #3: Only advance commitIndex up to what we actually have in our log
  if (commitIndex > raft.commitIndex && commitIndex <= raft.log.length - 1) {
    raft.commitIndex = commitIndex;
    persistLog();
  }

  const needsSync = commitIndex > raft.log.length - 1 ? raft.log.length : undefined;

  res.json({ term: raft.currentTerm, success: true, ...(needsSync !== undefined && { needsSync }) });
});

// ── /append-entries ───────────────────────────────────────────────────────────
// FIX #1: Full Raft §5.3 consistency check with proper rollback on conflict
app.post("/append-entries", (req, res) => {
  const { term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

  if (term < raft.currentTerm) {
    return res.json({ term: raft.currentTerm, success: false });
  }

  if (term > raft.currentTerm || raft.role !== "follower") {
    becomeFollower(term, leaderId);
  } else {
    raft.leaderId    = leaderId;
    raft.currentTerm = term;
    resetElectionTimer();
  }

  // ── FIX #1: Strict prevLogIndex/prevLogTerm check (Raft §5.3) ─────────────
  if (prevLogIndex >= 0) {
    if (prevLogIndex >= raft.log.length) {
      // Missing the previous entry entirely
      L(`AppendEntries fail: need prevLogIndex=${prevLogIndex} but log length=${raft.log.length}`);
      return res.json({ term: raft.currentTerm, success: false, needsSync: raft.log.length });
    }
    const prevEntry = raft.log[prevLogIndex];
    if (prevEntry.term !== prevLogTerm) {
      // ── FIX #1: Rollback conflicting suffix, but protect committed entries ─
      L(`AppendEntries term mismatch at prevLogIndex=${prevLogIndex}: have term=${prevEntry.term}, need term=${prevLogTerm}`);
      // FIX #3: Never rollback past commitIndex
      const rollbackTo = Math.max(prevLogIndex, raft.commitIndex + 1);
      raft.log.splice(rollbackTo);
      L(`Log rolled back to length=${raft.log.length} (commitIndex=${raft.commitIndex} protected)`);
      return res.json({ term: raft.currentTerm, success: false, needsSync: raft.log.length });
    }
  }

  // ── FIX #1 + FIX #3: Smart append with conflict detection ─────────────────
  if (entries && entries.length > 0) {
    const insertFrom = prevLogIndex + 1;

    // FIX #3: Block any attempt to overwrite committed entries
    if (wouldOverwriteCommitted(entries, insertFrom)) {
      return res.json({ term: raft.currentTerm, success: false });
    }

    // FIX #1: Walk entries and truncate on first term conflict
    for (let i = 0; i < entries.length; i++) {
      const logIdx = insertFrom + i;
      if (logIdx < raft.log.length && raft.log[logIdx].term !== entries[i].term) {
        L(`Conflict at log index ${logIdx}: truncating tail`);
        raft.log.splice(logIdx);
        break;
      }
    }

    // Append only entries we don't already have
    for (let i = 0; i < entries.length; i++) {
      const logIdx = insertFrom + i;
      if (logIdx >= raft.log.length) {
        raft.log.push(entries[i]);
      }
    }

    L(`log length=${raft.log.length} after append-entries`);
  }

  // FIX #3: Clamp commitIndex to actual log length
  if (leaderCommit > raft.commitIndex) {
    raft.commitIndex = Math.min(leaderCommit, raft.log.length - 1);
    persistLog();
  }

  res.json({ term: raft.currentTerm, success: true });
});

// ── /sync-log ─────────────────────────────────────────────────────────────────
// FIX #2: Proper index-based incremental reconciliation from fromIndex onward
app.post("/sync-log", (req, res) => {
  const { term, leaderId, fromIndex, entries, commitIndex } = req.body;

  if (term < raft.currentTerm) {
    return res.json({ term: raft.currentTerm, success: false });
  }

  becomeFollower(term, leaderId);

  if (entries && entries.length > 0) {
    // FIX #2: Use explicit fromIndex (not inferred from entries[0].index)
    const startIdx = (fromIndex !== undefined) ? fromIndex : entries[0].index;

    // FIX #3: Never overwrite committed entries with conflicting terms
    if (wouldOverwriteCommitted(entries, startIdx)) {
      L(`sync-log: safety guard prevented overwriting committed entries`);
      return res.json({ term: raft.currentTerm, success: false });
    }

    // Precise splice: keep everything before startIdx, replace from there
    raft.log = raft.log.slice(0, startIdx).concat(entries);

    // FIX #2: Clamp commitIndex to our actual log length
    raft.commitIndex = Math.min(commitIndex, raft.log.length - 1);
    persistLog();
    L(`sync-log: restored ${entries.length} entries [${startIdx}..${raft.log.length - 1}] -> commitIndex=${raft.commitIndex}`);
  }

  res.json({ term: raft.currentTerm, success: true });
});

// ──────────────────────────────────────────────────────────────────────────────
//  Routes – Stroke ingestion  (called by gateway)
// ──────────────────────────────────────────────────────────────────────────────
app.post("/stroke", async (req, res) => {
  if (raft.role !== "leader") {
    return res.status(302).json({ redirect: true, leader: raft.leaderId });
  }

  const stroke = req.body;
  const entry  = {
    index  : raft.log.length,
    term   : raft.currentTerm,
    stroke,
  };

  raft.log.push(entry);
  L(`Stroke appended at index ${entry.index}`);

  const committed = await replicateEntry(entry);
  if (committed) {
    raft.commitIndex = entry.index;
    L(`Stroke committed at index ${entry.index}`);
    persistLog();
    return res.json({ success: true, committed: true, entry });
  }

  L(`Stroke not committed (insufficient acks or term changed)`);
  res.json({ success: false, committed: false });
});

// ──────────────────────────────────────────────────────────────────────────────
//  Boot
// ──────────────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  loadPersistedLog();
  L(`Listening on :${PORT}  peers=${PEERS.join(", ")}`);
  resetElectionTimer();
});
