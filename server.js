"use strict";

const http = require("http");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = Number(process.env.PORT || 10000);
const WS_TICK_MS = 1000 / 60;
const STATE_BROADCAST_MS = 33;
const TURN_TIME_LIMIT = 18;
const BASE_PX = 600;
const STONE_R_RATIO = 0.052;
const MAX_PULL = 100;
const MAX_SPEED = 17;
const POWER_CURVE = 1.15;
const SETTLE_EPS = 0.04;
const COLLISION_RESTITUTION = 0.90;
const BUMPER_RESTITUTION = 0.72;
const GAME_COST = { practice: 0, ai: 8, casual: 10, rank: 15 };
const RATING_DELTA = { win: 22, loss: -16, draw: 0 };

const MAPS = {
  classic: { friction: 0.978, radiusRatio: 0.42, bumpers: [] },
  ice: { friction: 0.991, radiusRatio: 0.42, bumpers: [] },
  bumper: { friction: 0.978, radiusRatio: 0.42, bumpers: [[0, -0.16], [0, 0.16], [-0.18, 0], [0.18, 0]] },
  mini: { friction: 0.974, radiusRatio: 0.30, bumpers: [] },
};

const UPGRADES = {
  mass: { max: 5, costs: [90, 135, 195, 270, 360] },
  grip: { max: 5, costs: [75, 115, 170, 235, 320] },
  power: { max: 5, costs: [95, 140, 210, 290, 385] },
  bounce: { max: 3, costs: [130, 215, 330] },
  aim: { max: 1, costs: [300] },
};

const SKINS = {
  basic: { cost: 0 },
  neon: { cost: 200 },
  gold: { cost: 470 },
  magma: { cost: 735 },
  aurora: { cost: 1070 },
};

const NAME_POOL = [
  "알까기왕", "돌격알", "알까기초보", "검은돌", "흰돌", "돌멩이", "알까기마스터",
  "한방알", "돌돌이", "알신", "스톤맨", "알까기고수", "쓱쓱이", "통통알",
  "돌격대", "알까기장인", "스톤킹", "알파돌", "빨간알", "파란알", "초록알",
  "보라알", "황금알", "무적알", "행운의알", "알폭탄", "돌의신", "슈퍼알",
  "알까기전사", "스톤히어로",
];

const DEFAULT_ACCOUNT = {
  nickname: "플레이어",
  wins: 0,
  games: 0,
  bestStreak: 0,
  curStreak: 0,
  playSeconds: 0,
  coins: 100,
  rankRating: 1000,
  rankWins: 0,
  rankGames: 0,
  upgrades: {},
  skins: ["basic"],
  skin: "basic",
};

// Render Web Service의 파일 시스템은 기본적으로 영속적이지 않으므로
// DATABASE_URL이 있으면 Postgres에 계정을 저장합니다. 없으면 로컬 테스트용 메모리 모드로 동작합니다.
const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false },
      max: 5,
    })
  : null;

const memoryAccounts = new Map();

async function initDb() {
  if (!pool) {
    console.warn("[DB] DATABASE_URL is not set; using temporary in-memory account storage.");
    return;
  }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS alkkagi_accounts (
      account_id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  console.log("[DB] Postgres ready");
}

function cloneAccount(x) {
  return JSON.parse(JSON.stringify(x));
}

function normalizeAccount(accountId, data) {
  const d = { ...cloneAccount(DEFAULT_ACCOUNT), ...(data || {}) };
  d.accountId = accountId;
  d.nickname = sanitizeNickname(d.nickname || "플레이어");
  d.wins = Math.max(0, Number(d.wins) || 0);
  d.games = Math.max(0, Number(d.games) || 0);
  d.bestStreak = Math.max(0, Number(d.bestStreak) || 0);
  d.curStreak = Math.max(0, Number(d.curStreak) || 0);
  d.playSeconds = Math.max(0, Number(d.playSeconds) || 0);
  d.coins = Math.max(0, Number(d.coins) || 0);
  d.rankRating = Number.isFinite(Number(d.rankRating)) ? Number(d.rankRating) : 1000;
  d.rankWins = Math.max(0, Number(d.rankWins) || 0);
  d.rankGames = Math.max(0, Number(d.rankGames) || 0);
  d.upgrades = d.upgrades && typeof d.upgrades === "object" ? d.upgrades : {};
  d.skins = Array.isArray(d.skins) && d.skins.length ? [...new Set(d.skins)] : ["basic"];
  if (!d.skins.includes("basic")) d.skins.unshift("basic");
  d.skin = d.skins.includes(d.skin) ? d.skin : "basic";
  return d;
}

async function loadAccount(accountId, nicknameHint) {
  if (pool) {
    const result = await pool.query("SELECT data FROM alkkagi_accounts WHERE account_id = $1", [accountId]);
    if (result.rowCount) return { account: normalizeAccount(accountId, result.rows[0].data), created: false };
    const created = normalizeAccount(accountId, { nickname: sanitizeNickname(nicknameHint || "플레이어") });
    await pool.query(
      "INSERT INTO alkkagi_accounts(account_id,data) VALUES($1,$2::jsonb) ON CONFLICT (account_id) DO NOTHING",
      [accountId, JSON.stringify(created)]
    );
    const reread = await pool.query("SELECT data FROM alkkagi_accounts WHERE account_id = $1", [accountId]);
    return { account: normalizeAccount(accountId, reread.rows[0]?.data || created), created: true };
  }

  if (!memoryAccounts.has(accountId)) {
    memoryAccounts.set(accountId, normalizeAccount(accountId, { nickname: nicknameHint || "플레이어" }));
    return { account: normalizeAccount(accountId, memoryAccounts.get(accountId)), created: true };
  }
  return { account: normalizeAccount(accountId, memoryAccounts.get(accountId)), created: false };
}

async function saveAccount(account) {
  const normalized = normalizeAccount(account.accountId, account);
  if (pool) {
    await pool.query(
      `INSERT INTO alkkagi_accounts(account_id,data)
       VALUES($1,$2::jsonb)
       ON CONFLICT(account_id)
       DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()`,
      [normalized.accountId, JSON.stringify(normalized)]
    );
  } else {
    memoryAccounts.set(normalized.accountId, cloneAccount(normalized));
  }
  return normalized;
}

function sanitizeNickname(name) {
  const n = String(name || "").trim().slice(0, 12);
  if (!n || !/^[가-힣a-zA-Z0-9 _-]+$/.test(n)) return "플레이어";
  return n;
}

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    console.error("[SEND]", err.message);
  }
}

function broadcastRoom(room, data, exceptClientId = null) {
  for (const clientId of room.clientIds) {
    if (clientId === exceptClientId) continue;
    const c = clients.get(clientId);
    if (c) send(c.ws, data);
  }
}

function broadcastRoomAll(room, data) {
  broadcastRoom(room, data, null);
}

function makeRoomId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
}

class ServerEngine {
  constructor({ map, playerCount, stoneCount, buffsByPlayer = null }) {
    this.mapDef = MAPS[map] || MAPS.classic;
    this.playerCount = playerCount;
    this.stoneCount = stoneCount;
    this.boardPx = BASE_PX;
    this.scale = 1;
    this.boardR = BASE_PX * this.mapDef.radiusRatio;
    this.cx = BASE_PX / 2;
    this.cy = BASE_PX / 2;
    this.stoneR = this.boardR * STONE_R_RATIO * (1 / 0.42) * 0.9;
    this.bumpers = this.mapDef.bumpers.map(([x, y]) => ({
      x: this.cx + x * BASE_PX,
      y: this.cy + y * BASE_PX,
      r: this.boardR * 0.09,
    }));
    this.buffsByPlayer = buffsByPlayer || null;
    this.stones = [];
    this.turn = 0;
    this.moving = false;
    this.winner = null;
    this.draw = false;
    this._genRandomStones();
  }

  _genRandomStones() {
    const maxR = this.boardR - this.stoneR * 1.35;
    const minGap = this.stoneR * 2.12;
    const positions = [];
    const total = this.playerCount * this.stoneCount;

    for (let id = 0; id < total; id++) {
      const owner = Math.floor(id / this.stoneCount);
      let placed = false;
      for (let attempt = 0; attempt < 700 && !placed; attempt++) {
        const angle = Math.random() * Math.PI * 2;
        const radius = Math.sqrt(Math.random()) * maxR * 0.92;
        const p = {
          x: this.cx + Math.cos(angle) * radius,
          y: this.cy + Math.sin(angle) * radius,
          owner,
        };
        let ok = true;
        for (const q of positions) {
          if (Math.hypot(p.x - q.x, p.y - q.y) < minGap) {
            ok = false;
            break;
          }
        }
        if (ok) {
          for (const b of this.bumpers) {
            if (Math.hypot(p.x - b.x, p.y - b.y) < b.r + this.stoneR * 1.25) {
              ok = false;
              break;
            }
          }
        }
        if (ok) {
          positions.push(p);
          placed = true;
        }
      }

      if (!placed) {
        const angle = (id * 2.399963229728653) % (Math.PI * 2);
        const radius = maxR * 0.55;
        positions.push({
          x: this.cx + Math.cos(angle) * radius,
          y: this.cy + Math.sin(angle) * radius,
          owner,
        });
      }
    }

    for (let id = 0; id < positions.length; id++) {
      const p = positions[id];
      const bf = this.buffsByPlayer?.[p.owner] || null;
      this.stones.push({
        id,
        owner: p.owner,
        x: p.x,
        y: p.y,
        vx: 0,
        vy: 0,
        r: this.stoneR,
        m: 1 + (bf?.massBonus || 0),
        fric: this.mapDef.friction - (bf?.gripBonus || 0),
        rest: COLLISION_RESTITUTION + (bf?.restBonus || 0),
        pw: 1 + (bf?.powerBonus || 0),
        alive: true,
      });
    }
  }

  aliveCountFor(owner) {
    return this.stones.filter((s) => s.owner === owner && s.alive).length;
  }

  alivePlayers() {
    return [...new Set(this.stones.filter((s) => s.alive).map((s) => s.owner))];
  }

  nextTurn() {
    const alive = this.alivePlayers();
    if (alive.length === 0) {
      this.winner = null;
      this.draw = true;
      return;
    }
    if (alive.length === 1) {
      this.winner = alive[0];
      this.draw = false;
      return;
    }
    for (let i = 1; i <= this.playerCount; i++) {
      const next = (this.turn + i) % this.playerCount;
      if (alive.includes(next)) {
        this.turn = next;
        return;
      }
    }
  }

  shoot(stoneId, dxNorm, dyNorm) {
    if (this.moving || this.winner != null || this.draw) return false;
    const s = this.stones.find((x) => x.id === stoneId && x.alive);
    if (!s) return false;
    let dx = Number(dxNorm) * BASE_PX;
    let dy = Number(dyNorm) * BASE_PX;
    const d0 = Math.hypot(dx, dy);
    if (!Number.isFinite(d0) || d0 < 4) return false;

    const maxPull = MAX_PULL;
    const d = Math.min(d0, maxPull);
    const p = Math.pow(d / maxPull, POWER_CURVE);
    const nx = dx / d0;
    const ny = dy / d0;
    const speed = MAX_SPEED * p * (s.pw || 1) / (1 + ((s.m || 1) - 1) * 0.25);
    s.vx = nx * speed;
    s.vy = ny * speed;
    this.moving = true;
    return true;
  }

  step() {
    if (!this.moving) return false;
    const eps = SETTLE_EPS;
    let any = false;

    for (const s of this.stones) {
      if (!s.alive) continue;
      s.x += s.vx;
      s.y += s.vy;
      const f = s.fric || this.mapDef.friction;
      s.vx *= f;
      s.vy *= f;
      if (Math.hypot(s.vx, s.vy) < eps) {
        s.vx = 0;
        s.vy = 0;
      } else {
        any = true;
      }
    }

    for (const s of this.stones) {
      if (!s.alive) continue;
      for (const b of this.bumpers) {
        let dx = s.x - b.x;
        let dy = s.y - b.y;
        let d = Math.hypot(dx, dy);
        const min = b.r + s.r;
        if (d < min) {
          if (d < 1e-7) {
            dx = 1;
            dy = 0;
            d = 1;
          }
          const nx = dx / d;
          const ny = dy / d;
          s.x = b.x + nx * min;
          s.y = b.y + ny * min;
          const vn = s.vx * nx + s.vy * ny;
          if (vn < 0) {
            s.vx -= (1 + BUMPER_RESTITUTION) * vn * nx;
            s.vy -= (1 + BUMPER_RESTITUTION) * vn * ny;
          }
          any = true;
        }
      }
    }

    const alive = this.stones.filter((s) => s.alive);
    for (let i = 0; i < alive.length; i++) {
      for (let j = i + 1; j < alive.length; j++) {
        const A = alive[i];
        const B = alive[j];
        let dx = B.x - A.x;
        let dy = B.y - A.y;
        let d = Math.hypot(dx, dy);
        const min = A.r + B.r;
        if (d < min) {
          if (d < 1e-7) {
            const q = (A.id * 1103515245 + B.id * 12345) >>> 0;
            const ang = (q % 360) * Math.PI / 180;
            dx = Math.cos(ang);
            dy = Math.sin(ang);
          } else {
            dx /= d;
            dy /= d;
          }
          const nx = dx;
          const ny = dy;
          const ov = min - Math.max(d, 1e-7);
          A.x -= nx * ov * 0.5;
          A.y -= ny * ov * 0.5;
          B.x += nx * ov * 0.5;
          B.y += ny * ov * 0.5;
          const invM = 1 / A.m + 1 / B.m;
          const rel = (B.vx - A.vx) * nx + (B.vy - A.vy) * ny;
          if (rel < 0) {
            const rest = ((A.rest || COLLISION_RESTITUTION) + (B.rest || COLLISION_RESTITUTION)) / 2;
            const imp = -(1 + rest) * rel / invM;
            A.vx -= imp * nx / A.m;
            A.vy -= imp * ny / A.m;
            B.vx += imp * nx / B.m;
            B.vy += imp * ny / B.m;
          }
          any = true;
        }
      }
    }

    for (const s of this.stones) {
      if (s.alive && Math.hypot(s.x - this.cx, s.y - this.cy) - s.r > this.boardR) {
        s.alive = false;
      }
    }

    this.moving = any;
    if (!this.moving) this.nextTurn();
    return true;
  }

  serialize(deadlineAt = null) {
    return {
      norm: true,
      stones: this.stones.map((s) => ({
        id: s.id,
        owner: s.owner,
        x: s.x / BASE_PX,
        y: s.y / BASE_PX,
        alive: s.alive,
      })),
      turn: this.turn,
      moving: this.moving,
      winner: this.winner,
      draw: this.draw,
      deadlineAt,
    };
  }
}

function buildAIBuffs() {
  return null;
}

function aiChooseShot(engine, playerIndex, level) {
  const cfg = level === "easy" ? { err: 0.55, pw: 0.70 } : level === "hard" ? { err: 0.10, pw: 1 } : { err: 0.28, pw: 0.85 };
  const mine = engine.stones.filter((s) => s.owner === playerIndex && s.alive);
  const enemies = engine.stones.filter((s) => s.owner !== playerIndex && s.alive);
  if (!mine.length || !enemies.length) return null;

  let best = null;
  let score = -Infinity;
  for (const my of mine) {
    for (const en of enemies) {
      const d = Math.hypot(en.x - my.x, en.y - my.y);
      const edge = engine.boardR - Math.hypot(en.x - engine.cx, en.y - engine.cy);
      const sc = -d * 0.6 + edge * 1.2;
      if (sc > score) {
        score = sc;
        best = { my, en };
      }
    }
  }
  let angle = Math.atan2(best.en.y - best.my.y, best.en.x - best.my.x);
  angle += (Math.random() - 0.5) * cfg.err;
  const power = Math.min(1, (0.55 + Math.random() * 0.45) * cfg.pw);
  const pull = MAX_PULL * power;
  return { stoneId: best.my.id, dx: Math.cos(angle) * pull / BASE_PX, dy: Math.sin(angle) * pull / BASE_PX };
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify({ ok: true, service: "alkkagi-server", db: !!pool }));
  }
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

const wss = new WebSocketServer({ server });
const clients = new Map();
const rooms = new Map();
const queues = new Map();
const usedNames = new Map();
let nextClientId = 1;

function randomName() {
  const available = NAME_POOL.filter((name) => !usedNames.has(name));
  if (available.length) return available[Math.floor(Math.random() * available.length)];
  let i = 1;
  while (usedNames.has(`손님${i}`)) i++;
  return `손님${i}`;
}

function setClientName(client, name) {
  const safe = sanitizeNickname(name);
  if (client.name === safe) return true;
  if (usedNames.has(safe) && usedNames.get(safe) !== client.id) {
    send(client.ws, { type: "name_taken", name: safe });
    return false;
  }
  if (client.name) usedNames.delete(client.name);
  client.name = safe;
  usedNames.set(safe, client.id);
  send(client.ws, { type: "name_changed", name: safe });
  if (client.roomId) {
    const room = rooms.get(client.roomId);
    if (room) broadcastRoom(room, { type: "player_name_changed", playerId: client.id, name: safe }, client.id);
  }
  return true;
}

function requireAuth(client) {
  return Boolean(client.accountId && client.account);
}

function publicAccount(account) {
  return cloneAccount(account);
}

async function sendAccount(client, extra = {}) {
  if (!client.account) return;
  client.account = normalizeAccount(client.account.accountId, client.account);
  send(client.ws, { type: "account_state", account: publicAccount(client.account), ...extra });
}

async function saveClientAccount(client, extra = {}) {
  if (!client.account) return;
  client.account = await saveAccount(client.account);
  await sendAccount(client, extra);
}

function accountCanBuy(account, cost) {
  return account.coins >= cost;
}

async function handleAuth(client, msg) {
  let accountId = String(msg.accountId || "").trim().slice(0, 64);
  if (!accountId) accountId = `guest-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const nickname = sanitizeNickname(msg.nickname || client.name);
  try {
    client.accountId = accountId;
    const loaded = await loadAccount(accountId, nickname);
    client.account = loaded.account;
    if (loaded.created && nickname) {
      client.account.nickname = nickname;
      await saveAccount(client.account);
    }
    setClientName(client, client.account.nickname);
    await sendAccount(client);
  } catch (err) {
    console.error("[AUTH]", err);
    send(client.ws, { type: "auth_failed" });
  }
}

function queueKey(msg) {
  const playerCount = Math.max(2, Math.min(4, Number(msg.playerCount) || 2));
  const map = MAPS[msg.map] ? msg.map : "classic";
  const stoneCount = Math.max(1, Math.min(4, Number(msg.stoneCount) || 2));
  const ranked = !!msg.ranked;
  return `${playerCount}:${map}:${stoneCount}:${ranked ? "rank" : "casual"}`;
}

function enqueueMatch(client, msg) {
  if (!requireAuth(client) || client.roomId) return;
  const key = queueKey(msg);
  const [pcStr, map, scStr, rankedLabel] = key.split(":");
  const playerCount = Number(pcStr);
  const stoneCount = Number(scStr);
  const ranked = rankedLabel === "rank";
  const cost = ranked ? GAME_COST.rank : GAME_COST.casual;

  if (!accountCanBuy(client.account, cost)) {
    send(client.ws, { type: "match_cancelled", reason: "insufficient_funds", cost });
    return;
  }

  client.searchKey = key;
  if (!queues.has(key)) queues.set(key, []);
  const queue = queues.get(key);
  if (!queue.includes(client.id)) queue.push(client.id);

  send(client.ws, {
    type: "searching",
    playerCount,
    map,
    stoneCount,
    ranked,
    queueSize: queue.length,
  });

  if (queue.length >= playerCount) {
    const ids = queue.splice(0, playerCount).filter((id) => clients.has(id));
    if (ids.length === playerCount) {
      const matched = ids.map((id) => clients.get(id));
      if (matched.every((c) => c.account && c.account.coins >= cost)) {
        createRoom({
          mode: "online",
          ranked,
          map,
          playerCount,
          stoneCount,
          clientList: matched,
        }).catch((err) => console.error("[MATCH CREATE]", err));
      } else {
        for (const c of matched) send(c.ws, { type: "match_cancelled", reason: "insufficient_funds", cost });
      }
    }
  }

  if (!queue.length) queues.delete(key);
}

function cancelSearch(client) {
  if (!client.searchKey) return;
  const q = queues.get(client.searchKey);
  if (q) {
    const i = q.indexOf(client.id);
    if (i >= 0) q.splice(i, 1);
    if (!q.length) queues.delete(client.searchKey);
  }
  client.searchKey = null;
  send(client.ws, { type: "search_cancelled" });
}

function makePlayers(clientList) {
  return clientList.map((client, index) => ({
    clientId: client.id,
    index,
    name: client.account?.nickname || client.name || `Player ${index + 1}`,
  }));
}

function startTurn(room) {
  room.turnDeadline = Date.now() + TURN_TIME_LIMIT * 1000;
  room.aiDueAt = null;
  if (room.aiFlags[room.engine.turn]) {
    room.aiDueAt = Date.now() + 500 + Math.random() * 400;
  }
  broadcastState(room, true);
}

function broadcastState(room, force = false) {
  const now = Date.now();
  if (!force && now - room.lastBroadcast < STATE_BROADCAST_MS) return;
  room.lastBroadcast = now;
  broadcastRoomAll(room, { type: "state", state: room.engine.serialize(room.turnDeadline) });
}

async function chargeEntry(room) {
  if (!room.entryCost) return true;
  for (const client of room.clientList) {
    if (!client.account || client.account.coins < room.entryCost) return false;
  }
  for (const client of room.clientList) {
    client.account.coins -= room.entryCost;
  }
  await Promise.all(room.clientList.map((c) => saveClientAccount(c)));
  return true;
}

async function createRoom({ mode, ranked = false, map = "classic", playerCount, stoneCount, clientList, aiFlags, aiLevel, buffsByPlayer = null }) {
  const roomId = makeRoomId();
  const effectiveFlags = Array.isArray(aiFlags) ? aiFlags.slice(0, playerCount) : Array.from({ length: playerCount }, (_, i) => i > 0);
  while (effectiveFlags.length < playerCount) effectiveFlags.push(false);

  const room = {
    id: roomId,
    mode,
    ranked: !!ranked,
    map: MAPS[map] ? map : "classic",
    playerCount,
    stoneCount,
    aiFlags: effectiveFlags,
    aiLevel: aiLevel || "normal",
    clientList,
    clientIds: clientList.map((c) => c.id),
    players: makePlayers(clientList),
    engine: new ServerEngine({ map, playerCount, stoneCount, buffsByPlayer }),
    turnDeadline: null,
    aiDueAt: null,
    lastBroadcast: 0,
    finished: false,
    entryCost: mode === "online" ? (ranked ? GAME_COST.rank : GAME_COST.casual) : mode === "ai" ? GAME_COST.ai : 0,
  };

  rooms.set(roomId, room);
  for (let i = 0; i < clientList.length; i++) {
    const client = clientList[i];
    client.roomId = roomId;
    client.searchKey = null;
    client.ownerIndex = mode === "online" ? i : 0;
  }

  try {
    const charged = await chargeEntry(room);
    if (!charged) {
      for (const c of clientList) send(c.ws, { type: "match_cancelled", reason: "insufficient_funds", cost: room.entryCost });
      rooms.delete(roomId);
      for (const c of clientList) {
        c.roomId = null;
        c.ownerIndex = null;
      }
      return;
    }

    for (const client of clientList) {
      send(client.ws, {
        type: "match_found",
        roomId,
        map: room.map,
        playerCount,
        stoneCount,
        playerIndex: client.ownerIndex,
        hostId: clientList[0].id,
        ranked: room.ranked,
        players: room.players,
        isHost: client.id === clientList[0].id,
      });
    }

    startTurn(room);
    broadcastRoomAll(room, {
      type: "game_started",
      roomId,
      mode,
      ranked: room.ranked,
      map: room.map,
      playerCount,
      stoneCount,
      playerIndex: clientList[0].ownerIndex,
      names: room.players.map((p) => p.name),
      players: room.players,
      aiFlags: room.aiFlags,
      aiLevel: room.aiLevel,
      state: room.engine.serialize(room.turnDeadline),
    });
    console.log(`[ROOM CREATE] ${roomId} ${mode} ${room.players.map((p) => p.name).join(", ")}`);
  } catch (err) {
    console.error("[ROOM]", err);
    rooms.delete(roomId);
    for (const c of clientList) {
      c.roomId = null;
      c.ownerIndex = null;
      send(c.ws, { type: "game_create_result", ok: false, reason: "server_error" });
    }
  }
}

function createPrivateGame(client, msg) {
  if (!requireAuth(client) || client.roomId) return;
  const mode = msg.mode === "ai" ? "ai" : "practice";
  const playerCount = mode === "ai"
    ? Math.max(2, Math.min(4, Number(msg.playerCount) || 2))
    : Math.max(2, Math.min(8, Number(msg.playerCount) || 4));
  const stoneCount = mode === "ai"
    ? 2
    : Math.max(1, Math.min(4, Number(msg.stoneCount) || 2));
  const map = MAPS[msg.map] ? msg.map : "classic";
  const aiFlags = mode === "ai"
    ? Array.from({ length: playerCount }, (_, i) => i > 0)
    : Array.from({ length: playerCount }, (_, i) => !!msg.aiFlags?.[i]);

  if (mode === "ai" && client.account.coins < GAME_COST.ai) {
    send(client.ws, { type: "game_create_result", ok: false, reason: "insufficient_funds", cost: GAME_COST.ai });
    return;
  }

  const u = client.account.upgrades || {};
  const buffsByPlayer = mode === "practice" || mode === "ai" ? [{
    massBonus: (Number(u.mass) || 0) * 0.18,
    gripBonus: (Number(u.grip) || 0) * 0.0030,
    powerBonus: (Number(u.power) || 0) * 0.04,
    restBonus: (Number(u.bounce) || 0) * 0.06,
  }] : null;

  createRoom({
    mode,
    ranked: false,
    map,
    playerCount,
    stoneCount,
    clientList: [client],
    aiFlags,
    aiLevel: String(msg.aiLevel || "normal"),
    buffsByPlayer,
  }).catch((err) => console.error("[PRIVATE GAME]", err));
}

function currentController(room) {
  if (room.mode === "online") {
    return room.clientList[room.engine.turn] || null;
  }
  return room.clientList[0] || null;
}

function canControl(client, room) {
  if (room.mode === "online") return room.engine.turn >= 0 && room.clientList[room.engine.turn]?.id === client.id;
  return room.clientList[0]?.id === client.id && !room.aiFlags[room.engine.turn];
}

function handleShoot(client, msg) {
  const room = client.roomId ? rooms.get(client.roomId) : null;
  if (!room || room.finished || room.engine.moving || room.engine.winner != null || room.engine.draw) return;
  if (!canControl(client, room)) return;

  const stoneId = Number(msg.stoneId);
  const stone = room.engine.stones.find((s) => s.id === stoneId && s.alive);
  if (!stone || stone.owner !== room.engine.turn) return;

  const dx = Math.max(-1, Math.min(1, Number(msg.dx) || 0));
  const dy = Math.max(-1, Math.min(1, Number(msg.dy) || 0));
  if (!room.engine.shoot(stoneId, dx, dy)) return;

  room.turnDeadline = null;
  room.aiDueAt = null;
  broadcastState(room, true);
}

async function finishRoom(room) {
  if (room.finished) return;
  if (room.engine.winner == null && !room.engine.draw) return;
  room.finished = true;
  const winner = room.engine.draw ? null : room.engine.winner;
  const draw = !!room.engine.draw;
  const rewards = new Map();

  if (room.mode === "practice") {
    for (const client of room.clientList) {
      send(client.ws, { type: "game_over", winner, draw, reward: { coin: 0, rp: null }, rp: null });
      client.roomId = null;
      client.ownerIndex = null;
    }
    setTimeout(() => rooms.delete(room.id), 1500);
    console.log(`[GAME OVER] ${room.id} practice winner=${winner} draw=${draw}`);
    return;
  }

  for (let i = 0; i < room.playerCount; i++) {
    const client = room.mode === "online" ? room.clientList[i] : room.clientList[0];
    if (!client || !client.account) continue;
    if (rewards.has(client.id)) continue;

    const isWinner = !draw && winner === i;
    const isRanked = room.mode === "online" && room.ranked;
    const coinReward = (!draw && isWinner) ? room.playerCount * 8 : 0;
    const rpDelta = isRanked ? (draw ? RATING_DELTA.draw : isWinner ? RATING_DELTA.win : RATING_DELTA.loss) : 0;

    client.account.games += 1;
    if (draw) {
      client.account.curStreak = 0;
    } else if (isWinner) {
      client.account.wins += 1;
      client.account.curStreak += 1;
      client.account.bestStreak = Math.max(client.account.bestStreak, client.account.curStreak);
    } else {
      client.account.curStreak = 0;
    }

    if (isRanked) {
      client.account.rankGames += 1;
      if (isWinner) client.account.rankWins += 1;
      client.account.rankRating = Math.max(0, client.account.rankRating + rpDelta);
    }

    client.account.coins += coinReward;
    rewards.set(client.id, { coin: coinReward, rp: rpDelta ? `${rpDelta > 0 ? "+" : ""}${rpDelta} RP` : null });
  }

  await Promise.all(room.clientList.map((client) => client.account ? saveAccount(client.account) : null));

  for (const client of room.clientList) {
    const reward = rewards.get(client.id) || { coin: 0, rp: null };
    send(client.ws, { type: "account_state", account: publicAccount(client.account), matchReward: reward });
    send(client.ws, { type: "game_over", winner, draw, reward, rp: reward.rp });
    client.roomId = null;
    client.ownerIndex = null;
  }

  setTimeout(() => rooms.delete(room.id), 1500);
  console.log(`[GAME OVER] ${room.id} winner=${winner} draw=${draw}`);
}

async function tickRooms() {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (room.finished) continue;

    if (room.engine.moving) {
      room.engine.step();
      broadcastState(room, false);
      if (!room.engine.moving) {
        if (room.engine.winner != null || room.engine.draw) {
          await finishRoom(room);
        } else {
          startTurn(room);
        }
      }
      continue;
    }

    if (room.engine.winner != null || room.engine.draw) {
      await finishRoom(room);
      continue;
    }

    if (room.aiFlags[room.engine.turn]) {
      if (room.aiDueAt && now >= room.aiDueAt) {
        const shot = aiChooseShot(room.engine, room.engine.turn, room.aiLevel);
        room.aiDueAt = null;
        room.turnDeadline = null;
        if (shot) {
          room.engine.shoot(shot.stoneId, shot.dx, shot.dy);
          broadcastState(room, true);
        } else {
          room.engine.nextTurn();
          startTurn(room);
        }
      }
      continue;
    }

    if (room.turnDeadline && now >= room.turnDeadline) {
      room.engine.nextTurn();
      if (room.engine.winner != null || room.engine.draw) await finishRoom(room);
      else startTurn(room);
    }
  }
}

wss.on("connection", (ws) => {
  const clientId = String(nextClientId++);
  const client = {
    id: clientId,
    ws,
    name: randomName(),
    accountId: null,
    account: null,
    roomId: null,
    ownerIndex: null,
    searchKey: null,
  };
  clients.set(clientId, client);
  usedNames.set(client.name, client.id);

  send(ws, { type: "welcome", clientId });

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!msg || typeof msg.type !== "string") return;

    try {
      switch (msg.type) {
        case "auth":
          await handleAuth(client, msg);
          break;
        case "set_name":
          setClientName(client, msg.name);
          break;
        case "set_account_nickname":
          if (requireAuth(client)) {
            client.account.nickname = sanitizeNickname(msg.name);
            client.name = client.account.nickname;
            await saveClientAccount(client);
          }
          break;
        case "find_match":
          enqueueMatch(client, msg);
          break;
        case "cancel_search":
          cancelSearch(client);
          break;
        case "create_game":
          createPrivateGame(client, msg);
          break;
        case "shoot":
          handleShoot(client, msg);
          break;
        case "buy_upgrade":
          if (requireAuth(client)) {
            const id = String(msg.id || "");
            const u = UPGRADES[id];
            const level = Number(client.account.upgrades[id] || 0);
            if (!u || level >= u.max) {
              send(client.ws, { type: "buy_result", ok: false, reason: "invalid" });
              break;
            }
            const cost = u.costs[level];
            if (client.account.coins < cost) {
              send(client.ws, { type: "buy_result", ok: false, reason: "insufficient_funds", cost });
              break;
            }
            client.account.coins -= cost;
            client.account.upgrades[id] = level + 1;
            await saveAccount(client.account);
            send(client.ws, { type: "buy_result", ok: true, account: publicAccount(client.account) });
          }
          break;
        case "buy_skin":
          if (requireAuth(client)) {
            const id = String(msg.id || "");
            const skin = SKINS[id];
            if (!skin || client.account.skins.includes(id)) {
              send(client.ws, { type: "buy_result", ok: false, reason: "invalid" });
              break;
            }
            if (client.account.coins < skin.cost) {
              send(client.ws, { type: "buy_result", ok: false, reason: "insufficient_funds", cost: skin.cost });
              break;
            }
            client.account.coins -= skin.cost;
            client.account.skins.push(id);
            client.account.skin = id;
            await saveAccount(client.account);
            send(client.ws, { type: "buy_result", ok: true, account: publicAccount(client.account) });
          }
          break;
        case "equip_skin":
          if (requireAuth(client)) {
            const id = String(msg.id || "");
            if (!client.account.skins.includes(id)) {
              send(client.ws, { type: "buy_result", ok: false, reason: "invalid" });
              break;
            }
            client.account.skin = id;
            await saveAccount(client.account);
            send(client.ws, { type: "buy_result", ok: true, account: publicAccount(client.account) });
          }
          break;
        case "add_playtime":
          if (requireAuth(client)) {
            const seconds = Math.max(0, Math.min(3600, Math.round(Number(msg.seconds) || 0)));
            if (seconds) {
              client.account.playSeconds += seconds;
              await saveAccount(client.account);
            }
          }
          break;
        case "leave_room":
          leaveRoom(client);
          break;
        case "ping":
          send(ws, { type: "pong", time: Date.now() });
          break;
        default:
          break;
      }
    } catch (err) {
      console.error(`[MSG ${client.id}]`, err);
    }
  });

  ws.on("close", () => {
    cancelSearch(client);
    leaveRoom(client);
    usedNames.delete(client.name);
    clients.delete(client.id);
  });

  ws.on("error", (err) => console.error(`[WS ERROR ${client.id}]`, err.message));
});

async function leaveRoom(client) {
  cancelSearch(client);
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  if (!room) {
    client.roomId = null;
    client.ownerIndex = null;
    return;
  }

  // 이미 종료된 방이면 연결만 정리합니다.
  if (room.finished) {
    client.roomId = null;
    client.ownerIndex = null;
    return;
  }

  // 온라인: 나간 플레이어를 패배 처리합니다. 나머지 한 명이 승리합니다.
  if (room.mode === "online") {
    const leavingIndex = room.clientList.findIndex((c) => c.id === client.id);
    const winnerIndex = room.engine.alivePlayers().find((i) => i !== leavingIndex);
    if (winnerIndex != null) {
      room.engine.winner = winnerIndex;
      room.engine.draw = false;
    } else {
      room.engine.winner = null;
      room.engine.draw = true;
    }
    await finishRoom(room);
    return;
  }

  // AI 전투 중 이탈은 플레이어 패배입니다. 연습은 기록 없이 종료합니다.
  if (room.mode === "ai") {
    room.engine.winner = room.playerCount > 1 ? 1 : null;
    room.engine.draw = room.engine.winner == null;
    await finishRoom(room);
    return;
  }

  room.finished = true;
  rooms.delete(room.id);
  client.roomId = null;
  client.ownerIndex = null;
}

setInterval(() => {
  for (const ws of wss.clients) {
    try {
      ws.ping();
    } catch {}
  }
}, 25000);

setInterval(() => {
  tickRooms().catch((err) => console.error("[TICK]", err));
}, WS_TICK_MS);

(async () => {
  try {
    await initDb();
  } catch (err) {
    console.error("[DB INIT]", err);
    process.exitCode = 1;
    return;
  }
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`Alkkagi authoritative server listening on ${PORT}`);
  });
})();
