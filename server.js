// ============================================================
// 알까기 온라인 서버
// - package.json 의존성: "ws" 하나만 사용 (가볍게, Render 무료 플랜에 적합)
// - 서버는 "방장(host) 클라이언트가 물리 연산을 하고, 서버는 방을 만들고
//   메시지를 방 안의 다른 사람들에게 그대로 전달(relay)"하는 구조입니다.
//   → 서버가 물리 엔진을 돌리지 않아 CPU 부담이 거의 없고, 클라이언트 로직을
//     그대로 재사용할 수 있어 안정적입니다.
// ============================================================

const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

// 헬스체크용 HTTP 서버 (Render는 HTTP 응답이 있어야 "살아있다"고 판단합니다)
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Alkkagi WebSocket server is running.\n');
});

const wss = new WebSocketServer({ server: httpServer });

// --------------------------------------------------------------
// 상태
// --------------------------------------------------------------
let nextClientId = 1;
let nextRoomId = 1;

/** clientId -> { ws, name, roomId } */
const clients = new Map();

/** queueKey(`${playerCount}:${map}`) -> [clientId, ...] 대기열 */
const queues = new Map();

/** roomId -> { id, map, playerCount, stoneCount, players:[clientId,...], hostId, alive:Set } */
const rooms = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcastRoom(room, obj, exceptId = null) {
  for (const cid of room.players) {
    if (cid === exceptId) continue;
    const c = clients.get(cid);
    if (c) send(c.ws, obj);
  }
}

function queueKey(playerCount, map) {
  return `${playerCount}:${map}`;
}

function removeFromAllQueues(clientId) {
  for (const [key, list] of queues) {
    const idx = list.indexOf(clientId);
    if (idx !== -1) list.splice(idx, 1);
    if (list.length === 0) queues.delete(key);
  }
}

function leaveRoom(clientId) {
  const c = clients.get(clientId);
  if (!c || !c.roomId) return;
  const room = rooms.get(c.roomId);
  c.roomId = null;
  if (!room) return;

  room.players = room.players.filter((id) => id !== clientId);
  room.alive.delete(clientId);

  if (room.players.length === 0) {
    rooms.delete(room.id);
    return;
  }

  // 방장이 나갔다면 다음 사람에게 방장을 위임합니다.
  const hostLeft = room.hostId === clientId;
  if (hostLeft) {
    room.hostId = room.players[0];
  }

  broadcastRoom(room, {
    type: 'player_left',
    clientId,
    newHostId: room.hostId,
  });
}

// --------------------------------------------------------------
// 연결 처리
// --------------------------------------------------------------
wss.on('connection', (ws) => {
  const clientId = nextClientId++;
  clients.set(clientId, { ws, name: `손님${clientId}`, roomId: null });

  send(ws, { type: 'welcome', clientId });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // 잘못된 메시지는 조용히 무시 (서버가 죽지 않도록)
    }

    const c = clients.get(clientId);
    if (!c) return;

    switch (msg.type) {
      // ---- 매치메이킹 ----
      case 'find_match': {
        const playerCount = Math.min(4, Math.max(2, Number(msg.playerCount) || 2));
        const map = String(msg.map || 'classic');
        const stoneCount = Math.min(4, Math.max(1, Number(msg.stoneCount) || 2));
        if (typeof msg.name === 'string' && msg.name.trim()) {
          c.name = msg.name.trim().slice(0, 16);
        }

        const key = queueKey(playerCount, map);
        if (!queues.has(key)) queues.set(key, []);
        const list = queues.get(key);
        if (!list.includes(clientId)) list.push(clientId);

        send(ws, { type: 'searching', playerCount, map });

        if (list.length >= playerCount) {
          const memberIds = list.splice(0, playerCount);
          const roomId = nextRoomId++;
          const room = {
            id: roomId,
            map,
            playerCount,
            stoneCount,
            players: memberIds,
            hostId: memberIds[0],
            alive: new Set(memberIds),
          };
          rooms.set(roomId, room);
          if (list.length === 0) queues.delete(key);

          memberIds.forEach((id) => {
            const member = clients.get(id);
            if (member) member.roomId = roomId;
          });

          const rosterFor = () =>
            memberIds.map((id, idx) => ({
              clientId: id,
              index: idx,
              name: clients.get(id)?.name || `손님${id}`,
            }));

          memberIds.forEach((id) => {
            const member = clients.get(id);
            if (!member) return;
            send(member.ws, {
              type: 'match_found',
              roomId,
              map,
              stoneCount,
              you: id,
              hostId: room.hostId,
              isHost: room.hostId === id,
              players: rosterFor(),
            });
          });
        }
        break;
      }

      case 'cancel_search': {
        removeFromAllQueues(clientId);
        send(ws, { type: 'search_cancelled' });
        break;
      }

      // ---- 게임 중 릴레이 ----
      // 참가자가 자기 턴에 스톤을 튕기면(shoot) 서버는 그대로 방장에게 전달합니다.
      case 'shoot': {
        if (!c.roomId) return;
        const room = rooms.get(c.roomId);
        if (!room) return;
        const host = clients.get(room.hostId);
        if (host) {
          send(host.ws, { type: 'shoot', from: clientId, ...msg });
        }
        break;
      }

      // 방장이 물리 연산 결과(스톤 위치/턴/생존자)를 브로드캐스트합니다.
      case 'state': {
        if (!c.roomId) return;
        const room = rooms.get(c.roomId);
        if (!room || room.hostId !== clientId) return; // 방장만 상태를 보낼 수 있음
        broadcastRoom(room, { type: 'state', ...msg }, clientId);
        break;
      }

      // 게임 종료 알림(방장이 승자를 판정해 브로드캐스트)
      case 'game_over': {
        if (!c.roomId) return;
        const room = rooms.get(c.roomId);
        if (!room || room.hostId !== clientId) return;
        broadcastRoom(room, { type: 'game_over', ...msg }, clientId);
        break;
      }

      // 채팅/이모트 등 부가 기능 (있으면 그대로 중계)
      case 'chat': {
        if (!c.roomId) return;
        const room = rooms.get(c.roomId);
        if (!room) return;
        broadcastRoom(room, { type: 'chat', from: clientId, name: c.name, text: String(msg.text || '').slice(0, 200) });
        break;
      }

      case 'leave_room': {
        leaveRoom(clientId);
        break;
      }

      case 'ping': {
        send(ws, { type: 'pong', t: msg.t });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    removeFromAllQueues(clientId);
    leaveRoom(clientId);
    clients.delete(clientId);
  });

  ws.on('error', () => {
    // 연결 오류는 close 이벤트로 이어지므로 별도 처리 불필요
  });
});

httpServer.listen(PORT, () => {
  console.log(`Alkkagi WS server listening on :${PORT}`);
});
