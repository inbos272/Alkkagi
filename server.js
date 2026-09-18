const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// =========================
// HTTP 서버
// =========================

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8",
  });

  res.end("Alkkagi WebSocket Server is running!");
});

// =========================
// WebSocket 서버
// =========================

const wss = new WebSocketServer({ server });

// =========================
// 데이터
// =========================

let nextClientId = 1;

const clients = new Map();
const queues = new Map();
const rooms = new Map();

// 사용 중인 닉네임
const usedNames = new Map();

// =========================
// 닉네임
// =========================

const NAME_POOL = [
  "알까기왕",
  "돌격알",
  "알까기초보",
  "검은돌",
  "흰돌",
  "돌멩이",
  "알까기마스터",
  "한방알",
  "돌돌이",
  "알신",
  "스톤맨",
  "알까기고수",
  "쓱쓱이",
  "통통알",
  "돌격대",
  "알까기장인",
  "스톤킹",
  "알파돌",
  "빨간알",
  "파란알",
  "초록알",
  "보라알",
  "황금알",
  "무적알",
  "행운의알",
  "알폭탄",
  "돌의신",
  "슈퍼알",
  "알까기전사",
  "스톤히어로",
];

function randomName() {
  const available = NAME_POOL.filter(
    (name) => !usedNames.has(name)
  );

  if (available.length > 0) {
    return available[
      Math.floor(Math.random() * available.length)
    ];
  }

  // 기본 이름 풀이 모두 사용된 경우
  let i = 1;

  while (usedNames.has(`손님${i}`)) {
    i++;
  }

  return `손님${i}`;
}

function setClientName(client, name) {
  if (typeof name !== "string") {
    return false;
  }

  name = name.trim();

  if (name.length < 1 || name.length > 12) {
    return false;
  }

  // 허용 문자
  if (!/^[가-힣a-zA-Z0-9 _-]+$/.test(name)) {
    return false;
  }

  const oldName = client.name;

  // 같은 이름이면 변경할 필요 없음
  if (oldName === name) {
    return true;
  }

  // 다른 사람이 사용 중
  if (usedNames.has(name)) {
    send(client.ws, {
      type: "name_taken",
      name,
    });

    return false;
  }

  // 기존 이름 제거
  if (oldName) {
    usedNames.delete(oldName);
  }

  client.name = name;
  usedNames.set(name, client.id);

  send(client.ws, {
    type: "name_changed",
    name,
  });

  // 방 안의 다른 사람들에게 알림
  if (client.roomId) {
    broadcastRoom(client.roomId, {
      type: "player_name_changed",
      playerId: client.id,
      name,
    });
  }

  return true;
}

// =========================
// 유틸
// =========================

function send(ws, data) {
  if (!ws || ws.readyState !== ws.OPEN) {
    return;
  }

  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    console.error("send error:", err);
  }
}

function broadcastRoom(roomId, data, exceptId = null) {
  const room = rooms.get(roomId);

  if (!room) {
    return;
  }

  for (const playerId of room.players) {
    if (playerId === exceptId) {
      continue;
    }

    const client = clients.get(playerId);

    if (client) {
      send(client.ws, data);
    }
  }
}

function makeRoomId() {
  return (
    Date.now().toString(36) +
    Math.random().toString(36).slice(2, 8)
  );
}

// =========================
// 연결
// =========================

wss.on("connection", (ws) => {
  const clientId = String(nextClientId++);

  const client = {
    id: clientId,
    ws,
    name: randomName(),
    roomId: null,
    isHost: false,
  };

  clients.set(clientId, client);
  usedNames.set(client.name, client.id);

  console.log(
    `[CONNECT] ${client.id} / ${client.name}`
  );

  send(ws, {
    type: "welcome",
    clientId: client.id,
    name: client.name,
  });

  // =========================
  // 메시지
  // =========================

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      console.log("Invalid JSON:", raw.toString());
      return;
    }

    if (!msg || typeof msg.type !== "string") {
      return;
    }

    // =========================
    // 닉네임 변경
    // =========================

    if (msg.type === "set_name") {
      setClientName(client, msg.name);
      return;
    }

    // =========================
    // 매칭 시작
    // =========================

    if (msg.type === "find_match") {
      findMatch(client, msg);
      return;
    }

    // =========================
    // 매칭 취소
    // =========================

    if (msg.type === "cancel_search") {
      cancelSearch(client);
      return;
    }

    // =========================
    // 발사
    // =========================

    if (msg.type === "shoot") {
      handleShoot(client, msg);
      return;
    }

    // =========================
    // 게임 상태
    // =========================

    if (msg.type === "state") {
      handleState(client, msg);
      return;
    }

    // =========================
    // 게임 종료
    // =========================

    if (msg.type === "game_over") {
      handleGameOver(client, msg);
      return;
    }

    // =========================
    // 채팅
    // =========================

    if (msg.type === "chat") {
      handleChat(client, msg);
      return;
    }

    // =========================
    // 방 나가기
    // =========================

    if (msg.type === "leave_room") {
      leaveRoom(client);
      return;
    }

    // =========================
    // 핑
    // =========================

    if (msg.type === "ping") {
      send(ws, {
        type: "pong",
        time: Date.now(),
      });

      return;
    }
  });

  // =========================
  // 연결 종료
  // =========================

  ws.on("close", () => {
    console.log(
      `[DISCONNECT] ${client.id} / ${client.name}`
    );

    cancelSearch(client);
    leaveRoom(client);

    usedNames.delete(client.name);
    clients.delete(client.id);
  });

  ws.on("error", (err) => {
    console.error(
      `[WS ERROR] ${client.id}`,
      err.message
    );
  });
});

// =========================
// 매칭
// =========================

function findMatch(client, msg) {
  if (client.roomId) {
    return;
  }

  const playerCount = Math.max(
    2,
    Math.min(8, Number(msg.playerCount) || 2)
  );

  const map = String(msg.map || "classic");

  const stoneCount = Math.max(
    1,
    Math.min(4, Number(msg.stoneCount) || 1)
  );

  // 플레이어 수 + 맵 + 알 개수를 모두 매칭 조건으로 사용
  const queueKey =
    `${playerCount}:${map}:${stoneCount}`;

  client.searchKey = queueKey;

  if (!queues.has(queueKey)) {
    queues.set(queueKey, []);
  }

  const queue = queues.get(queueKey);

  // 중복 등록 방지
  if (!queue.includes(client.id)) {
    queue.push(client.id);
  }

  console.log(
    `[MATCH] ${client.name} joined queue ${queueKey}`
  );

  send(client.ws, {
    type: "searching",
    playerCount,
    map,
    stoneCount,
    queueSize: queue.length,
  });

  // 충분한 인원이 모이면 방 생성
  while (queue.length >= playerCount) {
    const playerIds = queue.splice(0, playerCount);

    const validPlayers = playerIds.filter(
      (id) => clients.has(id)
    );

    if (validPlayers.length < playerCount) {
      continue;
    }

    createRoom(
      validPlayers,
      playerCount,
      map,
      stoneCount
    );
  }

  if (queue.length === 0) {
    queues.delete(queueKey);
  }
}

// =========================
// 매칭 취소
// =========================

function cancelSearch(client) {
  const key = client.searchKey;

  if (!key) {
    return;
  }

  const queue = queues.get(key);

  if (queue) {
    const index = queue.indexOf(client.id);

    if (index !== -1) {
      queue.splice(index, 1);
    }

    if (queue.length === 0) {
      queues.delete(key);
    }
  }

  client.searchKey = null;

  send(client.ws, {
    type: "search_cancelled",
  });
}

// =========================
// 방 생성
// =========================

function createRoom(
  playerIds,
  playerCount,
  map,
  stoneCount
) {
  const roomId = makeRoomId();

  const room = {
    id: roomId,
    map,
    playerCount,
    stoneCount,

    players: playerIds,

    hostId: playerIds[0],

    alive: playerIds.map(() => true),

    gameStarted: false,
    gameOver: false,
  };

  rooms.set(roomId, room);

  console.log(
    `[ROOM CREATE] ${roomId}`,
    room.players
  );

  // 플레이어 정보
  const playerList = room.players.map(
    (id, index) => {
      const p = clients.get(id);

      return {
        id,
        index,
        name: p ? p.name : `Player ${index + 1}`,
      };
    }
  );

  for (let i = 0; i < room.players.length; i++) {
    const playerId = room.players[i];
    const player = clients.get(playerId);

    if (!player) {
      continue;
    }

    player.roomId = roomId;
    player.isHost = playerId === room.hostId;
    player.searchKey = null;

    send(player.ws, {
      type: "match_found",

      roomId,

      map,
      playerCount,
      stoneCount,

      playerIndex: i,

      hostId: room.hostId,

      players: playerList,
    });
  }
}

// =========================
// 발사 처리
// =========================

function handleShoot(client, msg) {
  if (!client.roomId) {
    return;
  }

  const room = rooms.get(client.roomId);

  if (!room) {
    return;
  }

  if (room.gameOver) {
    return;
  }

  // 서버는 물리 계산을 하지 않고
  // 현재 방의 호스트에게 발사 명령을 전달한다.
  if (client.id !== room.hostId) {
    broadcastRoom(
      room.id,
      {
        type: "shoot_request",
        playerId: client.id,
        stoneId: msg.stoneId,
        dx: msg.dx,
        dy: msg.dy,
      },
      client.id
    );

    return;
  }

  broadcastRoom(room.id, {
    type: "shoot",
    playerId: client.id,
    stoneId: msg.stoneId,
    dx: msg.dx,
    dy: msg.dy,
  });
}

// =========================
// 상태 동기화
// =========================

function handleState(client, msg) {
  if (!client.roomId) {
    return;
  }

  const room = rooms.get(client.roomId);

  if (!room) {
    return;
  }

  // 호스트만 전체 상태를 전송할 수 있음
  if (client.id !== room.hostId) {
    return;
  }

  broadcastRoom(
    room.id,
    {
      type: "state",
      state: msg.state,
    },
    client.id
  );
}

// =========================
// 게임 종료
// =========================

function handleGameOver(client, msg) {
  if (!client.roomId) {
    return;
  }

  const room = rooms.get(client.roomId);

  if (!room) {
    return;
  }

  // 호스트가 게임 종료를 판단
  if (client.id !== room.hostId) {
    return;
  }

  if (room.gameOver) {
    return;
  }

  room.gameOver = true;

  const winner =
    typeof msg.winner === "number"
      ? msg.winner
      : null;

  console.log(
    `[GAME OVER] room=${room.id}, winner=${winner}`
  );

  broadcastRoom(room.id, {
    type: "game_over",
    winner,
  });
}

// =========================
// 채팅
// =========================

function handleChat(client, msg) {
  if (!client.roomId) {
    return;
  }

  const room = rooms.get(client.roomId);

  if (!room) {
    return;
  }

  let text = String(msg.text || "").trim();

  if (!text) {
    return;
  }

  // 너무 긴 메시지 방지
  if (text.length > 200) {
    text = text.slice(0, 200);
  }

  broadcastRoom(room.id, {
    type: "chat",
    playerId: client.id,
    name: client.name,
    text,
    time: Date.now(),
  });
}

// =========================
// 방 나가기
// =========================

function leaveRoom(client) {
  if (!client.roomId) {
    return;
  }

  const roomId = client.roomId;
  const room = rooms.get(roomId);

  client.roomId = null;
  client.isHost = false;

  if (!room) {
    return;
  }

  const index = room.players.indexOf(client.id);

  if (index !== -1) {
    room.players.splice(index, 1);
  }

  room.alive.splice(index, 1);

  console.log(
    `[LEAVE] ${client.name} left room ${roomId}`
  );

  // 방에 남은 사람에게 알림
  broadcastRoom(roomId, {
    type: "player_left",
    playerId: client.id,
    name: client.name,
  });

  // 방에 사람이 없으면 삭제
  if (room.players.length === 0) {
    rooms.delete(roomId);

    console.log(
      `[ROOM DELETE] ${roomId}`
    );

    return;
  }

  // =========================
  // 호스트 변경
  // =========================

  if (room.hostId === client.id) {
    room.hostId = room.players[0];

    const newHost = clients.get(room.hostId);

    if (newHost) {
      newHost.isHost = true;
    }

    broadcastRoom(roomId, {
      type: "host_changed",
      hostId: room.hostId,
    });

    console.log(
      `[HOST CHANGE] room=${roomId}, host=${room.hostId}`
    );
  }
}

// =========================
// 서버 시작
// =========================

server.listen(PORT, () => {
  console.log(
    `Alkkagi server listening on port ${PORT}`
  );
});
