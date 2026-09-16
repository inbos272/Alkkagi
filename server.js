const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8787);
const ROOT = __dirname;
const INDEX = path.join(ROOT, '알까기_개선버전_v9.html');

const queues = new Map([
  [2, []],
  [3, []],
  [4, []]
]);

const matches = new Map();
let nextMatchId = 1;

function send(ws, msg) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(msg));
  }
}

function removeFromQueue(ws) {
  for (const q of queues.values()) {
    const idx = q.indexOf(ws);
    if (idx >= 0) {
      q.splice(idx, 1);
    }
  }

  ws.queueSize = null;
}

function closeMatchForPlayer(ws, reason = 'A player left the match.') {
  const matchId = ws.matchId;

  if (!matchId) return;

  const match = matches.get(matchId);

  if (!match) return;

  for (const p of match.players) {
    if (p.ws !== ws) {
      send(p.ws, {
        type: 'matchClosed',
        reason
      });
    }

    p.ws.matchId = null;
  }

  matches.delete(matchId);
  ws.matchId = null;
}

function tryMatch(size) {
  const q = queues.get(size);

  while (q && q.length >= size) {
    const players = q.splice(0, size);

    const matchId =
      `m${Date.now().toString(36)}-${nextMatchId++}`;

    const map =
      players[0].queueMap || 'classic';

    const match = {
      id: matchId,
      size,
      map,
      players: players.map((ws, i) => ({
        ws,
        nickname: ws.nickname || `Player ${i + 1}`,
        index: i
      }))
    };

    matches.set(matchId, match);

    match.players.forEach((p, i) => {
      p.ws.matchId = matchId;
      p.ws.queueSize = null;

      send(p.ws, {
        type: 'matchFound',
        matchId,
        isHost: i === 0,
        playerIndex: i,
        playerCount: size,
        map,
        players: match.players.map(x => ({
          index: x.index,
          nickname: x.nickname
        }))
      });
    });
  }
}

const server = http.createServer((req, res) => {
  const url = new URL(
    req.url,
    `http://${req.headers.host || 'localhost'}`
  );

  if (url.pathname === '/health') {
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8'
    });

    res.end(
      JSON.stringify({
        ok: true,
        queues: Object.fromEntries(
          [...queues].map(([k, v]) => [k, v.length])
        )
      })
    );

    return;
  }

  let filePath =
    url.pathname === '/'
      ? INDEX
      : path.join(
          ROOT,
          decodeURIComponent(
            url.pathname.replace(/^\//, '')
          )
        );

  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  if (
    !fs.existsSync(filePath) ||
    !fs.statSync(filePath).isFile()
  ) {
    res.writeHead(404);
    res.end('Not found');
    return;
  }

  const ext = path.extname(filePath).toLowerCase();

  const type =
    ext === '.html'
      ? 'text/html; charset=utf-8'
      : ext === '.js'
      ? 'text/javascript; charset=utf-8'
      : 'application/octet-stream';

  res.writeHead(200, {
    'Content-Type': type,
    'Cache-Control': 'no-cache'
  });

  fs.createReadStream(filePath).pipe(res);
});

const wss = new WebSocketServer({
  noServer: true
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(
    req.url,
    `http://${req.headers.host || 'localhost'}`
  );

  if (url.pathname !== '/ws') {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(
    req,
    socket,
    head,
    ws => wss.emit('connection', ws, req)
  );
});

wss.on('connection', ws => {
  ws.queueSize = null;
  ws.matchId = null;
  ws.nickname = 'Player';

  ws.on('message', raw => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // -----------------------------
    // Join matchmaking queue
    // -----------------------------
    if (msg.type === 'queueJoin') {
      const size = Math.max(
        2,
        Math.min(4, Number(msg.size) || 2)
      );

      removeFromQueue(ws);

      ws.nickname =
        String(msg.nickname || 'Player').slice(0, 16);

      ws.queueMap = [
        'classic',
        'ice',
        'bumper',
        'tiny'
      ].includes(msg.map)
        ? msg.map
        : 'classic';

      ws.queueSize = size;

      queues.get(size).push(ws);

      send(ws, {
        type: 'queueJoined',
        size
      });

      tryMatch(size);

      return;
    }

    // -----------------------------
    // Leave matchmaking queue
    // -----------------------------
    if (msg.type === 'queueLeave') {
      removeFromQueue(ws);
      return;
    }

    // -----------------------------
    // Online game messages
    // -----------------------------
    if (
      msg.type === 'shot' ||
      msg.type === 'state'
    ) {
      if (!ws.matchId) {
        return;
      }

      const match = matches.get(ws.matchId);

      if (!match) {
        return;
      }

      // Remote player sends shot information.
      if (msg.type === 'shot') {
        for (const p of match.players) {
          if (p.ws !== ws) {
            send(p.ws, {
              ...msg,
              matchId: ws.matchId
            });
          }
        }
      }

      // Only the host may broadcast game state.
      else if (
        msg.type === 'state' &&
        match.players[0].ws === ws
      ) {
        for (const p of match.players) {
          if (p.ws !== ws) {
            send(p.ws, msg);
          }
        }
      }

      return;
    }

    // -----------------------------
    // Leave active match
    // -----------------------------
    if (msg.type === 'leaveMatch') {
      closeMatchForPlayer(
        ws,
        'A player left the match.'
      );

      return;
    }
  });

  ws.on('close', () => {
    removeFromQueue(ws);

    if (ws.matchId) {
      closeMatchForPlayer(
        ws,
        'A player disconnected.'
      );
    }
  });
});

server.listen(PORT, () => {
  console.log(
    `알까기 server listening on http://localhost:${PORT}`
  );

  console.log(
    `Online WebSocket endpoint: ws://localhost:${PORT}/ws`
  );
});
