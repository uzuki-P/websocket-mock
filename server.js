const express = require('express');
const http = require('http');
const { WebSocketServer: WSServer } = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const PORT = process.env.DASHBOARD_PORT || process.env.PORT || 3000;
const WS_DEFAULT_PORT = process.env.WS_DEFAULT_PORT || 8080;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let wsServer = null;
let clients = new Map();
let messageLog = [];

function addLog(type, data, clientId) {
  const entry = {
    id: crypto.randomUUID(),
    timestamp: new Date().toISOString(),
    type,
    data: typeof data === 'string' ? data : JSON.stringify(data),
    clientId: clientId || null,
  };
  messageLog.push(entry);
  if (messageLog.length > 500) messageLog = messageLog.slice(-500);
  return entry;
}

function broadcastToSSE(event, data) {
  for (const res of sseClients.values()) {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
}

const sseClients = new Map();

app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write('\n');
  const id = crypto.randomUUID();
  sseClients.set(id, res);
  req.on('close', () => sseClients.delete(id));
});

app.get('/api/server/status', (req, res) => {
  res.json({
    running: wsServer !== null,
    port: wsServer ? wsServer.options.port : null,
    path: wsServer ? wsServer.options.path : null,
    clientCount: clients.size,
  });
});

app.post('/api/server/start', (req, res) => {
  if (wsServer) {
    return res.status(400).json({ error: 'Server already running' });
  }
  const port = req.body.port || WS_DEFAULT_PORT;
  if (port < 1 || port > 65535) {
    return res.status(400).json({ error: 'Invalid port number' });
  }

  let path = req.body.path || '/';
  if (!path.startsWith('/')) path = '/' + path;

  try {
    wsServer = new WSServer({ port, path });
    wsServer.options.port = port;
    wsServer.options.path = path;

    wsServer.on('connection', (ws, req) => {
      const clientId = crypto.randomUUID();
      const clientInfo = {
        id: clientId,
        ip: req.socket.remoteAddress,
        connectedAt: new Date().toISOString(),
        ws,
      };
      clients.set(clientId, clientInfo);

      const logEntry = addLog('connect', `Client connected from ${clientInfo.ip}`, clientId);
      broadcastToSSE('client_connected', { client: { id: clientId, ip: clientInfo.ip, connectedAt: clientInfo.connectedAt }, log: logEntry });
      broadcastToSSE('status', { running: true, port, path, clientCount: clients.size });

      ws.on('message', (raw) => {
        const text = raw.toString();
        const logEntry = addLog('received', text, clientId);
        broadcastToSSE('message', { log: logEntry, fromClient: clientId });
      });

      ws.on('close', () => {
        clients.delete(clientId);
        const logEntry = addLog('disconnect', `Client disconnected`, clientId);
        broadcastToSSE('client_disconnected', { clientId, log: logEntry });
        broadcastToSSE('status', { running: true, port, path, clientCount: clients.size });
      });

      ws.on('error', () => {
        clients.delete(clientId);
      });
    });

    wsServer.on('error', (err) => {
      wsServer = null;
      res.status(500).json({ error: `Failed to start: ${err.message}` });
    });

    wsServer.on('listening', () => {
      broadcastToSSE('status', { running: true, port, path, clientCount: 0 });
      res.json({ success: true, port, path });
    });
  } catch (err) {
    wsServer = null;
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/server/stop', (req, res) => {
  if (!wsServer) {
    return res.status(400).json({ error: 'Server not running' });
  }
  for (const client of clients.values()) {
    client.ws.close();
  }
  clients.clear();
  wsServer.close();
  wsServer = null;
  broadcastToSSE('status', { running: false, port: null, clientCount: 0 });
  res.json({ success: true });
});

app.get('/api/clients', (req, res) => {
  const list = [];
  for (const c of clients.values()) {
    list.push({ id: c.id, ip: c.ip, connectedAt: c.connectedAt });
  }
  res.json(list);
});

app.post('/api/broadcast', (req, res) => {
  if (!wsServer) return res.status(400).json({ error: 'Server not running' });
  const { data } = req.body;
  if (data === undefined) return res.status(400).json({ error: 'Data is required' });

  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  let sentCount = 0;
  for (const client of clients.values()) {
    if (client.ws.readyState === 1) {
      client.ws.send(payload);
      sentCount++;
    }
  }
  const logEntry = addLog('sent', payload, null);
  broadcastToSSE('message', { log: logEntry, broadcast: true });
  res.json({ success: true, sentTo: sentCount });
});

app.post('/api/send/:clientId', (req, res) => {
  if (!wsServer) return res.status(400).json({ error: 'Server not running' });
  const { clientId } = req.params;
  const client = clients.get(clientId);
  if (!client || client.ws.readyState !== 1) {
    return res.status(404).json({ error: 'Client not found or disconnected' });
  }
  const { data } = req.body;
  if (data === undefined) return res.status(400).json({ error: 'Data is required' });

  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  client.ws.send(payload);
  const logEntry = addLog('sent', payload, clientId);
  broadcastToSSE('message', { log: logEntry, targetClient: clientId });
  res.json({ success: true });
});

app.get('/api/messages', (req, res) => {
  res.json(messageLog);
});

app.delete('/api/messages', (req, res) => {
  messageLog = [];
  broadcastToSSE('messages_cleared', {});
  res.json({ success: true });
});

server.listen(PORT, () => {
  console.log(`  WebSocket Server Manager`);
  console.log(`  Dashboard: http://localhost:${PORT}`);
  if (WS_DEFAULT_PORT !== 8080) {
    console.log(`  Default WS port: ${WS_DEFAULT_PORT}`);
  }
  console.log();
});
