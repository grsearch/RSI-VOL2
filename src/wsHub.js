'use strict';
// src/wsHub.js — WebSocket 广播中心

const WebSocket = require('ws');
let _wss = null;

function init(server) {
  _wss = new WebSocket.Server({ server });
  _wss.on('connection', ws => {
    ws.on('error', () => {});
  });
}

function broadcast(data) {
  if (!_wss) return;
  const msg = JSON.stringify(data);
  _wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(msg); } catch (_) {}
    }
  });
}

module.exports = { init, broadcast };
