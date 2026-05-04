// SyncSound — Server per Glitch.com
// Copia TUTTO questo contenuto nel file server.js su Glitch

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const rooms = new Map();

function genCode() {
  const c = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code;
  do { code = Array.from({length:6},()=>c[Math.floor(Math.random()*c.length)]).join(""); } while(rooms.has(code));
  return code;
}
function genId() { return crypto.randomBytes(4).toString("hex"); }

function broadcastToRoom(room, msg, excludeId) {
  const data = JSON.stringify(msg);
  for (const [id, peer] of room.peers) {
    if (id !== excludeId && peer.ws.readyState === 1) peer.ws.send(data);
  }
}

function getRoomInfo(room) {
  const members = [];
  for (const [id, p] of room.peers) {
    members.push({ id, name:p.name, isHost:p.isHost, isRelay:p.isRelay, bufferReady:p.bufferReady, latency:p.latency });
  }
  return { code:room.code, members, audioReady:room.audioReady, playback:room.playback };
}

// ─── HTTP ───
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type,X-Filename");
  if (req.method==="OPTIONS") { res.writeHead(200); res.end(); return; }

  // Download audio
  const audioMatch = req.url.match(/^\/audio\/([A-Z0-9]{6})$/);
  if (audioMatch) {
    const room = rooms.get(audioMatch[1]);
    if (!room||!room.audioBuffer) { res.writeHead(404); res.end(); return; }
    const ext = path.extname(room.audioFilename).toLowerCase();
    const mime = {".mp3":"audio/mpeg",".wav":"audio/wav",".ogg":"audio/ogg",".m4a":"audio/mp4",".aac":"audio/aac"};
    res.writeHead(200,{"Content-Type":mime[ext]||"audio/mpeg","Content-Length":room.audioBuffer.length});
    res.end(room.audioBuffer);
    return;
  }

  // Upload audio
  const upMatch = req.url.match(/^\/upload\/([A-Z0-9]{6})$/);
  if (upMatch && req.method==="POST") {
    const room = rooms.get(upMatch[1]);
    if (!room) { res.writeHead(404); res.end(); return; }
    const chunks = [];
    req.on("data",c=>chunks.push(c));
    req.on("end",()=>{
      room.audioBuffer = Buffer.concat(chunks);
      room.audioFilename = req.headers["x-filename"]||"track.mp3";
      room.audioReady = true;
      console.log(`[${room.code}] Audio: ${room.audioFilename} (${(room.audioBuffer.length/1048576).toFixed(1)}MB)`);
      broadcastToRoom(room,{type:"audio_ready",filename:room.audioFilename,size:room.audioBuffer.length});
      res.writeHead(200,{"Content-Type":"application/json"});
      res.end(JSON.stringify({ok:true}));
    });
    return;
  }

  // Serve index.html (Glitch: public folder or inline)
  if (req.url === "/" || req.url === "/index.html") {
    const filePath = path.join(__dirname, "public", "index.html");
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end("index.html not found"); return; }
      res.writeHead(200, {"Content-Type":"text/html; charset=utf-8"});
      res.end(data);
    });
    return;
  }

  // Other static
  const filePath = path.join(__dirname, "public", req.url);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end(); return; }
    const ext = path.extname(filePath);
    const m = {".js":"text/javascript",".css":"text/css",".json":"application/json",".png":"image/png"};
    res.writeHead(200, {"Content-Type": m[ext]||"application/octet-stream"});
    res.end(data);
  });
});

// ─── WebSocket ───
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  const peerId = genId();
  let currentRoom = null;
  function send(msg) { if(ws.readyState===1) ws.send(JSON.stringify(msg)); }

  ws.on("message", (raw) => {
    let msg;
    try { msg=JSON.parse(raw); } catch { return; }

    switch(msg.type) {

      case "create_room": {
        const code = genCode();
        const room = {
          code, hostId:peerId, peers:new Map(),
          audioBuffer:null, audioReady:false, audioFilename:"",
          playback:{playing:false,startedAt:null,pausedAt:0,trackDuration:0},
          createdAt:Date.now()
        };
        room.peers.set(peerId,{ws,name:msg.name||"Host",isHost:true,isRelay:false,bufferReady:true,latency:0,clockOffset:0});
        rooms.set(code,room);
        currentRoom=code;
        send({type:"room_created",peerId,room:getRoomInfo(room)});
        console.log(`[${code}] Created by "${msg.name}"`);
        break;
      }

      case "join_room": {
        const code=(msg.code||"").toUpperCase().trim();
        const room=rooms.get(code);
        if(!room){send({type:"error",message:"Stanza non trovata"});return;}
        room.peers.set(peerId,{ws,name:msg.name||"Guest",isHost:false,isRelay:false,bufferReady:false,latency:null,clockOffset:0});
        currentRoom=code;
        send({type:"room_joined",peerId,room:getRoomInfo(room)});
        if(room.audioReady) send({type:"audio_ready",filename:room.audioFilename,size:room.audioBuffer.length});
        broadcastToRoom(room,{type:"peer_joined",peerId,name:msg.name,room:getRoomInfo(room)},peerId);
        console.log(`[${code}] "${msg.name}" joined (${room.peers.size})`);
        break;
      }

      case "sync_ping": {
        const t2=Date.now();
        send({type:"sync_pong",t1:msg.t1,t2,t3:Date.now(),round:msg.round});
        break;
      }

      case "sync_complete": {
        const room=rooms.get(currentRoom);
        if(!room)return;
        const peer=room.peers.get(peerId);
        if(!peer)return;
        peer.clockOffset=msg.offset;
        peer.latency=msg.avgRtt;
        console.log(`[${currentRoom}] ${peer.name} synced: ${msg.offset.toFixed(1)}ms RTT=${msg.avgRtt.toFixed(1)}ms`);
        broadcastToRoom(room,{type:"peer_updated",room:getRoomInfo(room)});
        break;
      }

      case "buffer_ready": {
        const room=rooms.get(currentRoom);
        if(!room)return;
        const peer=room.peers.get(peerId);
        if(!peer)return;
        peer.bufferReady=true;
        if(msg.duration) room.playback.trackDuration=msg.duration;
        console.log(`[${currentRoom}] ${peer.name} buffer ready`);
        broadcastToRoom(room,{type:"peer_updated",room:getRoomInfo(room)});
        if(room.playback.playing) {
          send({type:"playback_sync",action:"play",startedAt:room.playback.startedAt,pausedAt:room.playback.pausedAt,serverTime:Date.now()});
        }
        break;
      }

      case "playback_control": {
        const room=rooms.get(currentRoom);
        if(!room||room.hostId!==peerId)return;
        const now=Date.now();
        if(msg.action==="play"){
          const pos=msg.position??room.playback.pausedAt;
          room.playback.playing=true;
          room.playback.startedAt=now;
          room.playback.pausedAt=pos;
          broadcastToRoom(room,{type:"playback_sync",action:"play",startedAt:now,pausedAt:pos,serverTime:now});
          console.log(`[${currentRoom}] ▶ Play @ ${pos.toFixed(1)}s`);
        } else if(msg.action==="pause"){
          const elapsed=(now-room.playback.startedAt)/1000;
          const pos=room.playback.pausedAt+elapsed;
          room.playback.playing=false;
          room.playback.pausedAt=pos;
          room.playback.startedAt=null;
          broadcastToRoom(room,{type:"playback_sync",action:"pause",position:pos,serverTime:now});
          console.log(`[${currentRoom}] ⏸ Pause @ ${pos.toFixed(1)}s`);
        } else if(msg.action==="seek"){
          room.playback.pausedAt=msg.position;
          if(room.playback.playing) room.playback.startedAt=now;
          broadcastToRoom(room,{type:"playback_sync",action:"seek",position:msg.position,playing:room.playback.playing,startedAt:room.playback.playing?now:null,serverTime:now});
        }
        break;
      }

      case "become_relay": {
        const room=rooms.get(currentRoom);
        if(!room)return;
        const peer=room.peers.get(peerId);
        if(peer) peer.isRelay=true;
        broadcastToRoom(room,{type:"peer_updated",room:getRoomInfo(room)});
        break;
      }
    }
  });

  ws.on("close",()=>{
    const room=rooms.get(currentRoom);
    if(!room)return;
    const peer=room.peers.get(peerId);
    room.peers.delete(peerId);
    if(peer?.isHost){
      broadcastToRoom(room,{type:"room_closed",reason:"L'host ha chiuso la stanza"});
      rooms.delete(currentRoom);
      console.log(`[${currentRoom}] Host left → closed`);
    } else {
      broadcastToRoom(room,{type:"peer_left",peerId,room:getRoomInfo(room)});
      console.log(`[${currentRoom}] "${peer?.name}" left`);
    }
  });
  ws.on("error",()=>{});
});

setInterval(()=>{
  for(const[code,room]of rooms){
    if(Date.now()-room.createdAt>6*3600000){
      broadcastToRoom(room,{type:"room_closed",reason:"Sessione scaduta"});
      rooms.delete(code);
    }
  }
},60000);

server.listen(PORT,"0.0.0.0",()=>{
  console.log(`🔊 SyncSound running on port ${PORT}`);
});
