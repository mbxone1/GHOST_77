const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

const rooms = new Map();
const users = new Map();
const pendingJoins = new Map();

app.use(express.static(path.join(__dirname)));
app.use(cors());

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
  console.log('New:', socket.id);

  socket.on('create-room', ({ nickname, userId }) => {
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase();
    rooms.set(roomId, { 
      users: [], 
      messages: [],
      hostId: socket.id,
      maxUsers: 10
    });
    socket.emit('room-created', { roomId });
  });

  socket.on('request-join-room', ({ roomId, nickname, userId }) => {
    const room = rooms.get(roomId);
    if (!room) {
      socket.emit('error', 'Room not found');
      return;
    }
    if (room.users.length >= room.maxUsers) {
      socket.emit('error', 'Room full');
      return;
    }

    pendingJoins.set(socket.id, { roomId, nickname, userId });

    socket.to(roomId).emit('join-request', {
      requesterId: socket.id,
      nickname,
      userId
    });

    if (room.users.length === 0) {
      approveJoin(socket.id);
    }
  });

  socket.on('approve-join', ({ requesterId, approved }) => {
    const hostUser = users.get(socket.id);
    if (!hostUser) return;

    const room = rooms.get(hostUser.roomId);
    if (!room || room.hostId !== socket.id) return;

    if (approved) {
      approveJoin(requesterId);
    } else {
      const req = pendingJoins.get(requesterId);
      if (req) {
        io.to(requesterId).emit('join-rejected', { reason: 'Rejected' });
        pendingJoins.delete(requesterId);
      }
    }
  });

  function approveJoin(requesterId) {
    const req = pendingJoins.get(requesterId);
    if (!req) return;

    const room = rooms.get(req.roomId);
    if (!room) return;

    const all = Array.from(io.sockets.sockets.values());
    const target = all.find(s => s.id === requesterId);
    if (!target) return;

    target.join(req.roomId);
    room.users.push({ id: requesterId, nickname: req.nickname, userId: req.userId });
    users.set(requesterId, { nickname: req.nickname, roomId: req.roomId, userId: req.userId });

    target.to(req.roomId).emit('user-joined', { nickname: req.nickname, userId: req.userId });
    target.emit('joined-room', { roomId: req.roomId, users: room.users });
    
    pendingJoins.delete(requesterId);
  }

  socket.on('send-message', ({ roomId, encrypted, timestamp }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const msg = {
      sender: user.nickname,
      senderId: user.userId,
      encrypted,
      timestamp,
      id: Date.now()
    };

    const room = rooms.get(roomId);
    if (room) room.messages.push(msg);

    socket.to(roomId).emit('new-message', msg);
  });

  socket.on('typing', ({ roomId, isTyping }) => {
    const user = users.get(socket.id);
    if (user) {
      socket.to(roomId).emit('user-typing', { 
        nickname: user.nickname, 
        userId: user.userId, 
        isTyping 
      });
    }
  });

  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      const room = rooms.get(user.roomId);
      if (room) {
        room.users = room.users.filter(u => u.id !== socket.id);
        socket.to(user.roomId).emit('user-left', { 
          nickname: user.nickname, 
          userId: user.userId 
        });
        if (room.users.length === 0) {
          rooms.delete(user.roomId);
        } else {
          if (room.hostId === socket.id && room.users.length > 0) {
            room.hostId = room.users[0].id;
          }
        }
      }
      users.delete(socket.id);
    }
    pendingJoins.delete(socket.id);
  });
});

setInterval(() => {
  for (const [id, room] of rooms) {
    if (room.users.length === 0) rooms.delete(id);
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('GHOST 77 on port ' + PORT);
});
