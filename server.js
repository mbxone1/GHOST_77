const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 10e6 // 10MB for file uploads
});

const rooms = new Map(); // roomId -> {users, messages, roomKey, hostId, maxUsers}
const users = new Map();   // socketId -> {nickname, roomId, userId}
const pendingJoins = new Map();

app.use(express.static(path.join(__dirname)));
app.use(cors());

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Generate room encryption key
function generateRoomKey() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let key = '';
  for(let i = 0; i < 32; i++) key += chars.charAt(Math.floor(Math.random() * chars.length));
  return key;
}

io.on('connection', (socket) => {
  console.log('New:', socket.id);

  // CREATE ROOM
  socket.on('create-room', ({ nickname, userId }) => {
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase();
    const roomKey = generateRoomKey();
    rooms.set(roomId, { 
      users: [], 
      messages: [],
      roomKey: roomKey,     // SHARED KEY for all users in room
      hostId: socket.id,
      maxUsers: 10
    });
    socket.emit('room-created', { roomId, roomKey }); // Send key to creator
  });

  // REQUEST JOIN
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

  // APPROVE JOIN
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

    // Send room key to new user
    target.emit('room-key', { roomKey: room.roomKey });
    target.emit('joined-room', { roomId: req.roomId, users: room.users });
    target.to(req.roomId).emit('user-joined', { nickname: req.nickname, userId: req.userId });

    pendingJoins.delete(requesterId);
  }

  // SEND MESSAGE (text)
  socket.on('send-message', ({ roomId, encrypted, timestamp, type }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const msg = {
      sender: user.nickname,
      senderId: user.userId,
      encrypted,
      timestamp,
      type: type || 'text',
      id: Date.now()
    };

    const room = rooms.get(roomId);
    if (room) room.messages.push(msg);

    socket.to(roomId).emit('new-message', msg);
  });

  // SEND FILE (image, video, audio, document)
  socket.on('send-file', ({ roomId, fileData, fileName, fileType, encrypted, timestamp }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const msg = {
      sender: user.nickname,
      senderId: user.userId,
      fileData,
      fileName,
      fileType,
      encrypted,
      timestamp,
      type: 'file',
      id: Date.now()
    };

    const room = rooms.get(roomId);
    if (room) room.messages.push(msg);

    socket.to(roomId).emit('new-message', msg);
  });

  // VOICE MESSAGE
  socket.on('send-voice', ({ roomId, audioData, duration, encrypted, timestamp }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const msg = {
      sender: user.nickname,
      senderId: user.userId,
      audioData,
      duration,
      encrypted,
      timestamp,
      type: 'voice',
      id: Date.now()
    };

    const room = rooms.get(roomId);
    if (room) room.messages.push(msg);

    socket.to(roomId).emit('new-message', msg);
  });

  // TYPING
  socket.on('typing', ({ roomId, isTyping }) => {
    const user = users.get(socket.id);
    if (user) {
      socket.to(roomId).emit('user-typing', { nickname: user.nickname, userId: user.userId, isTyping });
    }
  });

  // MESSAGE REACTION
  socket.on('message-reaction', ({ roomId, messageId, reaction }) => {
    socket.to(roomId).emit('message-reaction', { messageId, reaction, userId: users.get(socket.id)?.userId });
  });

  // DELETE MESSAGE
  socket.on('delete-message', ({ roomId, messageId }) => {
    const room = rooms.get(roomId);
    if (room) {
      room.messages = room.messages.filter(m => m.id !== messageId);
    }
    io.to(roomId).emit('message-deleted', { messageId });
  });

  // DISCONNECT
  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      const room = rooms.get(user.roomId);
      if (room) {
        room.users = room.users.filter(u => u.id !== socket.id);
        socket.to(user.roomId).emit('user-left', { nickname: user.nickname, userId: user.userId });
        if (room.users.length === 0) {
          rooms.delete(user.roomId);
          console.log('Room deleted:', user.roomId);
        } else {
          if (room.hostId === socket.id && room.users.length > 0) {
            room.hostId = room.users[0].id;
          }
        }
      }
      users.delete(socket.id);
    }
    pendingJoins.delete(socket.id);
    console.log('Disconnected:', socket.id);
  });
});

setInterval(() => {
  for (const [id, room] of rooms) {
    if (room.users.length === 0) rooms.delete(id);
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('GHOST 77 v3.0 on port ' + PORT);
});
