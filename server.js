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

// ====== كلشي في RAM ======
const rooms = new Map();
const users = new Map();

// تقديم الملفات الثابتة (Frontend)
app.use(express.static(path.join(__dirname)));
app.use(cors());

// الصفحة الرئيسية
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// ====== Socket.IO ======
io.on('connection', (socket) => {
  console.log('🔌 جديد:', socket.id);

  // إنشاء غرفة
  socket.on('create-room', ({ nickname }) => {
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase();
    rooms.set(roomId, { users: [], messages: [] });
    socket.emit('room-created', { roomId });
  });

  // الانضمام لغرفة
  socket.on('join-room', ({ roomId, nickname }) => {
    const room = rooms.get(roomId);
    if (!room) {
      socket.emit('error', 'الغرفة م exista');
      return;
    }
    if (room.users.length >= 2) {
      socket.emit('error', 'الغرفة ممتلئة');
      return;
    }

    socket.join(roomId);
    room.users.push({ id: socket.id, nickname });
    users.set(socket.id, { nickname, roomId });

    socket.to(roomId).emit('user-joined', { nickname });
    socket.emit('joined-room', { roomId, users: room.users });
  });

  // إرسال رسالة مشفرة
  socket.on('send-message', ({ roomId, encrypted, timestamp }) => {
    const user = users.get(socket.id);
    if (!user) return;

    const msg = {
      sender: user.nickname,
      encrypted,
      timestamp,
      id: Date.now()
    };

    const room = rooms.get(roomId);
    if (room) room.messages.push(msg);

    socket.to(roomId).emit('new-message', msg);
  });

  // مؤشر الكتابة
  socket.on('typing', ({ roomId, isTyping }) => {
    const user = users.get(socket.id);
    if (user) {
      socket.to(roomId).emit('user-typing', { nickname: user.nickname, isTyping });
    }
  });

  // قطع الاتصال
  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      const room = rooms.get(user.roomId);
      if (room) {
        room.users = room.users.filter(u => u.id !== socket.id);
        socket.to(user.roomId).emit('user-left', { nickname: user.nickname });
        if (room.users.length === 0) {
          rooms.delete(user.roomId);
          console.log('🗑️ غرفة محيوفة:', user.roomId);
        }
      }
      users.delete(socket.id);
    }
    console.log('❌ خرج:', socket.id);
  });
});

// تنظيف الغرف الفارغة كل 30 دقيقة
setInterval(() => {
  for (const [id, room] of rooms) {
    if (room.users.length === 0) rooms.delete(id);
  }
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 NionChat شغال على البورت ${PORT}`);
});