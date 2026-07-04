
# Let's create the updated server.js with all the requested features

server_js_new = '''const express = require('express');
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
const pendingJoins = new Map(); // طلبات الانضمام المعلقة

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

  // طلب الانضمام لغرفة (يحتاج موافقة)
  socket.on('request-join-room', ({ roomId, nickname, userId }) => {
    const room = rooms.get(roomId);
    if (!room) {
      socket.emit('error', 'الغرفة م exista');
      return;
    }
    if (room.users.length >= room.maxUsers) {
      socket.emit('error', 'الغرفة ممتلئة');
      return;
    }

    // حفظ الطلب
    pendingJoins.set(socket.id, { roomId, nickname, userId });

    // إرسال طلب للمضيف
    socket.to(roomId).emit('join-request', {
      requesterId: socket.id,
      nickname,
      userId
    });

    // إذا الغرفة فارغة، ندخل مباشرة
    if (room.users.length === 0) {
      approveJoin(socket.id);
    }
  });

  // الموافقة على الانضمام
  socket.on('approve-join', ({ requesterId, approved }) => {
    const hostUser = users.get(socket.id);
    if (!hostUser) return;

    const room = rooms.get(hostUser.roomId);
    if (!room || room.hostId !== socket.id) return;

    if (approved) {
      approveJoin(requesterId);
    } else {
      const request = pendingJoins.get(requesterId);
      if (request) {
        io.to(requesterId).emit('join-rejected', { reason: 'تم رفض طلبك' });
        pendingJoins.delete(requesterId);
      }
    }
  });

  function approveJoin(requesterId) {
    const request = pendingJoins.get(requesterId);
    if (!request) return;

    const room = rooms.get(request.roomId);
    if (!room) return;

    const socketObj = io.sockets.sockets.get(requesterId);
    if (!socketObj) return;

    socketObj.join(request.roomId);
    room.users.push({ id: requesterId, nickname: request.nickname, userId: request.userId });
    users.set(requesterId, { nickname: request.nickname, roomId: request.roomId, userId: request.userId });

    socketObj.to(request.roomId).emit('user-joined', { nickname: request.nickname, userId: request.userId });
    socketObj.emit('joined-room', { roomId: request.roomId, users: room.users });
    
    pendingJoins.delete(requesterId);
  }

  // إرسال رسالة مشفرة
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

  // مؤشر الكتابة
  socket.on('typing', ({ roomId, isTyping }) => {
    const user = users.get(socket.id);
    if (user) {
      socket.to(roomId).emit('user-typing', { nickname: user.nickname, userId: user.userId, isTyping });
    }
  });

  // قطع الاتصال
  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      const room = rooms.get(user.roomId);
      if (room) {
        room.users = room.users.filter(u => u.id !== socket.id);
        socket.to(user.roomId).emit('user-left', { nickname: user.nickname, userId: user.userId });
        if (room.users.length === 0) {
          rooms.delete(user.roomId);
          console.log('🗑️ غرفة محيوفة:', user.roomId);
        } else {
          // تحديث المضيف إذا خرج
          if (room.hostId === socket.id && room.users.length > 0) {
            room.hostId = room.users[0].id;
          }
        }
      }
      users.delete(socket.id);
    }
    pendingJoins.delete(socket.id);
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
  console.log(`🚀 GHOST 77 شغال على البورت ${PORT}`);
});
'''

with open('/mnt/agents/output/server.js', 'w', encoding='utf-8') as f:
    f.write(server_js_new)

print("✅ server.js created successfully!")
