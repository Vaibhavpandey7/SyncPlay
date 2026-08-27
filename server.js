const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const multer = require('multer');
const fs = require('fs');
const { execFile, spawn } = require('child_process');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 3000;
const SYNC_DELAY = 500; // ms — 500ms buffering cushion for high-precision WebAudio alignment
const MAX_ROOM = 4;
const UPLOADS = path.join(__dirname, 'uploads');
const CACHE = path.join(__dirname, 'cache');

[UPLOADS, CACHE].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d); });

// ─── Multer (file upload) ─────────────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: UPLOADS,
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '.mp3';
    const uid = Math.random().toString(36).substring(2, 8);
    cb(null, `${req.params.roomId.toUpperCase()}_${Date.now()}_${uid}${ext}`);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (_, file, cb) =>
    cb(null, /audio\/|video\/mp4/.test(file.mimetype))
});

// ─── Room state ───────────────────────────────────────────────────────────────
const rooms = new Map();

function makeRoom(id, hostSocketId, hostName, hostToken) {
  const token = hostToken || hostSocketId;
  return {
    id,
    hostId: hostSocketId,
    hostToken: token,
    playlist: [],          // array of { id, trackName, audioFile, addedBy }
    currentTrackIndex: -1,
    audioFile: null, trackName: null,
    isPlaying: false, position: 0,
    serverTimeAtUpdate: Date.now(),
    positionHistory: [],   // Server-side rolling 5-sample buffer for jitter-free room sync
    hostHardwareLatency: 0, // Hardware audio output latency of Host device
    downloading: false,
    users: new Map([[token, { name: hostName, isHost: true, socketId: hostSocketId, offline: false, disconnectTimer: null }]])
  };
}

function publicRoom(room) {
  return {
    id: room.id,
    playlist: room.playlist,
    currentTrackIndex: room.currentTrackIndex,
    trackName: room.trackName,
    hasAudio: !!room.audioFile,
    downloading: room.downloading,
    isPlaying: room.isPlaying,
    position: room.position,
    serverTimeAtUpdate: room.serverTimeAtUpdate,
    users: [...room.users.entries()].map(([token, u]) => ({
      id: u.socketId,
      userToken: token,
      name: u.name,
      isHost: u.isHost,
      offline: !!u.offline
    })),
    maxSize: MAX_ROOM
  };
}

function addTrackToRoom(room, trackName, filePath, userName = 'Someone') {
  let idx = room.playlist.findIndex(t => t.audioFile === filePath);
  let isNew = false;
  if (idx === -1) {
    const trackObj = {
      id: 'tr_' + Math.random().toString(36).substring(2, 8),
      trackName,
      audioFile: filePath,
      addedBy: userName
    };
    room.playlist.push(trackObj);
    idx = room.playlist.length - 1;
    isNew = true;
  }

  // Activate track immediately ONLY IF room has no active song
  const activated = (room.currentTrackIndex === -1 || !room.audioFile);

  if (activated) {
    room.currentTrackIndex = idx;
    room.audioFile = filePath;
    room.trackName = trackName;
    room.isPlaying = false;
    room.position = 0;
    room.positionHistory = [];
    room.serverTimeAtUpdate = Date.now();
  }

  return { idx, isNew, activated };
}

function notifyTrackAdded(roomId, room, trackName, filePath, userName) {
  const { activated } = addTrackToRoom(room, trackName, filePath, userName);

  if (activated) {
    io.to(roomId).emit('track-loaded', {
      trackName,
      audioUrl: getAudioUrl(roomId, room),
      playlist: room.playlist,
      currentTrackIndex: room.currentTrackIndex
    });
  } else {
    // Add to queue without interrupting current song!
    io.to(roomId).emit('playlist-updated', {
      playlist: room.playlist,
      currentTrackIndex: room.currentTrackIndex,
      addedTrackName: trackName,
      addedBy: userName
    });
  }
}

function cleanupRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room || room.users.size > 0) return;

  rooms.delete(roomId);
  console.log(`[Room ${roomId}] Deleted`);
  cleanOrphanFiles();
}

// Clean up all leftover files in uploads and cache directories that do not belong to active rooms
function cleanOrphanFiles() {
  const activeFiles = new Set();
  rooms.forEach(room => {
    room.playlist.forEach(track => {
      if (track.audioFile) {
        activeFiles.add(path.resolve(track.audioFile));
      }
    });
  });

  [UPLOADS, CACHE].forEach(dir => {
    if (!fs.existsSync(dir)) return;
    try {
      const files = fs.readdirSync(dir);
      files.forEach(file => {
        const filePath = path.join(dir, file);
        const resolvedPath = path.resolve(filePath);
        if (!activeFiles.has(resolvedPath)) {
          try {
            fs.unlinkSync(filePath);
            console.log(`[Cleanup] Deleted unused file: ${file}`);
          } catch (_) { }
        }
      });
    } catch (err) {
      console.error('[Cleanup Error]', err.message);
    }
  });
}

// Run orphan cleanup on server boot and every 15 minutes
cleanOrphanFiles();
setInterval(cleanOrphanFiles, 15 * 60 * 1000);

function getAudioUrl(roomId, room) {
  return `/audio/${roomId}?idx=${room.currentTrackIndex}&v=${Date.now()}`;
}

function genId() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

// ─── YouTube helpers ──────────────────────────────────────────────────────────
function extractVideoId(input) {
  input = input.trim();
  if (/^[a-zA-Z0-9_-]{11}$/.test(input)) return input;
  try {
    const u = new URL(input);
    if (u.hostname.includes('youtu.be')) return u.pathname.slice(1).split('?')[0];
    return u.searchParams.get('v');
  } catch { return null; }
}

const FFMPEG_BIN = fs.existsSync(path.join(__dirname, 'ffmpeg'))
  ? path.join(__dirname, 'ffmpeg')
  : 'ffmpeg'; // fall back to system ffmpeg if available

function cachedPath(videoId, ext = 'mp3') {
  return path.join(CACHE, `${videoId}.${ext}`);
}

// Find whichever cached file exists (webm/m4a preferred as native formats, mp3 as fallback)
function findCached(videoId) {
  for (const ext of ['webm', 'm4a', 'opus', 'mp3', 'ogg', 'wav', 'flac', 'aac']) {
    const p = cachedPath(videoId, ext);
    if (fs.existsSync(p)) return p;
  }
  if (fs.existsSync(CACHE)) {
    try {
      const files = fs.readdirSync(CACHE);
      const match = files.find(f => f.startsWith(`${videoId}.`));
      if (match) return path.join(CACHE, match);
    } catch (_) { }
  }
  return null;
}

const COOKIES_FILE = path.join(__dirname, 'cookies.txt');

// Download audio via yt-dlp with multi-client & cookie fallback pipeline
async function downloadAudio(videoId, roomId) {
  const attempts = [
    { client: 'tv,android',       useCookies: false },
    { client: 'creator,android',  useCookies: false },
    { client: 'android,web',      useCookies: false },
    { client: 'android,mweb,web', useCookies: true  }
  ];

  let lastError = null;

  for (let i = 0; i < attempts.length; i++) {
    const { client, useCookies } = attempts[i];
    const hasCookies = useCookies && fs.existsSync(COOKIES_FILE);

    // Skip attempt if useCookies is requested but cookies.txt does not exist
    if (useCookies && !hasCookies) continue;

    try {
      console.log(`[yt-dlp] Attempt ${i + 1}/${attempts.length} for ${videoId} (client: ${client}, cookies: ${hasCookies})`);
      return await executeYtdlp(videoId, roomId, client, hasCookies);
    } catch (err) {
      console.warn(`[yt-dlp] Attempt ${i + 1} failed for ${videoId}: ${err.message}`);
      lastError = err;
    }
  }

  throw lastError || new Error('All download attempts failed');
}

function executeYtdlp(videoId, roomId, client, useCookies) {
  return new Promise((resolve, reject) => {
    const outTemplate = path.join(CACHE, `${videoId}.%(ext)s`);
    const args = [
      '--no-playlist',
      '--js-runtimes', 'node',
      '--extractor-args', `youtube:player_client=${client}`,
      '-f', 'bestaudio/best',
      '--output', outTemplate,
      '--newline',
      '--no-simulate',
      '--print', 'before_dl:title'
    ];

    if (useCookies) {
      args.push('--cookies', COOKIES_FILE);
    }

    args.push(`https://www.youtube.com/watch?v=${videoId}`);

    const proc = spawn('yt-dlp', args);
    let title = videoId;
    let titleCaptured = false;
    let lastStderr = '';

    proc.stdout.on('data', chunk => {
      const lines = chunk.toString().split('\n').filter(Boolean);
      lines.forEach(line => {
        if (!titleCaptured && !line.startsWith('[')) {
          title = line.trim();
          titleCaptured = true;
          return;
        }

        const pctMatch = line.match(/\[download\]\s+([\d.]+)%/);
        if (pctMatch) {
          const percent = parseFloat(pctMatch[1]);
          io.to(roomId).emit('download-progress', {
            percent: Math.round(percent * 0.85),
            status: 'downloading'
          });
        }

        if (line.includes('[ExtractAudio]') || line.includes('Destination:')) {
          io.to(roomId).emit('download-progress', { percent: 90, status: 'converting' });
        }
      });
    });

    proc.stderr.on('data', chunk => {
      const line = chunk.toString();
      if (line.includes('ERROR')) {
        console.error(`[yt-dlp Error] ${line.trim()}`);
        lastStderr = line.replace(/ERROR:\s*/i, '').trim();
      }
    });

    proc.on('close', async code => {
      if (code !== 0) {
        return reject(new Error(lastStderr || `yt-dlp exited with code ${code}`));
      }

      let found = null;
      for (let i = 0; i < 5; i++) {
        found = findCached(videoId);
        if (found) break;
        await new Promise(r => setTimeout(r, 100));
      }

      if (found) {
        io.to(roomId).emit('download-progress', { percent: 100, status: 'done' });
        resolve({ filePath: found, title });
      } else {
        reject(new Error('Output file not found after download'));
      }
    });

    proc.on('error', err => {
      if (err.code === 'ENOENT') reject(new Error('yt-dlp not found. Please install it.'));
      else reject(err);
    });
  });
}

// ─── HTTP Routes ──────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/ping', (_, res) => res.json({ serverTime: Date.now() }));

app.get('/room/:id', (req, res) => {
  const room = rooms.get(req.params.id.toUpperCase());
  if (!room) return res.status(404).json({ error: 'Room not found' });
  res.json({ exists: true, userCount: room.users.size, maxSize: MAX_ROOM });
});

function getVideoTitle(videoId) {
  return new Promise(resolve => {
    const execArgs = [
      '--no-playlist',
      '--js-runtimes', 'node',
      '--extractor-args', 'youtube:player_client=tv,android',
      '--no-download',
      '--get-title',
      `https://www.youtube.com/watch?v=${videoId}`
    ];
    execFile('yt-dlp', execArgs, { timeout: 10000 }, (err, stdout) => {
      const title = (stdout || '').trim();
      resolve(title || videoId);
    });
  });
}

// ── YouTube download endpoint ─────────────────────────────────────────────────
app.post('/download/:roomId', async (req, res) => {
  const roomId = req.params.roomId.toUpperCase();
  const room = rooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Room not found' });
  if (room.downloading) return res.status(409).json({ error: 'Already downloading' });

  const { url } = req.body;
  const videoId = extractVideoId(url || '');
  if (!videoId) return res.status(400).json({ error: 'Invalid YouTube URL' });

  // Respond immediately — progress comes via socket
  res.json({ success: true, videoId });

  // Cache hit — serve instantly
  const cachedFile = findCached(videoId);
  if (cachedFile) {
    console.log(`[Cache HIT] ${videoId} → ${cachedFile}`);
    io.to(roomId).emit('download-progress', { percent: 100, status: 'done' });
    const trackName = await getVideoTitle(videoId);
    notifyTrackAdded(roomId, room, trackName, cachedFile, req.headers['x-user-name'] || 'Host');
    return;
  }

  // Cache miss — download
  console.log(`[Cache MISS] Downloading ${videoId} for room ${roomId}`);
  room.downloading = true;
  io.to(roomId).emit('download-start', { videoId });

  try {
    const { filePath, title } = await downloadAudio(videoId, roomId);

    // Fetch clean title if yt-dlp didn't provide one
    let trackName = title !== videoId ? title : videoId;
    notifyTrackAdded(roomId, room, trackName, filePath, req.headers['x-user-name'] || 'Host');
    room.downloading = false;
    console.log(`[Room ${roomId}] Track loaded: ${trackName}`);
  } catch (err) {
    room.downloading = false;
    console.error(`[Room ${roomId}] Download error:`, err.message);
    io.to(roomId).emit('download-error', { message: err.message });
  }
});

// ── File upload endpoint ──────────────────────────────────────────────────────
app.post('/upload/:roomId', (req, res) => {
  upload.single('audio')(req, res, err => {
    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'File is too large. Max 50 MB.' });
      }
      if (err.message === 'Request aborted' || err.code === 'ECONNRESET') {
        console.log(`[Upload] Client aborted upload for room ${req.params.roomId}`);
        return;
      }
      return res.status(400).json({ error: err.message || 'Upload failed' });
    }

    const roomId = req.params.roomId.toUpperCase();
    const room = rooms.get(roomId);
    if (!room) return res.status(404).json({ error: 'Room not found' });
    if (!req.file) return res.status(400).json({ error: 'No valid audio file' });

    const trackName = req.file.originalname.replace(/\.[^/.]+$/, '');
    notifyTrackAdded(roomId, room, trackName, req.file.path, req.headers['x-user-name'] || 'Host');

    console.log(`[Room ${roomId}] File uploaded: ${trackName}`);
    res.json({ success: true, trackName });
  });
});

// ── Stream audio ──────────────────────────────────────────────────────────────
app.get('/audio/:roomId', (req, res) => {
  const roomId = req.params.roomId.toUpperCase();
  const room = rooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Room not found' });

  let filePath = room.audioFile;
  if (req.query.idx !== undefined) {
    const idx = parseInt(req.query.idx, 10);
    if (room.playlist[idx] && fs.existsSync(room.playlist[idx].audioFile)) {
      filePath = room.playlist[idx].audioFile;
    }
  }

  if (!filePath || !fs.existsSync(filePath))
    return res.status(404).json({ error: 'No audio available' });

  const fileSize = fs.statSync(filePath).size;
  const range = req.headers.range;

  // Detect correct content type from file extension
  const ext = path.extname(filePath).toLowerCase();
  const MIME = {
    '.mp3': 'audio/mpeg',
    '.webm': 'audio/webm',
    '.m4a': 'audio/mp4',
    '.opus': 'audio/ogg',
    '.ogg': 'audio/ogg',
    '.wav': 'audio/wav',
    '.flac': 'audio/flac',
    '.aac': 'audio/aac',
  };
  const contentType = MIME[ext] || 'audio/mpeg';

  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  if (range) {
    const [startStr, endStr] = range.replace(/bytes=/, '').split('-');
    const start = parseInt(startStr, 10);
    const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': end - start + 1,
      'Content-Type': contentType,
      'Cache-Control': 'no-cache, no-store, must-revalidate',
    });
    fs.createReadStream(filePath, { start, end }).pipe(res);
  } else {
    res.writeHead(200, {
      'Content-Length': fileSize,
      'Content-Type': contentType,
      'Accept-Ranges': 'bytes',
      'Cache-Control': 'no-cache, no-store, must-revalidate',
    });
    fs.createReadStream(filePath).pipe(res);
  }
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log(`[Socket] +${socket.id}`);

  socket.on('ping', () => socket.emit('pong', { serverTime: Date.now() }));
  socket.on('ping-clock', cb => { if (typeof cb === 'function') cb({ serverTime: Date.now() }); });

  // Create room
  socket.on('create-room', ({ userName, userToken }, cb) => {
    const id = genId();
    const token = userToken || socket.id;
    const room = makeRoom(id, socket.id, userName || 'Host', token);
    rooms.set(id, room);
    socket.join(id);
    socket.data.roomId = id;
    socket.data.userToken = token;
    console.log(`[Room ${id}] Created by ${userName} (token: ${token})`);
    cb({ success: true, isHost: true, room: publicRoom(room) });
  });

  // Join or Rejoin room
  socket.on('join-room', ({ roomId, userName, userToken }, cb) => {
    const id = roomId.toUpperCase();
    const room = rooms.get(id);
    if (!room) return cb({ error: 'Room not found' });

    const token = userToken || socket.id;
    let user = room.users.get(token);

    if (user) {
      // Rejoining existing session!
      if (user.disconnectTimer) {
        clearTimeout(user.disconnectTimer);
        user.disconnectTimer = null;
      }
      user.socketId = socket.id;
      user.offline = false;
      if (userName) user.name = userName;
      if (user.isHost) room.hostId = socket.id;

      socket.join(id);
      socket.data.roomId = id;
      socket.data.userToken = token;

      socket.to(id).emit('user-status-changed', {
        users: publicRoom(room).users
      });
      console.log(`[Room ${id}] ${user.name} reconnected (token: ${token})`);
    } else {
      // New user joining
      if (room.users.size >= MAX_ROOM) return cb({ error: 'Room is full (max 4)' });

      user = { name: userName || 'Guest', isHost: false, socketId: socket.id, offline: false, disconnectTimer: null };
      room.users.set(token, user);
      socket.join(id);
      socket.data.roomId = id;
      socket.data.userToken = token;

      socket.to(id).emit('user-joined', {
        userId: socket.id, name: userName,
        users: publicRoom(room).users
      });
      console.log(`[Room ${id}] ${userName} joined`);
    }

    // Compute exact synced position for the joiner
    let syncPos = room.position;
    let syncPlayAt = null;
    if (room.isPlaying) {
      const now = Date.now();
      if (now >= room.serverTimeAtUpdate) {
        const elapsed = (now - room.serverTimeAtUpdate) / 1000;
        syncPos = Math.max(0, room.position + elapsed);
      }
      syncPlayAt = now + SYNC_DELAY;
    }

    cb({
      success: true,
      isHost: user.isHost,
      room: {
        ...publicRoom(room),
        position: syncPos,
        joinedAt: Date.now(),
        serverTimeAtUpdate: Date.now(),
        playAt: syncPlayAt
      }
    });
  });

  // Playback events — any user can send these
  socket.on('play', ({ position }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room) return;
    const now = Date.now();
    const playAt = now + SYNC_DELAY; // scheduled future start time
    const pos = typeof position === 'number' ? position : 0;
    room.isPlaying = true;
    room.position = pos;
    room.positionHistory = [];
    room.serverTimeAtUpdate = playAt; // Position is valid starting at playAt
    room.playAt = playAt;
    io.to(room.id).emit('play', { position: pos, playAt });
  });

  // Host real-time playback position pulse relay with server-side moving average smoothing
  socket.on('host-pulse', ({ position, serverTime, hardwareLatency }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room) return;
    const user = room.users.get(socket.data.userToken);
    if (!user || !user.isHost) return;

    if (typeof hardwareLatency === 'number') {
      room.hostHardwareLatency = hardwareLatency;
    }

    const now = Date.now();
    // Maintain 5-sample rolling history on server. Each entry stores position as-of its capture time.
    room.positionHistory.push({ pos: position, capturedAt: serverTime });
    if (room.positionHistory.length > 5) room.positionHistory.shift();

    // Server calculates smoothed average: advance each sample to 'now' and average them
    const avgPos = room.positionHistory.reduce((sum, s) => {
      const sElapsedSec = (now - s.capturedAt) / 1000;
      return sum + (s.pos + sElapsedSec);
    }, 0) / room.positionHistory.length;

    room.position = avgPos;
    room.serverTimeAtUpdate = now;

    socket.to(room.id).emit('sync-pulse', {
      position: avgPos,
      serverTime: now,
      isPlaying: room.isPlaying,
      hostHardwareLatency: room.hostHardwareLatency || 0
    });
  });

  socket.on('pause', ({ position }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room) return;
    room.isPlaying = false;
    room.position = typeof position === 'number' ? position : room.position;
    room.positionHistory = [];
    room.serverTimeAtUpdate = Date.now();
    io.to(room.id).emit('pause', { position: room.position });
  });

  socket.on('seek', ({ position }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room) return;
    const now = Date.now();
    const playAt = room.isPlaying ? now + SYNC_DELAY : null;
    const pos = typeof position === 'number' ? position : 0;
    room.position = pos;
    room.positionHistory = [];
    room.serverTimeAtUpdate = playAt || now;
    if (playAt) room.playAt = playAt;
    io.to(room.id).emit('seek', { position: pos, playAt, isPlaying: room.isPlaying });
  });




  // Playlist track selection (Host only)
  socket.on('select-track', ({ index, autoPlay }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room || index < 0 || index >= room.playlist.length) return;

    if (socket.id !== room.hostId) {
      return socket.emit('error-msg', { message: 'Only the host can switch songs' });
    }

    const track = room.playlist[index];
    const shouldAutoPlay = autoPlay === true || room.isPlaying;
    const now = Date.now();
    const playAt = shouldAutoPlay ? now + SYNC_DELAY : null;

    room.currentTrackIndex = index;
    room.audioFile = track.audioFile;
    room.trackName = track.trackName;
    room.isPlaying = shouldAutoPlay;
    room.position = 0;
    room.positionHistory = [];
    room.serverTimeAtUpdate = playAt || now;
    if (playAt) room.playAt = playAt;

    io.to(room.id).emit('track-loaded', {
      trackName: track.trackName,
      audioUrl: getAudioUrl(room.id, room),
      playlist: room.playlist,
      currentTrackIndex: room.currentTrackIndex,
      autoPlay: shouldAutoPlay,
      playAt
    });
    console.log(`[Room ${room.id}] Host switched to track #${index}: ${track.trackName} (autoPlay: ${shouldAutoPlay})`);
  });

  // Playlist track deletion (Host only)
  socket.on('remove-track', ({ index }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room || index < 0 || index >= room.playlist.length) return;

    if (socket.id !== room.hostId) {
      return socket.emit('error-msg', { message: 'Only the host can remove songs' });
    }

    const [removed] = room.playlist.splice(index, 1);
    console.log(`[Room ${room.id}] Host removed track #${index}: ${removed.trackName}`);

    if (removed.audioFile?.startsWith(UPLOADS) && fs.existsSync(removed.audioFile)) {
      const stillInPlaylist = room.playlist.some(t => t.audioFile === removed.audioFile);
      if (!stillInPlaylist) try { fs.unlinkSync(removed.audioFile); } catch (_) { }
    }

    if (room.playlist.length === 0) {
      room.currentTrackIndex = -1;
      room.audioFile = null;
      room.trackName = null;
      room.isPlaying = false;
      room.position = 0;
      room.positionHistory = [];
      io.to(room.id).emit('room-cleared', {});
      return;
    }

    if (index === room.currentTrackIndex) {
      const newIdx = Math.min(index, room.playlist.length - 1);
      const nextTrack = room.playlist[newIdx];
      room.currentTrackIndex = newIdx;
      room.audioFile = nextTrack.audioFile;
      room.trackName = nextTrack.trackName;
      room.isPlaying = false;
      room.position = 0;
      room.positionHistory = [];
      room.serverTimeAtUpdate = Date.now();

      io.to(room.id).emit('track-loaded', {
        trackName: nextTrack.trackName,
        audioUrl: getAudioUrl(room.id, room),
        playlist: room.playlist,
        currentTrackIndex: room.currentTrackIndex
      });
    } else {
      if (index < room.currentTrackIndex) {
        room.currentTrackIndex--;
      }
      io.to(room.id).emit('playlist-updated', {
        playlist: room.playlist,
        currentTrackIndex: room.currentTrackIndex
      });
    }
  });

  // Explicit room leave (when user clicks "Leave Room")
  socket.on('leave-room', () => {
    const roomId = socket.data.roomId;
    const userToken = socket.data.userToken;
    const room = rooms.get(roomId);
    if (!room) return;

    let userKey = userToken;
    let user = room.users.get(userKey);
    if (!user) {
      for (const [k, u] of room.users.entries()) {
        if (u.socketId === socket.id) {
          userKey = k;
          user = u;
          break;
        }
      }
    }
    if (!user) return;

    if (user.disconnectTimer) clearTimeout(user.disconnectTimer);

    const wasHost = user.isHost;
    room.users.delete(userKey);
    socket.leave(roomId);
    delete socket.data.roomId;
    delete socket.data.userToken;

    console.log(`[Room ${roomId}] ${user.name} left explicitly`);

    const remainingUsers = [...room.users.values()];
    if (remainingUsers.length === 0) {
      cleanupRoom(roomId);
      return;
    }

    let newHostId = room.hostId;
    if (wasHost) {
      const nextOnlineUser = remainingUsers.find(u => !u.offline) || remainingUsers[0];
      nextOnlineUser.isHost = true;
      room.hostId = nextOnlineUser.socketId;
      newHostId = nextOnlineUser.socketId;
    }

    // Pause room playback if host leaves or no online users remain
    const activeOnline = remainingUsers.filter(u => !u.offline);
    if (activeOnline.length === 0 || wasHost) {
      room.isPlaying = false;
      io.to(roomId).emit('pause', { position: room.position });
    }

    io.to(roomId).emit('user-left', {
      userId: socket.id,
      name: user.name,
      users: publicRoom(room).users,
      newHostId
    });
  });

  // Disconnect with 15-second grace window for reconnection
  socket.on('disconnect', () => {
    const roomId = socket.data.roomId;
    const userToken = socket.data.userToken;
    const room = rooms.get(roomId);
    if (!room) return;

    let userKey = userToken;
    let user = room.users.get(userKey);
    if (!user) {
      for (const [k, u] of room.users.entries()) {
        if (u.socketId === socket.id) {
          userKey = k;
          user = u;
          break;
        }
      }
    }
    if (!user) return;

    user.offline = true;
    console.log(`[Room ${roomId}] ${user.name} went offline (15s grace window started)`);

    // If host went offline, auto-pause room playback
    if (user.isHost) {
      room.isPlaying = false;
      io.to(roomId).emit('pause', { position: room.position });
    }

    io.to(roomId).emit('user-status-changed', {
      users: publicRoom(room).users
    });

    // 15-second grace period before officially removing user / reassigning host
    user.disconnectTimer = setTimeout(() => {
      if (!user.offline) return; // User reconnected during grace period!

      const wasHost = user.isHost;
      room.users.delete(userKey);
      console.log(`[Room ${roomId}] ${user.name} grace period expired -> removed`);

      const remainingUsers = [...room.users.values()];
      if (remainingUsers.length === 0) {
        cleanupRoom(roomId);
        return;
      }

      let newHostId = room.hostId;
      if (wasHost) {
        const nextOnlineUser = remainingUsers.find(u => !u.offline) || remainingUsers[0];
        nextOnlineUser.isHost = true;
        room.hostId = nextOnlineUser.socketId;
        newHostId = nextOnlineUser.socketId;
      }

      io.to(roomId).emit('user-left', {
        userId: socket.id,
        name: user.name,
        users: publicRoom(room).users,
        newHostId
      });
    }, 15000);
  });
});

// ─── Continuous Room Sync Pulse (5-second smooth interval) ──────────────────────
setInterval(() => {
  rooms.forEach(room => {
    if (room.isPlaying && room.users.size > 0) {
      const now = Date.now();
      if (now < room.serverTimeAtUpdate) return; // Skip while still in buffering delay
      const elapsed = (now - room.serverTimeAtUpdate) / 1000;
      const currentPosition = room.position + elapsed;
      io.to(room.id).emit('sync-pulse', {
        position: currentPosition,
        serverTime: now,
        isPlaying: true,
        hostHardwareLatency: room.hostHardwareLatency || 0
      });
    }
  });
}, 5000);

// Catch-all route to serve index.html for SPA page reloads / custom URLs
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Express Error Handler
app.use((err, req, res, next) => {
  if (err.message === 'Request aborted' || err.code === 'ECONNRESET') {
    console.log('[HTTP] Client aborted request');
    return;
  }
  console.error('[HTTP Error]', err.message);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  const os = require('os');
  const interfaces = os.networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        addresses.push(iface.address);
      }
    }
  }
  console.log(`\n🎵 SyncPlay running at:`);
  console.log(`   ➜ Local:   http://localhost:${PORT}`);
  addresses.forEach(ip => {
    console.log(`   ➜ Network (Phone/Wi-Fi): http://${ip}:${PORT}`);
  });
  console.log('');
});

