console.log(`[SyncPlay] Booting server process (pid: ${process.pid}, node: ${process.version})...`);
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const multer = require('multer');
const fs = require('fs');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const { execFile, spawn } = require('child_process');

process.on('uncaughtException', err => {
  console.error('[UNCAUGHT EXCEPTION]', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[UNHANDLED REJECTION]', reason);
});

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingTimeout: 20000,   // 20s heartbeat timeout for quick detection of dead sockets on mobile/flaky networks
  pingInterval: 10000,  // 10s ping interval for rapid health checks
  connectTimeout: 30000,
  transports: ['websocket', 'polling']
});

const PORT = process.env.PORT || 3000;
const SYNC_DELAY = 600; // ms — buffering cushion for high-precision WebAudio alignment
const MAX_ROOM = 4;
const RECONNECT_GRACE_MS = 60000; // 60-second grace window for seamless user reconnection
const UPLOADS = path.join(__dirname, 'uploads');
const CACHE = path.join(__dirname, 'cache');

[UPLOADS, CACHE].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d); });

// ─── Multer (file upload) ─────────────────────────────────────────────────────
const AUDIO_EXTS = /\.(mp3|m4a|aac|wav|ogg|opus|flac|webm|mp4|wma)$/i;

const storage = multer.diskStorage({
  destination: UPLOADS,
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '.mp3';
    const safeRoomId = (req.params.roomId || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 10) || 'ROOM';
    const uid = Math.random().toString(36).substring(2, 8);
    cb(null, `${safeRoomId}_${Date.now()}_${uid}${ext}`);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    const isAudioMime = !file.mimetype || /audio\/|video\/mp4|application\/octet-stream/.test(file.mimetype);
    const isAudioExt = AUDIO_EXTS.test(file.originalname);
    if (isAudioMime || isAudioExt) {
      cb(null, true);
    } else {
      cb(new Error('Only audio files (MP3, M4A, AAC, WAV, OGG, OPUS, FLAC) are supported.'));
    }
  }
});

// ─── Room state ───────────────────────────────────────────────────────────────
const rooms = new Map();

function sanitizeUserName(name, fallback = 'Guest') {
  if (!name || typeof name !== 'string') return fallback;
  const clean = name.replace(/[\u{1F300}-\u{1FAFF}]|[\u{1F600}-\u{1F64F}]|[\u{1F680}-\u{1F6FF}]|[\u{2600}-\u{27BF}]/gu, '')
                    .replace(/[^\w\s.-]/g, '')
                    .replace(/\s+/g, ' ')
                    .trim()
                    .slice(0, 20);
  return clean || fallback;
}

function makeRoom(id, hostSocketId, hostName, hostToken) {
  const token = hostToken || hostSocketId;
  const cleanHostName = sanitizeUserName(hostName, 'Host');
  return {
    id,
    creatorToken: token,
    hostId: hostSocketId,
    hostToken: token,
    playlist: [],          // array of { id, trackName, audioFile, addedBy, thumbnail }
    currentTrackIndex: -1,
    audioFile: null, trackName: null, thumbnail: null,
    isPlaying: false, position: 0,
    serverTimeAtUpdate: Date.now(),
    lastHostPulseTime: 0,  // Timestamp of latest host real-time pulse for pulse de-duplication
    positionHistory: [],   // Server-side rolling 5-sample buffer for jitter-free room sync
    hostHardwareLatency: 0, // Hardware audio output latency of Host device
    downloading: false,
    users: new Map([[token, { name: cleanHostName, isHost: true, socketId: hostSocketId, userToken: token, offline: false, isUploading: false, disconnectTimer: null }]])
  };
}

function publicRoom(room) {
  return {
    id: room.id,
    playlist: room.playlist,
    currentTrackIndex: room.currentTrackIndex,
    trackName: room.trackName,
    thumbnail: room.thumbnail || null,
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

function addTrackToRoom(room, trackName, filePath, userName = 'Someone', thumbnail = null) {
  let idx = room.playlist.findIndex(t => t.audioFile === filePath);
  let isNew = false;
  if (idx === -1) {
    const trackObj = {
      id: 'tr_' + Math.random().toString(36).substring(2, 8),
      trackName,
      audioFile: filePath,
      addedBy: userName,
      thumbnail: thumbnail || null
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
    room.thumbnail = thumbnail || (room.playlist[idx] && room.playlist[idx].thumbnail) || null;
    room.isPlaying = false;
    room.position = 0;
    room.positionHistory = [];
    room.serverTimeAtUpdate = Date.now();
  }

  return { idx, isNew, activated };
}

function notifyTrackAdded(roomId, room, trackName, filePath, userName, thumbnail = null) {
  const { activated } = addTrackToRoom(room, trackName, filePath, userName, thumbnail);

  if (activated) {
    io.to(roomId).emit('track-loaded', {
      trackName,
      thumbnail: room.thumbnail,
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

// Clean up old temporary files in uploads and cache directories (only files older than 24 hours)
function cleanOrphanFiles() {
  const activeFiles = new Set();
  rooms.forEach(room => {
    room.playlist.forEach(track => {
      if (track.audioFile) {
        activeFiles.add(path.resolve(track.audioFile));
      }
    });
  });

  const now = Date.now();
  const MAX_CACHE_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours
  const MAX_UPLOAD_AGE_MS = 6 * 60 * 60 * 1000;  // 6 hours

  [CACHE, UPLOADS].forEach(dir => {
    if (!fs.existsSync(dir)) return;
    try {
      const files = fs.readdirSync(dir);
      files.forEach(file => {
        const filePath = path.join(dir, file);
        const resolvedPath = path.resolve(filePath);
        if (activeFiles.has(resolvedPath)) return; // Keep files in active use

        try {
          const stats = fs.statSync(filePath);
          const age = now - stats.mtimeMs;
          const maxAge = dir === CACHE ? MAX_CACHE_AGE_MS : MAX_UPLOAD_AGE_MS;
          if (age > maxAge) {
            fs.unlinkSync(filePath);
            console.log(`[Cleanup] Deleted expired file (${Math.round(age / 3600000)}h old): ${file}`);
          }
        } catch (_) { }
      });
    } catch (err) {
      console.error('[Cleanup Error]', err.message);
    }
  });
}

// Run orphan cleanup every 1 hour (do not wipe active cache on boot)
setInterval(cleanOrphanFiles, 60 * 60 * 1000);

const TITLES_CACHE_FILE = path.join(CACHE, 'titles.json');
const titleCache = new Map();
try {
  if (fs.existsSync(TITLES_CACHE_FILE)) {
    const data = JSON.parse(fs.readFileSync(TITLES_CACHE_FILE, 'utf8'));
    Object.entries(data).forEach(([k, v]) => titleCache.set(k, v));
  }
} catch (_) {}

function saveTitleCache() {
  try {
    const obj = Object.fromEntries(titleCache);
    fs.writeFileSync(TITLES_CACHE_FILE, JSON.stringify(obj), 'utf8');
  } catch (_) {}
}

function getAudioUrl(roomId, room) {
  const track = room.playlist[room.currentTrackIndex];
  const trackId = track?.id || (room.audioFile ? path.basename(room.audioFile) : 'audio');
  return `/audio/${roomId}?idx=${room.currentTrackIndex}&t=${encodeURIComponent(trackId)}`;
}

function genId() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

// ─── YouTube helpers ──────────────────────────────────────────────────────────
const YOUTUBE_ID_REGEX = /^[a-zA-Z0-9_-]{11}$/;

function extractVideoId(input) {
  if (!input || typeof input !== 'string') return null;
  input = input.trim();
  if (YOUTUBE_ID_REGEX.test(input)) return input;

  let urlStr = input;
  if (!/^https?:\/\//i.test(urlStr)) {
    urlStr = 'https://' + urlStr;
  }

  try {
    const u = new URL(urlStr);
    let candidate = null;
    if (u.hostname.includes('youtu.be')) {
      candidate = u.pathname.slice(1).split('/')[0].split('?')[0];
    } else if (u.pathname.startsWith('/shorts/')) {
      candidate = u.pathname.split('/shorts/')[1].split('/')[0].split('?')[0];
    } else if (u.pathname.startsWith('/live/')) {
      candidate = u.pathname.split('/live/')[1].split('/')[0].split('?')[0];
    } else if (u.pathname.startsWith('/embed/')) {
      candidate = u.pathname.split('/embed/')[1].split('/')[0].split('?')[0];
    } else {
      candidate = u.searchParams.get('v');
    }
    if (candidate && YOUTUBE_ID_REGEX.test(candidate)) {
      return candidate;
    }
  } catch { }
  return null;
}

let FFMPEG_BIN = 'ffmpeg';
try {
  const localFfmpeg = path.join(__dirname, 'ffmpeg');
  if (fs.existsSync(localFfmpeg)) {
    fs.accessSync(localFfmpeg, fs.constants.X_OK);
    FFMPEG_BIN = localFfmpeg;
  }
} catch (_) {
  FFMPEG_BIN = 'ffmpeg';
}

// Compress large audio files (e.g. WAV, FLAC, or high-bitrate MP3) to lightweight 96k MP3 for fast mobile streaming
async function compressAudioIfLarge(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return filePath;
  try {
    const stats = fs.statSync(filePath);
    // If file is already smaller than 2.5 MB, don't re-encode
    if (stats.size <= 2.5 * 1024 * 1024) return filePath;

    const outPath = filePath.replace(/\.[^/.]+$/, '') + '_opt.mp3';
    return await new Promise(resolve => {
      const proc = spawn(FFMPEG_BIN, [
        '-y',
        '-i', filePath,
        '-vn',
        '-codec:a', 'libmp3lame',
        '-b:a', '96k',
        '-ar', '44100',
        outPath
      ]);

      const timer = setTimeout(() => {
        try { proc.kill('SIGKILL'); } catch (_) {}
        resolve(filePath);
      }, 25000);

      proc.on('error', () => {
        clearTimeout(timer);
        resolve(filePath);
      });

      proc.on('close', code => {
        clearTimeout(timer);
        if (code === 0 && fs.existsSync(outPath) && fs.statSync(outPath).size > 10000) {
          const optSize = fs.statSync(outPath).size;
          console.log(`[Audio Compression] ${path.basename(filePath)} (${(stats.size/1024/1024).toFixed(1)}MB) → ${(optSize/1024/1024).toFixed(1)}MB (-${Math.round((1 - optSize/stats.size)*100)}%)`);
          try { fs.unlinkSync(filePath); } catch (_) {}
          resolve(outPath);
        } else {
          resolve(filePath);
        }
      });
    });
  } catch (err) {
    console.warn('[Audio Compression] Skipped due to:', err.message);
    return filePath;
  }
}

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

// Sync cookies from environment variable if provided (useful for Render/Railway/Docker deployments)
if (process.env.YOUTUBE_COOKIES && process.env.YOUTUBE_COOKIES.trim()) {
  try {
    const rawVal = process.env.YOUTUBE_COOKIES.trim();
    const cookieData = rawVal.startsWith('ey') || (rawVal.length > 50 && !rawVal.includes('\n') && rawVal.includes('='))
      ? Buffer.from(rawVal, 'base64').toString('utf-8')
      : rawVal;
    fs.writeFileSync(COOKIES_FILE, cookieData, 'utf-8');
    console.log('[Cookies] Successfully synced YouTube cookies from YOUTUBE_COOKIES environment variable.');
  } catch (e) {
    console.warn('[Cookies] Failed to write YOUTUBE_COOKIES to cookies.txt:', e.message);
  }
}

// High-res direct converter pipeline for YouTube tracks (Bypasses bot-checks and datacenter blocks)
async function downloadViaConverterAPI(videoId, roomId) {
  console.log(`[Converter API] Starting fallback conversion for ${videoId}`);
  io.to(roomId).emit('download-progress', { percent: 15, status: 'connecting' });

  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*'
  };

  const startRes = await fetch(`https://loader.to/ajax/download.php?format=mp3&url=https://www.youtube.com/watch?v=${videoId}`, {
    headers,
    signal: AbortSignal.timeout(15000)
  });
  if (!startRes.ok) throw new Error(`Converter service returned HTTP ${startRes.status}`);

  const startData = await startRes.json();
  if (!startData.success || !startData.progress_url) throw new Error('Converter service could not initiate stream');

  const progressUrl = startData.progress_url;
  let downloadUrl = null;
  const title = startData.title || startData.info?.title || videoId;

  // Poll for completion (up to 30 attempts, 1.5s interval = max ~45s)
  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 1500));
    io.to(roomId).emit('download-progress', {
      percent: Math.min(25 + i * 2, 85),
      status: 'converting'
    });

    const pRes = await fetch(progressUrl, { headers, signal: AbortSignal.timeout(8000) }).catch(() => null);
    if (!pRes || !pRes.ok) continue;

    const pData = await pRes.json().catch(() => null);
    if (pData && (pData.success === 1 || pData.success === true || pData.download_url) && pData.download_url) {
      downloadUrl = pData.download_url;
      break;
    }
  }

  if (!downloadUrl) throw new Error('Converter timed out waiting for audio conversion');

  console.log(`[Converter API] Downloading audio stream for ${videoId}...`);
  io.to(roomId).emit('download-progress', { percent: 90, status: 'downloading' });

  const audioRes = await fetch(downloadUrl, { headers, signal: AbortSignal.timeout(60000) });
  if (!audioRes.ok) throw new Error(`Audio download failed with HTTP ${audioRes.status}`);

  const outPath = path.join(CACHE, `${videoId}.mp3`);
  const fileStream = fs.createWriteStream(outPath);

  // Pipe web stream to file stream with proper backpressure and error handling
  await pipeline(Readable.fromWeb(audioRes.body), fileStream);

  io.to(roomId).emit('download-progress', { percent: 100, status: 'done' });
  console.log(`[Converter API] Successfully saved ${videoId}.mp3 (${fs.statSync(outPath).size} bytes)`);
  return { filePath: outPath, title };
}

// Download audio via yt-dlp with multi-client & cloud converter fallback pipeline
async function downloadAudio(videoId, roomId) {
  const browserCookies = process.env.YTDLP_COOKIES_BROWSER;
  const hasCookiesFile = fs.existsSync(COOKIES_FILE);

  // Streamlined 2-step high-speed waterfall (drops hanging 7-attempt cascade)
  const attempts = [
    { client: 'android,ios', useCookies: hasCookiesFile, fromBrowser: browserCookies },
    { client: 'mweb,web', useCookies: false, fromBrowser: false }
  ];

  let lastError = null;

  for (let i = 0; i < attempts.length; i++) {
    const { client, useCookies, fromBrowser } = attempts[i];
    try {
      console.log(`[yt-dlp] Attempt ${i + 1}/${attempts.length} for ${videoId} (client: ${client}, cookies: ${!!useCookies})`);
      return await executeYtdlp(videoId, roomId, client, useCookies, fromBrowser);
    } catch (err) {
      console.warn(`[yt-dlp] Attempt ${i + 1} failed for ${videoId}: ${err.message}`);
      lastError = err;
    }
  }

  // Fast-fail fallback to Cloud Converter API
  console.log(`[downloadAudio] yt-dlp attempts failed for ${videoId}. Engaging Cloud Converter API...`);
  try {
    return await downloadViaConverterAPI(videoId, roomId);
  } catch (converterErr) {
    console.error(`[downloadAudio] Cloud Converter fallback failed: ${converterErr.message}`);
    throw new Error(converterErr.message || (lastError ? lastError.message : 'Failed to download YouTube audio.'));
  }
}

function executeYtdlp(videoId, roomId, client, useCookies, fromBrowser) {
  return new Promise((resolve, reject) => {
    const outTemplate = path.join(CACHE, `${videoId}.%(ext)s`);
    const args = [
      '--no-playlist',
      '--force-overwrites',
      '--no-continue',
      '--no-check-certificates',
      '--prefer-free-formats',
      '--concurrent-fragments', '5', // 5-thread parallel chunk download
      '--socket-timeout', '8',        // 8s timeout against stalls
      '--retries', '1',
      '--buffer-size', '64K',
      '--http-chunk-size', '10M',
      '--js-runtimes', `node:${process.execPath || '/usr/bin/node'}`
    ];

    if (client && client !== 'default') {
      args.push('--extractor-args', `youtube:player_client=${client}`);
    }

    if (fromBrowser) {
      args.push('--cookies-from-browser', fromBrowser);
    } else if (useCookies && fs.existsSync(COOKIES_FILE)) {
      args.push('--cookies', COOKIES_FILE);
    }

    if (fs.existsSync(FFMPEG_BIN)) {
      args.push('--ffmpeg-location', FFMPEG_BIN);
    }

    args.push(
      '-f', 'ba[abr<=128]/ba[ext=m4a]/ba/b', // Prioritize 96k-128k Opus/M4A: reduces payload by 75% for slow networks while preserving studio audio fidelity
      '--output', outTemplate,
      '--newline',
      '--no-simulate',
      '--print', 'before_dl:title',
      '--',
      `https://www.youtube.com/watch?v=${videoId}`
    );

    const proc = spawn('yt-dlp', args);
    let title = videoId;
    let titleCaptured = false;
    let lastStderr = '';

    // Hard kill if stalled longer than 22 seconds
    const killTimer = setTimeout(() => {
      try { proc.kill('SIGKILL'); } catch (_) {}
      reject(new Error('yt-dlp download timed out after 22s'));
    }, 22000);

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
      clearTimeout(killTimer);
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
        if (title && title !== videoId) {
          titleCache.set(videoId, title);
          saveTitleCache();
        }
        io.to(roomId).emit('download-progress', { percent: 100, status: 'done' });
        resolve({ filePath: found, title });
      } else {
        reject(new Error('Output file not found after download'));
      }
    });

    proc.on('error', err => {
      clearTimeout(killTimer);
      if (err.code === 'ENOENT') reject(new Error('yt-dlp not found. Please install it.'));
      else reject(err);
    });
  });
}

// ─── HTTP Routes ──────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Health check endpoints for Railway / Render container monitors
app.get(['/health', '/healthz'], (_, res) => res.status(200).send('OK'));
app.get('/ping', (_, res) => res.status(200).json({ status: 'ok', serverTime: Date.now() }));

app.get(['/api/room/:id', '/room/:id'], (req, res, next) => {
  const acceptsHtml = req.accepts(['json', 'html']) === 'html';
  if (req.path.startsWith('/room/') && acceptsHtml && !req.xhr && !req.headers['x-requested-with']) {
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  }
  const id = (req.params.id || '').toUpperCase();
  const room = rooms.get(id);
  if (!room) return res.status(404).json({ error: 'Room not found' });
  res.json({ exists: true, userCount: room.users.size, maxSize: MAX_ROOM });
});

// ─── Instant YouTube Search API ───────────────────────────────────────────────
const searchCache = new Map();

app.get('/api/search', async (req, res) => {
  const query = (req.query.q || '').trim();
  if (!query || query.length < 2) {
    return res.json({ results: [] });
  }

  const cacheKey = query.toLowerCase();
  if (searchCache.has(cacheKey)) {
    return res.json({ results: searchCache.get(cacheKey) });
  }

  try {
    const args = [
      '--no-playlist',
      '--flat-playlist',
      '--no-check-certificates',
      '--socket-timeout', '6',
      '--print', '%(id)s\t%(title)s\t%(duration_string)s',
      '--',
      `ytsearch4:${query}`
    ];

    const proc = spawn('yt-dlp', args);
    let output = '';

    const killTimer = setTimeout(() => {
      try { proc.kill('SIGKILL'); } catch (_) {}
    }, 7000);

    proc.stdout.on('data', chunk => { output += chunk.toString(); });

    proc.on('close', () => {
      clearTimeout(killTimer);
      const lines = output.split('\n').filter(Boolean);
      const results = [];

      for (const line of lines) {
        const parts = line.split('\t');
        if (parts.length >= 2) {
          const id = parts[0].trim();
          const title = parts[1].trim();
          const duration = parts[2] ? parts[2].trim() : '';
          if (id && /^[a-zA-Z0-9_-]{11}$/.test(id)) {
            results.push({
              id,
              title,
              duration,
              url: `https://www.youtube.com/watch?v=${id}`,
              thumbnail: `https://i.ytimg.com/vi/${id}/mqdefault.jpg`
            });
          }
        }
      }

      if (results.length > 0) {
        if (searchCache.size > 60) {
          const oldestKey = searchCache.keys().next().value;
          searchCache.delete(oldestKey);
        }
        searchCache.set(cacheKey, results);
      }

      res.json({ results });
    });

    proc.on('error', () => {
      clearTimeout(killTimer);
      res.json({ results: [] });
    });
  } catch (_) {
    res.json({ results: [] });
  }
});

async function getVideoTitle(videoId) {
  if (titleCache.has(videoId)) {
    return titleCache.get(videoId);
  }

  try {
    const res = await fetch(`https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`, {
      signal: AbortSignal.timeout(2500)
    });
    if (res.ok) {
      const data = await res.json();
      if (data.title) {
        titleCache.set(videoId, data.title);
        saveTitleCache();
        return data.title;
      }
    }
  } catch (_) {}

  return new Promise(resolve => {
    const execArgs = [
      '--no-playlist',
      '--no-check-certificates',
      '--socket-timeout', '4',
      '--retries', '1',
      '--no-download',
      '--get-title'
    ];
    if (fs.existsSync(COOKIES_FILE)) {
      execArgs.push('--cookies', COOKIES_FILE);
    }
    if (fs.existsSync(FFMPEG_BIN)) {
      execArgs.push('--ffmpeg-location', FFMPEG_BIN);
    }
    execArgs.push('--', `https://www.youtube.com/watch?v=${videoId}`);

    execFile('yt-dlp', execArgs, { timeout: 4500 }, (err, stdout) => {
      const title = (stdout || '').trim();
      const resolved = title || videoId;
      if (title) {
        titleCache.set(videoId, title);
        saveTitleCache();
      }
      resolve(resolved);
    });
  });
}

const ROOM_ID_VALIDATOR = /^[A-Z0-9]{4,10}$/;

// ── YouTube download endpoint ─────────────────────────────────────────────────
app.post('/download/:roomId', async (req, res) => {
  const roomId = (req.params.roomId || '').toUpperCase();
  if (!ROOM_ID_VALIDATOR.test(roomId)) return res.status(400).json({ error: 'Invalid room ID' });

  const room = rooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Room not found' });
  if (room.downloading) return res.status(409).json({ error: 'Already downloading' });

  const { url, userName: bodyUserName } = req.body || {};
  let headerUserName = req.headers['x-user-name'];
  if (headerUserName) {
    try { headerUserName = decodeURIComponent(headerUserName); } catch (_) {}
  }
  const userName = bodyUserName || headerUserName || 'Host';

  const videoId = extractVideoId(url || '');
  if (!videoId) return res.status(400).json({ error: 'Invalid YouTube URL or Video ID' });

  const thumbnail = `https://i.ytimg.com/vi/${videoId}/mqdefault.jpg`;

  // Respond immediately — progress comes via socket
  res.json({ success: true, videoId, thumbnail });

  // Cache hit — serve instantly
  const cachedFile = findCached(videoId);
  if (cachedFile) {
    console.log(`[Cache HIT] ${videoId} → ${cachedFile}`);
    io.to(roomId).emit('download-progress', { percent: 100, status: 'done' });
    const trackName = await getVideoTitle(videoId);
    notifyTrackAdded(roomId, room, trackName, cachedFile, userName, thumbnail);
    return;
  }

  // Cache miss — download
  console.log(`[Cache MISS] Downloading ${videoId} for room ${roomId}`);
  room.downloading = true;
  io.to(roomId).emit('download-start', { videoId, thumbnail });

  try {
    const { filePath, title } = await downloadAudio(videoId, roomId);

    // Fetch clean title if yt-dlp didn't provide one
    let trackName = title !== videoId ? title : videoId;
    notifyTrackAdded(roomId, room, trackName, filePath, userName, thumbnail);
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
  const roomId = (req.params.roomId || '').toUpperCase();
  if (!ROOM_ID_VALIDATOR.test(roomId)) return res.status(400).json({ error: 'Invalid room ID' });

  req.setTimeout(300000); // 5 minutes for mobile uploads
  res.setTimeout(300000);

  const room = rooms.get(roomId);
  const userToken = req.headers['x-user-token'] || req.query.token;
  let rawName = req.headers['x-user-name'] || req.query.userName || 'Host';
  if (rawName) {
    try { rawName = decodeURIComponent(rawName); } catch (_) {}
  }

  // Mark user as actively uploading so they are protected from disconnect / host-transfer
  let uploadingUser = null;
  if (room) {
    if (userToken) uploadingUser = room.users.get(userToken);
    if (!uploadingUser) {
      for (const u of room.users.values()) {
        if (u.name === rawName || (u.isHost && !uploadingUser)) uploadingUser = u;
      }
    }
    if (uploadingUser) {
      uploadingUser.isUploading = true;
      if (uploadingUser.disconnectTimer) {
        clearTimeout(uploadingUser.disconnectTimer);
        uploadingUser.disconnectTimer = null;
      }
    }
  }

  upload.single('audio')(req, res, err => {
    if (uploadingUser) uploadingUser.isUploading = false;

    if (err) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'File is too large. Max 50 MB.' });
      }
      if (err.message === 'Request aborted' || err.code === 'ECONNRESET') {
        console.log(`[Upload] Client aborted upload for room ${roomId}`);
        return;
      }
      return res.status(400).json({ error: err.message || 'Upload failed' });
    }

    if (!room) return res.status(404).json({ error: 'Room not found' });
    if (!req.file) return res.status(400).json({ error: 'No valid audio file received' });

    const finalUserName = (req.body && req.body.userName) || rawName || 'Host';
    const trackName = req.file.originalname.replace(/\.[^/.]+$/, '');

    // Optimize large uploads for fast mobile downloading on slow networks
    compressAudioIfLarge(req.file.path).then(finalPath => {
      notifyTrackAdded(roomId, room, trackName, finalPath, finalUserName);
      console.log(`[Room ${roomId}] File ready: ${trackName}`);
      res.json({ success: true, trackName });
    }).catch(compErr => {
      console.warn(`[Room ${roomId}] Compression fallback:`, compErr.message);
      notifyTrackAdded(roomId, room, trackName, req.file.path, finalUserName);
      res.json({ success: true, trackName });
    });
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
    if (room.playlist[idx] && room.playlist[idx].audioFile && fs.existsSync(room.playlist[idx].audioFile)) {
      filePath = room.playlist[idx].audioFile;
    }
  }

  // If path is missing or doesn't exist on disk, attempt fallback resolution from CACHE
  if (!filePath || !fs.existsSync(filePath)) {
    if (room.trackName) {
      const vid = extractVideoId(room.trackName) || extractVideoId(room.audioFile || '');
      if (vid) {
        const cached = findCached(vid);
        if (cached) filePath = cached;
      }
    }
  }

  if (!filePath || !fs.existsSync(filePath)) {
    console.warn(`[Audio 404] File not found for room ${roomId}: ${filePath}`);
    return res.status(404).json({ error: 'No audio available' });
  }

  const stats = fs.statSync(filePath);
  const fileSize = stats.size;
  const range = req.headers.range;
  const etag = `"${fileSize.toString(16)}-${Math.floor(stats.mtimeMs).toString(16)}"`;

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

  res.setHeader('ETag', etag);
  res.setHeader('Cache-Control', 'public, max-age=86400'); // Cache for 24 hours

  if (req.headers['if-none-match'] === etag) {
    return res.status(304).end();
  }

  if (range) {
    const [startStr, endStr] = range.replace(/bytes=/, '').split('-');
    const start = parseInt(startStr, 10);
    const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': end - start + 1,
      'Content-Type': contentType,
      'Cache-Control': 'public, max-age=86400',
    });
    fs.createReadStream(filePath, { start, end }).pipe(res);
  } else {
    res.writeHead(200, {
      'Content-Length': fileSize,
      'Content-Type': contentType,
      'Accept-Ranges': 'bytes',
      'Cache-Control': 'public, max-age=86400',
    });
    fs.createReadStream(filePath).pipe(res);
  }
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log(`[Socket] +${socket.id}`);

  socket.on('ping', () => socket.emit('pong', { serverTime: Date.now() }));
  socket.on('ping-clock', cb => { if (typeof cb === 'function') cb({ serverTime: Date.now() }); });

  // Live floating emoji reaction burst
  socket.on('send-reaction', ({ roomId, emoji, userName }) => {
    if (!roomId || !emoji) return;
    const safeEmoji = String(emoji).slice(0, 10);
    const safeName = String(userName || 'Someone').slice(0, 25);
    io.to(roomId.toUpperCase()).emit('reaction', {
      emoji: safeEmoji,
      userName: safeName,
      userId: socket.id,
      timestamp: Date.now()
    });
  });

  // Create room
  socket.on('create-room', ({ userName, userToken }, cb) => {
    const id = genId();
    const token = userToken || socket.id;
    const cleanName = sanitizeUserName(userName, 'Host');
    const room = makeRoom(id, socket.id, cleanName, token);
    rooms.set(id, room);
    socket.join(id);
    socket.data.roomId = id;
    socket.data.userToken = token;
    console.log(`[Room ${id}] Created by ${cleanName} (token: ${token})`);
    if (typeof cb === 'function') {
      cb({ success: true, isHost: true, room: publicRoom(room) });
    }
  });

  // Join or Rejoin room
  socket.on('join-room', ({ roomId, userName, userToken }, cb) => {
    if (!roomId) return typeof cb === 'function' && cb({ error: 'Room ID required' });
    const id = roomId.toUpperCase();
    const room = rooms.get(id);
    if (!room) return typeof cb === 'function' && cb({ error: 'Room not found' });

    const token = userToken || socket.id;
    const cleanName = sanitizeUserName(userName, 'Guest');
    let user = room.users.get(token);

    // 1. If not found by map key, look up by stored userToken
    if (!user) {
      for (const [k, u] of room.users.entries()) {
        if (u.userToken === token || k === token) {
          user = u;
          if (k !== token) {
            room.users.delete(k);
            room.users.set(token, user);
          }
          break;
        }
      }
    }

    // 2. If still not found, check if there is an offline user with the same name to reclaim
    if (!user && cleanName) {
      for (const [k, u] of room.users.entries()) {
        if (u.offline && u.name.trim().toLowerCase() === cleanName.toLowerCase()) {
          user = u;
          room.users.delete(k);
          room.users.set(token, user);
          break;
        }
      }
    }

    if (user) {
      // Rejoining existing session!
      if (user.disconnectTimer) {
        clearTimeout(user.disconnectTimer);
        user.disconnectTimer = null;
      }
      user.socketId = socket.id;
      user.userToken = token;
      user.offline = false;
      user.isUploading = false;
      if (cleanName) user.name = cleanName;

      // If user was creator or host, or only online user, restore host status!
      if (token === room.creatorToken || token === room.hostToken || user.isHost || [...room.users.values()].filter(u => !u.offline).length <= 1) {
        if (token === room.creatorToken) {
          for (const u of room.users.values()) {
            if (u !== user) u.isHost = false;
          }
        }
        user.isHost = true;
        room.hostId = socket.id;
        room.hostToken = token;
      }

      socket.join(id);
      socket.data.roomId = id;
      socket.data.userToken = token;

      io.to(id).emit('user-status-changed', {
        users: publicRoom(room).users,
        reconnectedName: user.name
      });
      console.log(`[Room ${id}] ${user.name} reconnected (token: ${token}, socket: ${socket.id}, isHost: ${user.isHost})`);
    } else {
      // New user joining
      if (room.users.size >= MAX_ROOM) return typeof cb === 'function' && cb({ error: 'Room is full (max 4)' });

      user = { name: cleanName, isHost: false, socketId: socket.id, userToken: token, offline: false, disconnectTimer: null };
      room.users.set(token, user);
      socket.join(id);
      socket.data.roomId = id;
      socket.data.userToken = token;

      socket.to(id).emit('user-joined', {
        userId: socket.id, name: cleanName,
        users: publicRoom(room).users
      });
      console.log(`[Room ${id}] ${cleanName} joined`);
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

    if (typeof cb === 'function') {
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
    }
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
    room.lastHostPulseTime = now;

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

  // Playlist track selection (Host or Creator)
  socket.on('select-track', ({ index, autoPlay }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room || index < 0 || index >= room.playlist.length) return;

    const token = socket.data.userToken;
    const isHostOrCreator = (socket.id === room.hostId || token === room.hostToken || token === room.creatorToken);
    const onlineUsers = [...room.users.values()].filter(u => !u.offline);
    const isSolo = onlineUsers.length <= 1;

    if (!isHostOrCreator && !isSolo) {
      return socket.emit('error-msg', { message: 'Only the host can switch songs' });
    }

    // If creator is switching, ensure creator has host role
    if (token === room.creatorToken && !room.users.get(token)?.isHost) {
      for (const u of room.users.values()) u.isHost = (u.userToken === token);
      room.hostId = socket.id;
      room.hostToken = token;
      io.to(room.id).emit('user-status-changed', { users: publicRoom(room).users });
    }

    const now = Date.now();
    const track = room.playlist[index];

    room.currentTrackIndex = index;
    room.audioFile = track.audioFile;
    room.trackName = track.trackName;
    room.thumbnail = track.thumbnail || null;
    room.isPlaying = false;
    room.position = 0;
    room.positionHistory = [];
    room.serverTimeAtUpdate = now;
    delete room.playAt;

    io.to(room.id).emit('track-loaded', {
      trackName: track.trackName,
      thumbnail: room.thumbnail,
      audioUrl: getAudioUrl(room.id, room),
      playlist: room.playlist,
      currentTrackIndex: room.currentTrackIndex,
      autoPlay: false,
      playAt: null
    });
    console.log(`[Room ${room.id}] Switched to track #${index}: ${track.trackName} (ready, paused)`);
  });

  // Transfer host privileges to another user in the room (Host only)
  socket.on('make-host', ({ targetUserToken }, cb) => {
    const room = rooms.get(socket.data.roomId);
    if (!room) return;

    if (socket.id !== room.hostId && socket.data.userToken !== room.hostToken) {
      return socket.emit('error-msg', { message: 'Only the current host can transfer host permissions' });
    }

    const newHost = room.users.get(targetUserToken);
    if (!newHost || newHost.offline) {
      return socket.emit('error-msg', { message: 'Target user is not available in room' });
    }

    // Reset host status for all users in room
    room.users.forEach(u => u.isHost = false);
    newHost.isHost = true;
    room.hostId = newHost.socketId;
    room.hostToken = targetUserToken;

    io.to(room.id).emit('user-status-changed', {
      users: publicRoom(room).users,
      newHostName: newHost.name,
      newHostToken: targetUserToken
    });

    console.log(`[Room ${room.id}] Host transferred to ${newHost.name} (token: ${targetUserToken})`);
    if (typeof cb === 'function') cb({ success: true });
  });

  // Playlist track deletion (Host only)
  socket.on('remove-track', ({ index }) => {
    const room = rooms.get(socket.data.roomId);
    if (!room || index < 0 || index >= room.playlist.length) return;

    if (socket.id !== room.hostId && socket.data.userToken !== room.hostToken) {
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
      room.thumbnail = null;
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
      room.thumbnail = nextTrack.thumbnail || null;
      room.isPlaying = false;
      room.position = 0;
      room.positionHistory = [];
      room.serverTimeAtUpdate = Date.now();

      io.to(room.id).emit('track-loaded', {
        trackName: nextTrack.trackName,
        thumbnail: room.thumbnail,
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
        if (u.socketId === socket.id || u.userToken === userToken) {
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
      if (nextOnlineUser) {
        nextOnlineUser.isHost = true;
        room.hostId = nextOnlineUser.socketId;
        for (const [t, u] of room.users.entries()) {
          if (u === nextOnlineUser) { room.hostToken = t; break; }
        }
        newHostId = nextOnlineUser.socketId;
      }
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

  // Disconnect with grace window for reconnection
  socket.on('disconnect', (reason) => {
    const roomId = socket.data.roomId;
    const userToken = socket.data.userToken;
    console.log(`[Socket] -${socket.id} disconnected (${reason})`);
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    let userKey = userToken;
    let user = room.users.get(userKey);
    if (!user) {
      for (const [k, u] of room.users.entries()) {
        if (u.socketId === socket.id || u.userToken === userToken) {
          userKey = k;
          user = u;
          break;
        }
      }
    }
    if (!user) return;

    // CRITICAL: If user is actively uploading a file, do not mark offline or start removal
    if (user.isUploading) {
      console.log(`[Room ${roomId}] User ${user.name} socket dropped during active upload — preserving active session.`);
      return;
    }

    user.offline = true;
    console.log(`[Room ${roomId}] ${user.name} went offline (${RECONNECT_GRACE_MS / 1000}s grace window started)`);

    io.to(roomId).emit('user-status-changed', {
      users: publicRoom(room).users
    });

    if (user.disconnectTimer) clearTimeout(user.disconnectTimer);

    // Grace period before officially removing user / reassigning host
    user.disconnectTimer = setTimeout(() => {
      // Re-verify room and user status before taking destructive action
      const curRoom = rooms.get(roomId);
      if (!curRoom) return;
      const curUser = curRoom.users.get(userKey) || [...curRoom.users.values()].find(u => u.userToken === user.userToken);

      // If user is no longer offline or is actively uploading, do nothing!
      if (!curUser || !curUser.offline || curUser.isUploading) {
        console.log(`[Room ${roomId}] User ${user.name} is active/uploading/reconnected — canceling removal.`);
        return;
      }

      const wasHost = curUser.isHost;
      curRoom.users.delete(userKey);
      console.log(`[Room ${roomId}] ${curUser.name} grace period expired -> removed`);

      const remainingUsers = [...curRoom.users.values()];
      if (remainingUsers.length === 0) {
        cleanupRoom(roomId);
        return;
      }

      let newHostId = curRoom.hostId;
      if (wasHost) {
        const nextOnlineUser = remainingUsers.find(u => !u.offline) || remainingUsers[0];
        if (nextOnlineUser) {
          nextOnlineUser.isHost = true;
          curRoom.hostId = nextOnlineUser.socketId;
          curRoom.hostToken = nextOnlineUser.userToken;
          newHostId = nextOnlineUser.socketId;
        }
      }

      io.to(roomId).emit('user-left', {
        userId: curUser.socketId || socket.id,
        name: curUser.name,
        users: publicRoom(curRoom).users,
        newHostId
      });
    }, RECONNECT_GRACE_MS);
  });
});

// ─── Continuous Room Sync Pulse (2.5-second fallback heartbeat) ───────────────
setInterval(() => {
  const now = Date.now();
  rooms.forEach(room => {
    // If room is playing, has users, and host is not actively sending fresh pulses
    if (room.isPlaying && room.users.size > 0) {
      if (now - (room.lastHostPulseTime || 0) < 4000) {
        return; // Suppress duplicate heartbeat when host pulse is active
      }
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
}, 2500);

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
server.timeout = 300000; // 5 minutes
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

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
  console.log(`   ➜ Healthcheck: http://localhost:${PORT}/health\n`);
});

// Graceful container termination for Railway / Docker
process.on('SIGTERM', () => {
  console.log('[SyncPlay] SIGTERM received. Closing HTTP server gracefully...');
  server.close(() => process.exit(0));
});
process.on('SIGINT', () => {
  console.log('[SyncPlay] SIGINT received. Closing HTTP server gracefully...');
  server.close(() => process.exit(0));
});

