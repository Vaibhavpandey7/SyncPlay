/* ═══════════════════════════════════════════════════════════
   SyncPlay — app.js  v2
   NTP sync · Socket.io · YouTube download · HTML5 Audio
═══════════════════════════════════════════════════════════ */
'use strict';

// ─── State ────────────────────────────────────────────────────────────────────
const state = {
  socket: null,
  myName: '',
  roomId: null,
  isHost: false,
  clockOffset: 0,
  audioOffsetMs: parseInt(localStorage.getItem('syncplay_audio_offset') || '0', 10),    // user-set device speaker latency compensation (ms)
  volume: parseFloat(localStorage.getItem('syncplay_volume') || '1.0'),
  isMuted: false,
  previousVolume: 1.0,
  currentThumbnail: null,
  roomIsPlaying: false, // server-authoritative play state — single source of truth for button
  duration: 0,
  isSeeking: false,
  progressInterval: null,
  driftInterval: null,
  playlist: [],
  currentTrackIndex: -1,
  offsetHistory: [], // Rolling 5-sample buffer for network jitter smoothing
  sync: {
    startPosition: 0,
    startLocalTime: 0,
    active: false,
    playTimeout: null,
  }
};

// ─── WebAudio API Engine ───────────────────────────────────────────────────────
class WebAudioSyncEngine {
  constructor() {
    this.ctx = null;
    this.buffer = null;
    this.source = null;
    this.gainNode = null;
    this.analyser = null;
    this.bufferCache = new Map();     // In-memory decoded AudioBuffer cache (0ms song switching)
    this.preloadPromises = new Map(); // In-flight preload promises
    this.startTime = 0;       // AudioContext.currentTime when playback started
    this.startPosition = 0;   // Song position (seconds) when started
    this.isPlaying = false;
    this.duration = 0;
    this.onEnded = null;
    this.volume = !isNaN(state.volume) ? Math.max(0, Math.min(1, state.volume)) : 1.0;
    this.pendingPlay = null;  // Queued play request while buffer is fetching/decoding
    this.isUnlocked = false;
  }

  init() {
    if (!this.ctx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      this.ctx = new AudioCtx();
      this.gainNode = this.ctx.createGain();
      this.gainNode.gain.value = this.volume;
      this.analyser = this.ctx.createAnalyser();
      this.analyser.fftSize = 64;
      this.analyser.smoothingTimeConstant = 0.8;
      this.gainNode.connect(this.analyser);
      this.analyser.connect(this.ctx.destination);
    }
    if (this.ctx.state === 'suspended') {
      this.ctx.resume().catch(() => {});
    }
  }

  getFrequencyData() {
    if (!this.analyser) return null;
    const data = new Uint8Array(this.analyser.frequencyBinCount);
    this.analyser.getByteFrequencyData(data);
    return data;
  }

  unlock() {
    this.init();
    if (!this.ctx) return;
    if (this.ctx.state === 'suspended') {
      this.ctx.resume().then(() => {
        this.isUnlocked = true;
        this.updateUnlockUI();
        if (state.roomIsPlaying && this.buffer && !this.isPlaying) {
          const nowServer = Date.now() + state.clockOffset;
          this.playAt(this.startPosition, nowServer, state.clockOffset);
        }
      }).catch(() => {});
    } else {
      this.isUnlocked = true;
      this.updateUnlockUI();
    }

    // Play a 1-sample silent sound buffer to guarantee iOS WebKit audio hardware engagement
    try {
      const silentBuf = this.ctx.createBuffer(1, 1, 22050);
      const src = this.ctx.createBufferSource();
      src.buffer = silentBuf;
      src.connect(this.ctx.destination);
      src.start(0);
    } catch (_) {}
  }

  updateUnlockUI() {
    const banner = document.getElementById('audio-unlock-banner');
    if (!banner) return;
    const isSuspended = this.ctx && this.ctx.state === 'suspended';
    const shouldShow = state.roomIsPlaying && isSuspended;
    banner.classList.toggle('hidden', !shouldShow);
  }

  async loadTrack(url) {
    this.init();
    this.stop();
    this.buffer = null;
    this.duration = 0;

    // 1. Instant Cache Hit in Memory (0ms download, 0ms decode!)
    if (this.bufferCache.has(url)) {
      const cached = this.bufferCache.get(url);
      this.buffer = cached.buffer;
      this.duration = cached.duration;
      if (this.pendingPlay) {
        const p = this.pendingPlay;
        this.pendingPlay = null;
        this.playAt(p.position, p.playAtServerTime, p.clockOffset);
      }
      return this.duration;
    }

    // 2. Check if background pre-loading already fetched and decoded this track
    if (this.preloadPromises.has(url)) {
      try {
        const cached = await this.preloadPromises.get(url);
        if (cached && cached.buffer) {
          this.buffer = cached.buffer;
          this.duration = cached.duration;
          if (this.pendingPlay) {
            const p = this.pendingPlay;
            this.pendingPlay = null;
            this.playAt(p.position, p.playAtServerTime, p.clockOffset);
          }
          return this.duration;
        }
      } catch (_) {}
    }

    let res = null;
    let lastErr = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        res = await fetch(url);
        if (res.ok) break;
      } catch (e) {
        lastErr = e;
      }
      await new Promise(r => setTimeout(r, 400));
    }

    if (!res || !res.ok) {
      const statusText = res ? `HTTP ${res.status}` : (lastErr?.message || 'Network error');
      throw new Error(statusText);
    }

    const arrayBuf = await res.arrayBuffer();

    // Universal compatibility with older iOS WebKit callback and modern Promise
    this.buffer = await new Promise((resolve, reject) => {
      this.ctx.decodeAudioData(
        arrayBuf,
        decoded => resolve(decoded),
        err => reject(err || new Error('Audio decode error'))
      );
    });

    this.duration = this.buffer.duration;

    // Cache decoded buffer in memory (LRU max 6 tracks ~60MB RAM)
    if (this.bufferCache.size >= 6) {
      const oldestKey = this.bufferCache.keys().next().value;
      this.bufferCache.delete(oldestKey);
    }
    this.bufferCache.set(url, { buffer: this.buffer, duration: this.duration });

    // If a play command arrived while downloading/decoding, immediately trigger it!
    if (this.pendingPlay) {
      const p = this.pendingPlay;
      this.pendingPlay = null;
      this.playAt(p.position, p.playAtServerTime, p.clockOffset);
    }

    return this.duration;
  }

  // Silently pre-fetch and decode upcoming track in background for gapless playback
  preloadTrack(url) {
    if (!url || this.bufferCache.has(url) || this.preloadPromises.has(url)) return;
    const promise = (async () => {
      this.init();
      const res = await fetch(url);
      if (!res.ok) throw new Error('Preload fetch failed');
      const arrayBuf = await res.arrayBuffer();
      const buffer = await new Promise((resolve, reject) => {
        this.ctx.decodeAudioData(arrayBuf, resolve, reject);
      });
      const data = { buffer, duration: buffer.duration };
      if (this.bufferCache.size >= 6) {
        const oldestKey = this.bufferCache.keys().next().value;
        this.bufferCache.delete(oldestKey);
      }
      this.bufferCache.set(url, data);
      this.preloadPromises.delete(url);
      return data;
    })().catch(() => {
      this.preloadPromises.delete(url);
    });
    this.preloadPromises.set(url, promise);
  }

  playAt(position, playAtServerTime, clockOffset) {
    this.init();
    this.stop();
    if (!this.buffer) {
      this.pendingPlay = { position, playAtServerTime, clockOffset };
      return;
    }

    // Convert server time to local time
    const localPlayAt = playAtServerTime - clockOffset;
    const nowLocal = Date.now();
    const delaySec = (localPlayAt - nowLocal) / 1000;

    // Absolute WebAudio Context High-Precision Hardware Start Time
    const audioCtxStart = this.ctx.currentTime + Math.max(0, delaySec);
    let startPos = Math.max(0, position);

    if (delaySec < 0) {
      // Late packet arrival compensation
      startPos += Math.abs(delaySec);
    }

    if (startPos >= this.duration) return;

    this.source = this.ctx.createBufferSource();
    this.source.buffer = this.buffer;
    this.source.connect(this.gainNode);

    this.source.onended = () => {
      if (this.isPlaying) {
        this.isPlaying = false;
        if (this.onEnded) this.onEnded();
      }
    };

    this.source.start(audioCtxStart, startPos);
    this.startTime = audioCtxStart;
    this.startPosition = startPos;
    this.isPlaying = true;
    this.updateUnlockUI();
  }

  pause(position = null) {
    if (position !== null) {
      this.startPosition = position;
    } else {
      this.startPosition = this.getCurrentTime();
    }
    this.stop();
    this.updateUnlockUI();
  }

  stop() {
    if (this.source) {
      this.source.onended = null;  // ← CRITICAL: prevent stale onended from firing after stop()
      try { this.source.stop(); } catch (_) {}
      try { this.source.disconnect(); } catch (_) {}
      this.source = null;
    }
    this.isPlaying = false;
  }

  setVolume(vol) {
    this.volume = Math.max(0, Math.min(1, vol));
    if (this.gainNode) {
      this.gainNode.gain.value = this.volume;
    }
  }

  // Checkpoint: save current audio position before changing rate.
  // CRITICAL: without this, getCurrentTime() applies the new rate to the ENTIRE
  // elapsed time since source.start(), not just the portion after the rate change.
  _checkpoint() {
    if (!this.source || !this.ctx) return;
    const elapsed = this.ctx.currentTime - this.startTime;
    if (elapsed <= 0) return; // Playback has not started yet (scheduled in future)
    const currentRate = this.source.playbackRate.value;
    this.startPosition = Math.min(this.duration, this.startPosition + elapsed * currentRate);
    this.startTime = this.ctx.currentTime;
  }

  // Nudge playback speed ±1-3% to smoothly close small drifts (no audio gap/click)
  setRate(rate) {
    if (!this.source) return;
    this._checkpoint();   // ← anchor startPosition before changing rate
    this.source.playbackRate.value = Math.max(0.9, Math.min(1.1, rate));
  }

  resetRate() {
    if (!this.source) return;
    this._checkpoint();   // ← anchor startPosition before resetting rate
    this.source.playbackRate.value = 1.0;
  }

  getCurrentTime() {
    if (!this.isPlaying || !this.ctx) return this.startPosition;
    const elapsed = this.ctx.currentTime - this.startTime;
    if (elapsed < 0) return this.startPosition; // Scheduled in future
    const rate = this.source ? this.source.playbackRate.value : 1.0;
    // elapsed * rate is only accurate since the last _checkpoint() call
    return Math.min(this.duration, this.startPosition + elapsed * rate);
  }

  // Auto-detect browser/hardware speaker output pipeline latency (base + output latency in seconds)
  getHardwareLatency() {
    if (!this.ctx) return 0;
    const outLat = this.ctx.outputLatency || 0;
    const baseLat = this.ctx.baseLatency || 0;
    return outLat + baseLat;
  }
}

const audioEngine = new WebAudioSyncEngine();

// ─── DOM refs ─────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const pages = { landing: $('page-landing'), room: $('page-room') };

// Landing
const inputCreateName = $('input-create-name');
const inputJoinName   = $('input-join-name');
const inputRoomCode   = $('input-room-code');
const btnCreate       = $('btn-create');
const btnJoin         = $('btn-join');
const landingError    = $('landing-error');

// Header
const displayRoomId   = $('display-room-id');
const btnCopyId       = $('btn-copy-id');
const btnShareRoom    = $('btn-share-room');
const btnLeave        = $('btn-leave');

// Load panel
const loadPanel       = $('load-panel');
const tabYt           = $('tab-yt');
const tabFile         = $('tab-file');
const panelYt         = $('panel-yt');
const panelFile       = $('panel-file');
const inputYtUrl      = $('input-yt-url');
const btnLoadYt       = $('btn-load-yt');
const ytError         = $('yt-error');
const dropZone        = $('drop-zone');
const inputFile       = $('input-file');
const fileError       = $('file-error');
const opProgressWrap  = $('progress-wrap');
const opProgressBar   = $('op-progress-bar');
const opProgressLabel = $('op-progress-label');

// Player states
const guestWaiting    = $('guest-waiting');
const hostPrompt      = $('host-prompt');
const playerCard      = $('player-card');
const albumArtWrap    = $('album-art-wrap');
const albumArtImg     = $('album-art-img');
const artBars         = $('art-bars');
const displayTrack    = $('display-track-name');
const controlsBar     = $('controls-bar');
const btnPlayPause    = $('btn-play-pause');
const iconPlay        = $('icon-play');
const iconPause       = $('icon-pause');
const btnSeekBack     = $('btn-seek-back');
const btnSeekFwd      = $('btn-seek-fwd');
const btnMute         = $('btn-mute');
const volumeSlider    = $('volume-slider');
const scrubTrack      = $('scrub-track');
const scrubFill       = $('scrub-fill');
const displayCurrent  = $('display-current');
const displayDuration = $('display-duration');
const ctrlBadge       = $('ctrl-badge');

// Users & Playlist
const userCount       = $('user-count');
const usersList       = $('users-list');
const playlistCount   = $('playlist-count');
const playlistList    = $('playlist-list');
const syncDot         = $('sync-dot');
const syncLabel       = $('sync-label');
const toastEl         = $('toast');


// ─── Utilities ────────────────────────────────────────────────────────────────

function showPage(name) {
  Object.values(pages).forEach(p => p.classList.remove('active'));
  pages[name].classList.add('active');
}

function showToast(msg, ms = 2800) {
  toastEl.textContent = msg;
  toastEl.classList.remove('hidden');
  clearTimeout(toastEl._t);
  toastEl._t = setTimeout(() => toastEl.classList.add('hidden'), ms);
}

function showErr(el, msg) {
  el.textContent = msg;
  el.classList.remove('hidden');
  setTimeout(() => el.classList.add('hidden'), 5000);
}

function fmt(s) {
  if (!isFinite(s) || s < 0) return '0:00';
  return `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}`;
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function setSyncStatus(st, driftMs = null) {
  if (st === 'host') {
    syncDot.className = 'sync-dot synced';
    syncLabel.textContent = '👑 Room Host (Master)';
  } else if (st === 'synced') {
    syncDot.className = 'sync-dot synced';
    syncLabel.textContent = driftMs !== null ? `Synced (${driftMs >= 0 ? '+' : ''}${driftMs}ms from Host)` : 'Synced';
  } else if (st === 'syncing') {
    syncDot.className = 'sync-dot syncing';
    syncLabel.textContent = driftMs !== null ? `Syncing (${driftMs >= 0 ? '+' : ''}${driftMs}ms from Host)` : 'Syncing…';
  } else if (st === 'error') {
    syncDot.className = 'sync-dot error';
    syncLabel.textContent = 'Sync error';
  }
}

function setPlayingVisuals(playing) {
  iconPlay.classList.toggle('hidden', playing);
  iconPause.classList.toggle('hidden', !playing);
  artBars.classList.toggle('playing', playing);
}


// ─── NTP Clock Sync ───────────────────────────────────────────────────────────

async function syncClock() {
  if (!state.isHost && syncLabel.textContent === 'Sync error') {
    setSyncStatus('syncing');
  }
  const samples = [];
  
  // Use Socket.io WebSocket ping if available for ultra-low latency, fallback to HTTP
  const getPing = () => {
    if (state.socket && state.socket.connected) {
      return new Promise(resolve => {
        let timedOut = false;
        const timer = setTimeout(() => {
          timedOut = true;
          fetch('/ping')
            .then(r => r.json())
            .then(d => resolve(d.serverTime))
            .catch(() => resolve(null));
        }, 1500);

        state.socket.emit('ping-clock', res => {
          if (timedOut) return;
          clearTimeout(timer);
          resolve(res?.serverTime || null);
        });
      });
    }
    return fetch('/ping')
      .then(r => r.json())
      .then(d => d.serverTime)
      .catch(() => null);
  };

  for (let i = 0; i < 6; i++) {
    const t1 = Date.now();
    const serverTime = await getPing();
    const t3 = Date.now();
    if (serverTime) {
      const rtt = t3 - t1;
      const offset = serverTime - (t1 + t3) / 2;
      samples.push({ rtt, offset });
    }
    await new Promise(r => setTimeout(r, 40));
  }
  if (samples.length > 0) {
    // Filter by lowest RTT (least network delay jitter)
    samples.sort((a, b) => a.rtt - b.rtt);
    const bestCandidates = samples.slice(0, Math.min(3, samples.length));
    bestCandidates.sort((a, b) => a.offset - b.offset);
    state.clockOffset = bestCandidates[Math.floor(bestCandidates.length / 2)].offset;
    if (!state.isHost) {
      setSyncStatus('synced');
    }
    console.log(`[NTP] bestRTT=${samples[0].rtt}ms offset=${state.clockOffset.toFixed(1)}ms`);
  }
}


// ─── Socket.io ────────────────────────────────────────────────────────────────

function getUserToken() {
  let token = localStorage.getItem('syncplay_user_token');
  if (!token) {
    token = 'usr_' + Math.random().toString(36).substring(2, 10);
    localStorage.setItem('syncplay_user_token', token);
  }
  return token;
}

function connectSocket() {
  if (state.socket && state.socket.connected) return;
  if (state.socket) {
    state.socket.removeAllListeners();
    state.socket.disconnect();
  }
  state.socket = io({
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 4000,
    timeout: 20000
  });

  state.socket.on('connect', () => {
    console.log('[Socket] Connected:', state.socket.id);
    // Auto-reconnect to room if already joined or saved in session
    const savedRoomId = state.roomId || sessionStorage.getItem('syncplay_room_id');
    const savedName = state.myName || sessionStorage.getItem('syncplay_user_name') || 'Listener';
    if (savedRoomId) {
      state.roomId = savedRoomId;
      state.myName = savedName;
      console.log('[Socket] Rejoining room:', state.roomId);
      state.socket.emit('join-room', {
        roomId: state.roomId,
        userName: state.myName,
        userToken: getUserToken()
      }, res => {
        if (res && res.success) {
          state.isHost = res.isHost;
          enterRoom(res.room, res.isHost);
          showToast('🟢 Reconnected to room');
        } else if (res && res.error) {
          showToast('❌ ' + res.error);
          state.roomId = null;
          state.isHost = false;
          sessionStorage.removeItem('syncplay_room_id');
          sessionStorage.removeItem('syncplay_user_name');
          showPage('landing');
        }
      });
    }
  });

  state.socket.on('disconnect', (reason) => {
    if (reason === 'io client disconnect') return;
    setSyncStatus('error');
    showToast('⚠️ Disconnected from server. Reconnecting…', 3000);
  });

  state.socket.on('user-status-changed', ({ users, newHostName, reconnectedName }) => {
    const myToken = getUserToken();
    const isMeHost = users.some(u => u.isHost && (u.id === state.socket?.id || u.userToken === myToken));

    if (isMeHost !== state.isHost) {
      state.isHost = isMeHost;
      applyMode();
      if (isMeHost) {
        showToast('👑 You are now the room Host!', 4000);
      }
    } else if (newHostName) {
      showToast(`👑 ${newHostName} is now the room Host`);
    } else if (reconnectedName) {
      showToast(`🟢 ${reconnectedName} reconnected`);
    }

    renderUsers(users);
  });

  state.socket.on('user-joined', ({ name, users }) => { renderUsers(users); showToast(`🎵 ${name} joined`); });
  state.socket.on('user-left',   ({ name, users, newHostId }) => {
    renderUsers(users);
    showToast(`👋 ${name} left`);
    if (newHostId === state.socket.id && !state.isHost) {
      state.isHost = true;
      applyMode();
      showToast('👑 You are now the host!', 4000);
    }
  });

  // Download progress (all clients see the bar)
  state.socket.on('download-start', () => {
    opProgressWrap.classList.remove('hidden');
    opProgressBar.style.width = '0%';
    opProgressLabel.textContent = 'Starting…';
    hostPrompt.classList.add('hidden');
    guestWaiting.classList.add('hidden');
  });

  state.socket.on('download-progress', ({ percent, status }) => {
    opProgressWrap.classList.remove('hidden');
    opProgressBar.style.width = percent + '%';
    const labels = { downloading: `Downloading… ${percent}%`, converting: 'Converting to MP3…', done: 'Done!' };
    opProgressLabel.textContent = labels[status] ?? `${percent}%`;
    if (status === 'done') setTimeout(() => opProgressWrap.classList.add('hidden'), 1200);
  });

  state.socket.on('download-error', ({ message }) => {
    opProgressWrap.classList.add('hidden');
    showErr(ytError, '❌ ' + message);
    showToast('Download failed: ' + message, 4000);
    // Restore prompt for the user to try again
    if (!playerCard.classList.contains('hidden')) return;
    hostPrompt.classList.remove('hidden');
  });

  // Track ready — load audio for everyone
  state.socket.on('track-loaded', ({ trackName, thumbnail, audioUrl, playlist, currentTrackIndex, autoPlay, playAt }) => {
    opProgressWrap.classList.add('hidden');
    state.roomIsPlaying = !!autoPlay;
    state.offsetHistory = [];
    setPlayingVisuals(!!autoPlay);
    loadAudio(audioUrl, trackName, 0, () => {
      if (autoPlay && playAt) {
        schedulePlay(0, playAt);
      }
    }, thumbnail);
    if (playlist) {
      state.playlist = playlist;
      state.currentTrackIndex = currentTrackIndex;
      renderPlaylist(playlist, currentTrackIndex);
    }
    showToast(`🎵 "${trackName}" loaded`);
  });

  // Playlist queue update without interrupting playback
  state.socket.on('playlist-updated', ({ playlist, currentTrackIndex, addedTrackName, addedBy }) => {
    if (playlist) {
      state.playlist = playlist;
      state.currentTrackIndex = currentTrackIndex;
      renderPlaylist(playlist, currentTrackIndex);
    }
    if (addedTrackName) {
      showToast(`🎶 "${addedTrackName}" added to queue by ${addedBy || 'someone'}`);
    }
  });

  // Room cleared (playlist empty)
  state.socket.on('room-cleared', () => {
    audioEngine.stop();
    playerCard.classList.add('hidden');
    controlsBar.classList.add('hidden');
    hostPrompt.classList.remove('hidden');
    state.playlist = [];
    state.currentTrackIndex = -1;
    state.offsetHistory = [];
    renderPlaylist([], -1);
    showToast('Playlist is now empty');
  });

  // Error notifications
  state.socket.on('error-msg', ({ message }) => {
    if (message) showToast('⚠️ ' + message);
  });

  // Playback events — ALL clients (host & guests) process these symmetrically
  state.socket.on('play',  ({ position, playAt }) => {
    state.roomIsPlaying = true;
    state.offsetHistory = [];
    schedulePlay(position, playAt);
  });
  state.socket.on('pause', ({ position }) => {
    state.roomIsPlaying = false;
    state.offsetHistory = [];
    clearTimeout(state.sync.playTimeout);
    audioEngine.pause(position);
    state.sync.active = false;
    setPlayingVisuals(false);
  });
  state.socket.on('seek',  ({ position, playAt, isPlaying }) => {
    state.roomIsPlaying = isPlaying;
    state.offsetHistory = [];
    clearTimeout(state.sync.playTimeout);
    if (isPlaying && playAt) {
      schedulePlay(position, playAt);
    } else {
      audioEngine.pause(position);
      state.sync.active = false;
      setPlayingVisuals(false);
    }
  });

  // ─── Hybrid Sync Controller ───────────────────────────────────────────────────
  // Zone 0: |drift| <= 5ms   → Perfect, no action
  // Zone 1: 5ms  < |drift| <= 150ms → Gentle speed nudge (±1%, inaudible)
  // Zone 2: 150ms < |drift| <= 500ms → Strong speed nudge (±3%)
  // Zone 3: |drift| > 500ms  → Hard seek (instant correction)
  state.socket.on('sync-pulse', ({ position, serverTime, isPlaying, hostHardwareLatency }) => {
    if (!isPlaying || state.isSeeking || !audioEngine.duration) return;

    if (state.isHost) {
      audioEngine.resetRate();
      setSyncStatus('host');
      return;
    }

    const nowServer = Date.now() + state.clockOffset;
    const elapsedSincePulse = (nowServer - serverTime) / 1000;
    
    // Auto-detect hardware audio speaker output latency difference between Guest and Host
    const guestHW = audioEngine.getHardwareLatency();
    const hostHW = typeof hostHardwareLatency === 'number' ? hostHardwareLatency : 0;
    const autoHwOffsetSec = guestHW - hostHW;

    // Total offset = Auto Hardware Latency Compensation + User Manual Offset
    const userManualOffsetSec = state.audioOffsetMs / 1000;
    const totalOffsetSec = autoHwOffsetSec + userManualOffsetSec;

    const targetPos = position + elapsedSincePulse + totalOffsetSec;

    // CRITICAL: If room is playing but guest is NOT playing yet (e.g. late buffer decode, late join, or un-muted)
    if (!audioEngine.isPlaying) {
      if (audioEngine.buffer && targetPos < audioEngine.duration) {
        console.log(`[Sync] Catching up unsynced guest playback from pulse at targetPos=${targetPos.toFixed(2)}s`);
        audioEngine.playAt(targetPos, Date.now() + state.clockOffset + 50, state.clockOffset);
        setPlayingVisuals(true);
        state.roomIsPlaying = true;
      }
      return;
    }

    const curTime = audioEngine.getCurrentTime();
    const rawDrift = curTime - targetPos;
    const rawDriftMs = Math.round(rawDrift * 1000);

    // Rolling 5-sample moving average — filters out single Wi-Fi jitter spikes
    state.offsetHistory.push(rawDriftMs);
    if (state.offsetHistory.length > 5) state.offsetHistory.shift();
    const smoothedDriftMs = Math.round(
      state.offsetHistory.reduce((a, b) => a + b, 0) / state.offsetHistory.length
    );
    const absDrift = Math.abs(smoothedDriftMs);

    if (absDrift > 500) {
      // Zone 3: Hard seek — large drift, instant correction
      audioEngine.playAt(targetPos, Date.now() + state.clockOffset, state.clockOffset);
      audioEngine.resetRate();
      state.offsetHistory = [];
      setSyncStatus('syncing', smoothedDriftMs);
      console.log(`[Sync] HARD SEEK drift=${smoothedDriftMs}ms`);
    } else if (absDrift > 150) {
      // Zone 2: Strong speed nudge ±3% to close medium drifts in ~5 seconds
      const nudge = smoothedDriftMs > 0 ? 0.97 : 1.03;
      audioEngine.setRate(nudge);
      setSyncStatus('syncing', smoothedDriftMs);
      console.log(`[Sync] SPEED NUDGE ${nudge}x drift=${smoothedDriftMs}ms`);
    } else if (absDrift > 5) {
      // Zone 1: Gentle nudge ±1% for small drifts — completely inaudible
      const nudge = smoothedDriftMs > 0 ? 0.99 : 1.01;
      audioEngine.setRate(nudge);
      setSyncStatus('synced', smoothedDriftMs);
    } else {
      // Zone 0: Perfect lockstep — reset to 1.0x
      audioEngine.resetRate();
      setSyncStatus('synced', smoothedDriftMs);
    }
  });
}


// ─── Synchronized Playback ───────────────────────────────────────────────────
function schedulePlay(position, playAt) {
  clearTimeout(state.sync.playTimeout);
  audioEngine.playAt(position, playAt, state.clockOffset);
  setPlayingVisuals(true);
}


// ─── Real-Time WebAudio Spectrum Visualizer ──────────────────────────────────
let visualizerAnimFrame = null;
function startVisualizerLoop() {
  if (visualizerAnimFrame) cancelAnimationFrame(visualizerAnimFrame);
  const spans = artBars ? artBars.querySelectorAll('span') : [];
  if (!spans || spans.length === 0) return;

  const updateSpectrum = () => {
    if (audioEngine.isPlaying && !artBars.classList.contains('hidden')) {
      const freqs = audioEngine.getFrequencyData();
      if (freqs && freqs.length > 0) {
        for (let i = 0; i < spans.length; i++) {
          const bin = Math.min(freqs.length - 1, Math.floor(i * (freqs.length / spans.length)));
          const val = freqs[bin]; // 0 - 255
          const heightPx = Math.max(5, Math.min(42, Math.round((val / 255) * 42)));
          spans[i].style.height = `${heightPx}px`;
        }
      }
    } else {
      spans.forEach(s => s.style.height = '');
    }
    visualizerAnimFrame = requestAnimationFrame(updateSpectrum);
  };
  visualizerAnimFrame = requestAnimationFrame(updateSpectrum);
}

// ─── MediaSession API Integration (Lockscreen & Background Controls) ──────────
function updateMediaSession(trackName, thumbnail = null) {
  if (!('mediaSession' in navigator)) return;
  const artwork = thumbnail ? [{ src: thumbnail, sizes: '320x180', type: 'image/jpeg' }] : [];
  navigator.mediaSession.metadata = new MediaMetadata({
    title: trackName || 'SyncPlay Audio',
    artist: 'SyncPlay (Room ' + (state.roomId || 'Shared') + ')',
    album: 'SyncPlay',
    artwork
  });

  try {
    navigator.mediaSession.setActionHandler('play', () => {
      if (state.isHost && !state.roomIsPlaying) btnPlayPause.click();
    });
    navigator.mediaSession.setActionHandler('pause', () => {
      if (state.isHost && state.roomIsPlaying) btnPlayPause.click();
    });
    navigator.mediaSession.setActionHandler('seekbackward', () => {
      if (state.isHost) btnSeekBack.click();
    });
    navigator.mediaSession.setActionHandler('seekforward', () => {
      if (state.isHost) btnSeekFwd.click();
    });
  } catch (_) {}
}

// ─── Load audio ───────────────────────────────────────────────────────────────
async function loadAudio(url, trackName, startPos = 0, onReadyCallback = null, thumbnail = null) {
  clearTimeout(state.sync.playTimeout);
  audioEngine.pause();
  setPlayingVisuals(false);
  state.sync.active = false;

  hostPrompt.classList.add('hidden');
  guestWaiting.classList.add('hidden');
  playerCard.classList.remove('hidden');
  controlsBar.classList.remove('hidden');
  displayTrack.textContent = trackName || 'Unknown Track';

  state.currentThumbnail = thumbnail || null;
  if (albumArtImg) {
    if (thumbnail) {
      albumArtImg.src = thumbnail;
      albumArtImg.classList.remove('hidden');
      albumArtWrap?.classList.add('has-thumb');
    } else {
      albumArtImg.classList.add('hidden');
      albumArtWrap?.classList.remove('has-thumb');
    }
  }

  updateMediaSession(trackName, thumbnail);
  startVisualizerLoop();

  try {
    showToast('⏳ Decoding audio buffer…');
    const dur = await audioEngine.loadTrack(url);
    state.duration = dur;
    displayDuration.textContent = fmt(state.duration);
    audioEngine.startPosition = startPos;
    startProgressLoop();
    showToast(`🎵 "${trackName}" ready`);
    if (onReadyCallback) onReadyCallback();
  } catch (err) {
    console.error('[WebAudio Load Error]', err);
    showToast('❌ Failed to load audio: ' + err.message);
  }
}

let hostPulseCounter = 0;
function startProgressLoop() {
  clearInterval(state.progressInterval);
  state.progressInterval = setInterval(() => {
    if (state.isSeeking || !audioEngine.duration) return;
    const curTime = audioEngine.getCurrentTime();
    const pct = (curTime / audioEngine.duration) * 100;
    scrubFill.style.width = pct + '%';
    displayCurrent.textContent = fmt(curTime);

    // Background Pre-Buffering: Silently preload next track when current song reaches 70% or 25s left
    if (state.playlist && state.currentTrackIndex + 1 < state.playlist.length && (pct >= 70 || (audioEngine.duration - curTime <= 25))) {
      const nextIdx = state.currentTrackIndex + 1;
      const nextTrack = state.playlist[nextIdx];
      if (nextTrack) {
        const nextUrl = `/audio/${state.roomId}?idx=${nextIdx}&t=${encodeURIComponent(nextTrack.id || 'track')}`;
        audioEngine.preloadTrack(nextUrl);
      }
    }

    // Host emits real-time position pulse every 2 seconds
    // serverTime = current server epoch: Date.now() + clockOffset
    // hardwareLatency = host device speaker hardware output latency (seconds)
    if (state.isHost && audioEngine.isPlaying && state.socket) {
      hostPulseCounter++;
      if (hostPulseCounter % 4 === 0) {
        state.socket.emit('host-pulse', {
          position: curTime,
          serverTime: Date.now() + state.clockOffset,
          hardwareLatency: audioEngine.getHardwareLatency()
        });
      }
    }
  }, 500);
}


// ─── Playback controls ────────────────────────────────────────────────────────

function setControlsEnabled(enabled) {
  btnPlayPause.disabled = !enabled;
  btnSeekBack.disabled  = !enabled;
  btnSeekFwd.disabled   = !enabled;
  scrubTrack.style.pointerEvents = enabled ? 'auto' : 'none';
    scrubTrack.style.cursor        = enabled ? 'pointer' : 'default';
}

btnPlayPause.addEventListener('click', () => {
  if (!state.isHost || !audioEngine.buffer) return;
  const pos = audioEngine.getCurrentTime();
  // Use server-authoritative state — never read DOM classes
  if (!state.roomIsPlaying) {
    state.socket.emit('play', { position: pos });
  } else {
    state.socket.emit('pause', { position: pos });
  }
});

btnSeekBack.addEventListener('click', () => {
  if (!state.isHost) return;
  const pos = Math.max(0, audioEngine.getCurrentTime() - 10);
  state.socket.emit('seek', { position: pos });
});

btnSeekFwd.addEventListener('click', () => {
  if (!state.isHost) return;
  const pos = Math.min(audioEngine.duration || 0, audioEngine.getCurrentTime() + 10);
  state.socket.emit('seek', { position: pos });
});

function toggleMute() {
  state.isMuted = !state.isMuted;
  if (state.isMuted) {
    state.previousVolume = audioEngine.volume || 1;
    audioEngine.setVolume(0);
    if (volumeSlider) volumeSlider.value = 0;
    btnMute?.classList.add('muted');
  } else {
    const restoredVol = state.previousVolume > 0 ? state.previousVolume : 1;
    audioEngine.setVolume(restoredVol);
    if (volumeSlider) volumeSlider.value = restoredVol;
    btnMute?.classList.remove('muted');
    try { localStorage.setItem('syncplay_volume', String(restoredVol)); } catch (_) {}
  }
}
btnMute?.addEventListener('click', toggleMute);

if (volumeSlider) {
  volumeSlider.value = !isNaN(state.volume) ? state.volume : 1;
  volumeSlider.addEventListener('input', () => {
    const v = parseFloat(volumeSlider.value);
    audioEngine.setVolume(v);
    if (v > 0 && state.isMuted) {
      state.isMuted = false;
      btnMute?.classList.remove('muted');
    }
    try { localStorage.setItem('syncplay_volume', String(v)); } catch (_) {}
  });
}

// Audio offset slider — compensates for device hardware speaker latency
// Scrubbing — mouse + touch support
let scrubbing = false;
let isPlayingBeforeScrub = false;

function startScrub(e) {
  if (!state.isHost || !audioEngine.duration) return;
  scrubbing = true;
  state.isSeeking = true;
  isPlayingBeforeScrub = audioEngine.isPlaying;
  doScrub(getEventPos(e));
}
function moveScrub(e) {
  if (!scrubbing) return;
  doScrub(getEventPos(e));
}
function endScrub(e) {
  if (!scrubbing) return;
  scrubbing = false;
  state.isSeeking = false;
  const pos = getScrubPos(getEventPos(e));
  state.socket.emit('seek', { position: pos });
  // Restore visual state so button matches actual playing state
  setPlayingVisuals(isPlayingBeforeScrub);
}

// Normalize mouse and touch events to a single {clientX} object
function getEventPos(e) {
  return e.touches ? e.touches[0] || e.changedTouches[0] : e;
}

scrubTrack.addEventListener('mousedown',  startScrub);
scrubTrack.addEventListener('touchstart', startScrub, { passive: true });
document.addEventListener('mousemove',  moveScrub);
document.addEventListener('touchmove',  moveScrub, { passive: true });
document.addEventListener('mouseup',    endScrub);
document.addEventListener('touchend',   endScrub);

function getScrubPos(e) {
  const r = scrubTrack.getBoundingClientRect();
  return Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)) * (audioEngine.duration || 0);
}
function doScrub(e) {
  const pos = getScrubPos(e);
  const pct = audioEngine.duration ? (pos / audioEngine.duration) * 100 : 0;  // ← use audioEngine.duration
  scrubFill.style.width = pct + '%';
  displayCurrent.textContent = fmt(pos);
}
const offsetSlider  = $('offset-slider');
const offsetDisplay = $('offset-display');
if (offsetSlider && offsetDisplay) {
  offsetSlider.value = state.audioOffsetMs;
  const sign = state.audioOffsetMs > 0 ? '+' : '';
  offsetDisplay.textContent = `${sign}${state.audioOffsetMs} ms`;
  offsetSlider.addEventListener('input', () => {
    state.audioOffsetMs = parseInt(offsetSlider.value, 10);
    const sign = state.audioOffsetMs > 0 ? '+' : '';
    offsetDisplay.textContent = `${sign}${state.audioOffsetMs} ms`;
    state.offsetHistory = []; // Reset moving average so new offset takes effect immediately
    try { localStorage.setItem('syncplay_audio_offset', String(state.audioOffsetMs)); } catch (_) {}
  });
}

audioEngine.onEnded = () => {
  setPlayingVisuals(false);
  state.roomIsPlaying = false;
  if (state.isHost && state.playlist && state.currentTrackIndex + 1 < state.playlist.length) {
    const nextIdx = state.currentTrackIndex + 1;
    showToast(`⏭ Loaded next track: "${state.playlist[nextIdx].trackName}" (click Play to start)`);
    state.socket.emit('select-track', { index: nextIdx, autoPlay: false });
  }
};





// ─── Host / Guest mode ────────────────────────────────────────────────────────

function applyMode() {
  const isHost = state.isHost;
  loadPanel.classList.toggle('hidden', false); // all users see load panel
  setControlsEnabled(isHost);
  ctrlBadge.className = 'ctrl-badge ' + (isHost ? 'host' : 'guest');
  ctrlBadge.textContent = isHost
    ? '👑 You are the host — controls are yours'
    : '🎧 Listening as guest — host controls playback';
}


// ─── Tabs ─────────────────────────────────────────────────────────────────────

tabYt.addEventListener('click', () => {
  tabYt.classList.add('active');   tabYt.setAttribute('aria-selected','true');
  tabFile.classList.remove('active'); tabFile.setAttribute('aria-selected','false');
  panelYt.classList.remove('hidden');
  panelFile.classList.add('hidden');
});

tabFile.addEventListener('click', () => {
  tabFile.classList.add('active');  tabFile.setAttribute('aria-selected','true');
  tabYt.classList.remove('active'); tabYt.setAttribute('aria-selected','false');
  panelFile.classList.remove('hidden');
  panelYt.classList.add('hidden');
});


// ─── YouTube Download ─────────────────────────────────────────────────────────

btnLoadYt.addEventListener('click', submitYtUrl);
inputYtUrl.addEventListener('keydown', e => { if (e.key === 'Enter') submitYtUrl(); });

async function submitYtUrl() {
  const url = inputYtUrl.value.trim();
  if (!url) { showErr(ytError, 'Please enter a YouTube URL or video ID.'); return; }
  ytError.classList.add('hidden');

  // Show loading state immediately
  opProgressWrap.classList.remove('hidden');
  opProgressBar.style.width = '5%';
  opProgressLabel.textContent = 'Checking cache…';
  btnLoadYt.disabled = true;

  try {
    const res = await fetch(`/download/${state.roomId}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-user-name': state.myName
      },
      body: JSON.stringify({ url })
    });
    const data = await res.json();
    if (!res.ok) {
      showErr(ytError, data.error || 'Download failed.');
      opProgressWrap.classList.add('hidden');
    }
    // Further progress comes via socket events
  } catch (err) {
    showErr(ytError, 'Network error. Is the server running?');
    opProgressWrap.classList.add('hidden');
  } finally {
    btnLoadYt.disabled = false;
    inputYtUrl.value = '';
  }
}


// ─── File Upload ──────────────────────────────────────────────────────────────

dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault(); dropZone.classList.remove('drag-over');
  if (e.dataTransfer.files[0]) uploadFile(e.dataTransfer.files[0]);
});
inputFile.addEventListener('change', () => { if (inputFile.files[0]) uploadFile(inputFile.files[0]); });

function uploadFile(file) {
  const isAudioExt = /\.(mp3|m4a|aac|wav|ogg|opus|flac|webm|mp4|wma)$/i.test(file.name);
  const isAudioMime = !file.type || file.type.startsWith('audio/') || file.type === 'video/mp4';

  if (!isAudioExt && !isAudioMime) {
    showErr(fileError, 'Please select an audio file (MP3, M4A, AAC, WAV, etc).');
    inputFile.value = '';
    return;
  }
  if (file.size > 50 * 1024 * 1024) {
    showErr(fileError, 'File is too large. Max size is 50 MB.');
    inputFile.value = '';
    return;
  }

  fileError.classList.add('hidden');
  opProgressWrap.classList.remove('hidden');
  opProgressBar.style.width = '0%';
  opProgressLabel.textContent = 'Uploading… 0%';

  const fd = new FormData();
  fd.append('audio', file);

  const xhr = new XMLHttpRequest();
  xhr.timeout = 300000; // 5 minute timeout for mobile uploads
  xhr.open('POST', `/upload/${state.roomId}`);
  xhr.setRequestHeader('x-user-name', state.myName);
  xhr.setRequestHeader('x-user-token', getUserToken());

  // Keep WebSocket connection active with heartbeats during heavy file upload
  const keepAliveTimer = setInterval(() => {
    if (state.socket && state.socket.connected) {
      state.socket.emit('ping-clock');
    }
  }, 10000);

  const cleanupUpload = () => {
    clearInterval(keepAliveTimer);
    opProgressWrap.classList.add('hidden');
    inputFile.value = '';
  };

  xhr.upload.addEventListener('progress', e => {
    if (e.lengthComputable) {
      const pct = Math.round((e.loaded / e.total) * 100);
      opProgressBar.style.width = pct + '%';
      opProgressLabel.textContent = `Uploading… ${pct}%`;
    }
  });

  xhr.addEventListener('load', () => {
    cleanupUpload();
    if (xhr.status !== 200) {
      try {
        const err = JSON.parse(xhr.responseText || '{}');
        showErr(fileError, err.error || 'Upload failed.');
      } catch (_) {
        showErr(fileError, `Upload failed (HTTP ${xhr.status}).`);
      }
    }
  });

  xhr.addEventListener('error', () => {
    cleanupUpload();
    showErr(fileError, 'Network connection lost during upload. Please try again.');
  });

  xhr.addEventListener('timeout', () => {
    cleanupUpload();
    showErr(fileError, 'Upload timed out. Check your internet connection or try a smaller file.');
  });

  xhr.send(fd);
}


// ─── Render Users & Playlist ──────────────────────────────────────────────────

function renderUsers(users) {
  usersList.innerHTML = '';
  userCount.textContent = users.length;
  const myToken = getUserToken();

  users.forEach(u => {
    const li = document.createElement('li');
    li.className = 'user-item' + (u.offline ? ' user-offline' : '');
    const isMe = u.id === state.socket?.id || u.userToken === myToken;

    const canMakeHost = state.isHost && !isMe && !u.isHost && !u.offline;
    const makeHostBtnHtml = canMakeHost ? `<button class="btn-make-host" title="Transfer Host role to ${esc(u.name)}">👑</button>` : '';

    li.innerHTML = `
      <div class="user-avatar">${esc(u.name.slice(0,2).toUpperCase())}</div>
      <span class="user-name">${esc(u.name)}</span>
      ${u.isHost  ? '<span class="user-tag tag-host">Host</span>' : ''}
      ${u.offline ? '<span class="user-tag tag-offline">Reconnecting…</span>' : ''}
      ${isMe      ? '<span class="user-tag tag-you">You</span>'   : ''}
      ${makeHostBtnHtml}
    `;

    if (canMakeHost) {
      const btn = li.querySelector('.btn-make-host');
      if (btn) {
        btn.addEventListener('click', e => {
          e.stopPropagation();
          if (confirm(`Transfer Host role to "${u.name}"?`)) {
            state.socket.emit('make-host', { targetUserToken: u.userToken });
          }
        });
      }
    }

    usersList.appendChild(li);
  });
}

function renderPlaylist(playlist, currentIndex) {
  if (!playlistList || !playlistCount) return;
  state.playlist = playlist || [];
  state.currentTrackIndex = currentIndex;
  playlistList.innerHTML = '';
  playlistCount.textContent = playlist ? playlist.length : 0;

  if (!playlist || playlist.length === 0) {
    playlistList.innerHTML = '<li class="playlist-empty">No songs in queue yet</li>';
    return;
  }

  playlist.forEach((track, idx) => {
    const li = document.createElement('li');
    const isCurrent = idx === currentIndex;
    li.className = 'playlist-item' + (isCurrent ? ' active' : '');
    const delBtnHtml = state.isHost ? `<button class="btn-del-track" title="Remove track">✕</button>` : '';
    const iconHtml = track.thumbnail
      ? `<img src="${esc(track.thumbnail)}" class="playlist-thumb" alt="art" />`
      : `<div class="playlist-icon">${isCurrent ? '▶' : (idx + 1)}</div>`;

    li.innerHTML = `
      ${iconHtml}
      <div class="playlist-meta">
        <span class="playlist-title">${esc(track.trackName)}</span>
        <span class="playlist-by">Added by ${esc(track.addedBy || 'Someone')}</span>
      </div>
      ${delBtnHtml}
    `;

    // Track click
    li.addEventListener('click', e => {
      if (e.target.classList.contains('btn-del-track')) return;
      if (!state.isHost) {
        showToast('🔒 Only the host can switch songs');
        return;
      }
      if (idx === currentIndex) return;
      showToast(`Loading "${track.trackName}"…`);
      state.socket.emit('select-track', { index: idx, autoPlay: false });
    });

    // Delete track click (Host only)
    if (state.isHost) {
      const delBtn = li.querySelector('.btn-del-track');
      if (delBtn) {
        delBtn.addEventListener('click', e => {
          e.stopPropagation();
          state.socket.emit('remove-track', { index: idx });
        });
      }
    }

    playlistList.appendChild(li);
  });
}


// ─── Landing ──────────────────────────────────────────────────────────────────

btnCreate.addEventListener('click', async () => {
  const name = inputCreateName.value.trim() || 'Host';
  if (!state.socket || !state.socket.connected) {
    connectSocket();
    if (!state.clockOffset) await syncClock();
  }
  state.myName = name;
  state.socket.emit('create-room', { userName: name, userToken: getUserToken() }, res => {
    if (res.error) { showErr(landingError, res.error); return; }
    enterRoom(res.room, true);
  });
});

btnJoin.addEventListener('click', async () => {
  const name = inputJoinName.value.trim() || 'Guest';
  const code = inputRoomCode.value.trim().toUpperCase();
  if (!code) { showErr(landingError, 'Please enter a room code.'); return; }
  if (!state.socket || !state.socket.connected) {
    connectSocket();
    if (!state.clockOffset) await syncClock();
  }
  state.myName = name;
  state.socket.emit('join-room', { roomId: code, userName: name, userToken: getUserToken() }, res => {
    if (res.error) { showErr(landingError, res.error); return; }
    enterRoom(res.room, res.isHost ?? false);
  });
});

inputRoomCode.addEventListener('input', e => { e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g,''); });
[inputCreateName, inputRoomCode, inputJoinName].forEach((el, i) => {
  el.addEventListener('keydown', e => { if (e.key === 'Enter') (i === 0 ? btnCreate : btnJoin).click(); });
});


// ─── Enter Room ───────────────────────────────────────────────────────────────

function enterRoom(roomData, isHost) {
  state.roomId = roomData.id;
  state.isHost = isHost;
  state.roomIsPlaying = roomData.isPlaying || false;
  setPlayingVisuals(state.roomIsPlaying);
  displayRoomId.textContent = roomData.id;

  try {
    sessionStorage.setItem('syncplay_room_id', roomData.id);
    if (state.myName) sessionStorage.setItem('syncplay_user_name', state.myName);
  } catch (_) {}

  showPage('room');
  applyMode();
  renderUsers(roomData.users);
  if (roomData.playlist) {
    renderPlaylist(roomData.playlist, roomData.currentTrackIndex);
  }

  loadPanel.classList.remove('hidden');

  if (roomData.hasAudio) {
    const wasPlaying = roomData.isPlaying;
    const joinedAt = roomData.joinedAt || Date.now();      // server epoch when we joined
    const snapshotPos = roomData.position;                  // server's live position at join time
    const idx = roomData.currentTrackIndex !== undefined ? roomData.currentTrackIndex : 0;
    const currentTrack = (roomData.playlist && roomData.playlist[idx]) || null;
    const trackThumb = roomData.thumbnail || currentTrack?.thumbnail || null;
    const audioUrl = `/audio/${roomData.id}?idx=${idx}&v=${Date.now()}`;

    loadAudio(audioUrl, roomData.trackName, 0, () => {
      if (wasPlaying) {
        // Recalculate live position: snapshot + time elapsed since join (covers decode delay)
        const decodeElapsedSec = (Date.now() + state.clockOffset - joinedAt) / 1000;
        const livePos = Math.max(0, Math.min(audioEngine.duration - 0.1, snapshotPos + decodeElapsedSec));
        const livePlayAt = Date.now() + state.clockOffset + 200; // 200ms buffer for WebAudio prime
        schedulePlay(livePos, livePlayAt);
        console.log(`[LateJoin] snapshotPos=${snapshotPos.toFixed(2)}s decodeElapsed=${decodeElapsedSec.toFixed(2)}s livePos=${livePos.toFixed(2)}s`);
      }
    }, trackThumb);
  } else if (roomData.downloading) {
    hostPrompt.classList.add('hidden');
    opProgressWrap.classList.remove('hidden');
    opProgressLabel.textContent = 'Downloading…';
  } else {
    hostPrompt.classList.remove('hidden');
    if (!isHost) guestWaiting.classList.remove('hidden');
  }
}



// ─── Room header controls ─────────────────────────────────────────────────────

btnCopyId.addEventListener('click', () => {
  navigator.clipboard.writeText(state.roomId).then(() => showToast('✅ Room code copied!'));
});

btnShareRoom?.addEventListener('click', () => {
  if (!state.roomId) return;
  const shareUrl = `${window.location.origin}/?room=${state.roomId}`;
  if (navigator.share) {
    navigator.share({
      title: 'SyncPlay — Listen Together',
      text: `Join my SyncPlay listening room: ${state.roomId}!`,
      url: shareUrl
    }).catch(() => {
      navigator.clipboard.writeText(shareUrl).then(() => showToast('🔗 Room invite link copied!'));
    });
  } else {
    navigator.clipboard.writeText(shareUrl).then(() => showToast('🔗 Room invite link copied!'));
  }
});

btnLeave.addEventListener('click', () => {
  audioEngine.stop();
  clearTimeout(state.sync.playTimeout);
  clearInterval(state.progressInterval);
  clearInterval(state.driftInterval);
  state.sync.active = false;
  if (state.socket) {
    state.socket.emit('leave-room');
  }

  try {
    sessionStorage.removeItem('syncplay_room_id');
    sessionStorage.removeItem('syncplay_user_name');
  } catch (_) {}

  // Reset UI
  playerCard.classList.add('hidden');
  controlsBar.classList.add('hidden');
  hostPrompt.classList.remove('hidden');
  guestWaiting.classList.add('hidden');
  loadPanel.classList.add('hidden');
  opProgressWrap.classList.add('hidden');
  scrubFill.style.width = '0%';
  displayCurrent.textContent = '0:00';
  displayDuration.textContent = '0:00';
  displayTrack.textContent = '—';
  if (albumArtImg) {
    albumArtImg.src = '';
    albumArtImg.classList.add('hidden');
    albumArtWrap?.classList.remove('has-thumb');
  }
  state.currentThumbnail = null;
  if ('mediaSession' in navigator) {
    navigator.mediaSession.metadata = null;
  }
  usersList.innerHTML = '';
  artBars.classList.remove('playing');
  setPlayingVisuals(false);
  state.roomId = null; state.isHost = false;

  showPage('landing');
  connectSocket(); syncClock();
});


// ─── Init & Gesture Audio Unlocking ──────────────────────────────────────────
const unlockAudio = () => {
  audioEngine.unlock();
};

['touchstart', 'touchend', 'click', 'pointerdown'].forEach(evt => {
  document.addEventListener(evt, unlockAudio, { passive: true });
});

const bannerEl = document.getElementById('audio-unlock-banner');
if (bannerEl) {
  bannerEl.addEventListener('click', e => {
    e.stopPropagation();
    audioEngine.unlock();
  });
}

// ─── Keyboard Shortcuts ─────────────────────────────────────────────────────
window.addEventListener('keydown', e => {
  if (['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement?.tagName)) return;
  if (e.code === 'Space') {
    e.preventDefault();
    if (state.isHost && !btnPlayPause.disabled) btnPlayPause.click();
  } else if (e.code === 'ArrowLeft') {
    e.preventDefault();
    if (state.isHost && !btnSeekBack.disabled) btnSeekBack.click();
  } else if (e.code === 'ArrowRight') {
    e.preventDefault();
    if (state.isHost && !btnSeekFwd.disabled) btnSeekFwd.click();
  } else if (e.code === 'KeyM') {
    e.preventDefault();
    toggleMute();
  } else if (e.code === 'ArrowUp') {
    e.preventDefault();
    const newVol = Math.min(1, (audioEngine.volume || 1) + 0.05);
    if (volumeSlider) { volumeSlider.value = newVol; volumeSlider.dispatchEvent(new Event('input')); }
  } else if (e.code === 'ArrowDown') {
    e.preventDefault();
    const newVol = Math.max(0, (audioEngine.volume || 1) - 0.05);
    if (volumeSlider) { volumeSlider.value = newVol; volumeSlider.dispatchEvent(new Event('input')); }
  }
});

// ─── URL Deep-link invite query handling ──────────────────────────────────────
function checkUrlRoomParam() {
  try {
    const params = new URLSearchParams(window.location.search);
    const roomParam = params.get('room');
    if (roomParam) {
      const clean = roomParam.trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 10);
      if (clean && inputRoomCode) {
        inputRoomCode.value = clean;
        showToast(`🎵 Room code "${clean}" loaded from link!`);
        if (inputJoinName && !inputJoinName.value) {
          inputJoinName.focus();
        }
      }
    }
  } catch (_) {}
}

checkUrlRoomParam();
connectSocket();
syncClock();
setInterval(syncClock, 60000); // Background NTP clock re-sync every 60 seconds
