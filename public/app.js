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
  audioOffsetMs: 0,    // user-set device speaker latency compensation (ms)
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
    this.startTime = 0;       // AudioContext.currentTime when playback started
    this.startPosition = 0;   // Song position (seconds) when started
    this.isPlaying = false;
    this.duration = 0;
    this.onEnded = null;
    this.volume = 1.0;
  }

  init() {
    if (!this.ctx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      this.ctx = new AudioCtx();
      this.gainNode = this.ctx.createGain();
      this.gainNode.connect(this.ctx.destination);
    }
    if (this.ctx.state === 'suspended') {
      this.ctx.resume().catch(() => {});
    }
  }

  async loadTrack(url) {
    this.init();
    this.stop();
    this.buffer = null;
    this.duration = 0;

    const res = await fetch(url);
    if (!res.ok) throw new Error('Failed to fetch audio stream');
    const arrayBuf = await res.arrayBuffer();
    this.buffer = await this.ctx.decodeAudioData(arrayBuf);
    this.duration = this.buffer.duration;
    return this.duration;
  }

  playAt(position, playAtServerTime, clockOffset) {
    this.init();
    this.stop();
    if (!this.buffer) return;

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
  }

  pause(position = null) {
    if (position !== null) {
      this.startPosition = position;
    } else {
      this.startPosition = this.getCurrentTime();
    }
    this.stop();
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
const audio = $('audio-el');

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
const artBars         = $('art-bars');
const displayTrack    = $('display-track-name');
const controlsBar     = $('controls-bar');
const btnPlayPause    = $('btn-play-pause');
const iconPlay        = $('icon-play');
const iconPause       = $('icon-pause');
const btnSeekBack     = $('btn-seek-back');
const btnSeekFwd      = $('btn-seek-fwd');
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
  setSyncStatus('syncing');
  const samples = [];
  
  // Use Socket.io WebSocket ping if available for ultra-low latency, fallback to HTTP
  const getPing = () => {
    if (state.socket && state.socket.connected) {
      return new Promise(resolve => {
        state.socket.emit('ping-clock', ({ serverTime }) => resolve(serverTime));
      });
    }
    return fetch('/ping').then(r => r.json()).then(d => d.serverTime);
  };

  for (let i = 0; i < 5; i++) {
    const t1 = Date.now();
    const serverTime = await getPing();
    const t3 = Date.now();
    if (serverTime) {
      samples.push(serverTime - (t1 + t3) / 2);
    }
    await new Promise(r => setTimeout(r, 60));
  }
  if (samples.length > 0) {
    samples.sort((a, b) => a - b);
    state.clockOffset = samples[Math.floor(samples.length / 2)];
    setSyncStatus('synced');
    console.log(`[NTP] offset=${state.clockOffset.toFixed(1)}ms`);
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
  if (state.socket) state.socket.disconnect();
  state.socket = io();

  state.socket.on('connect', () => {
    console.log('[Socket]', state.socket.id);
    // Auto-reconnect to room if already joined
    if (state.roomId) {
      console.log('[Socket] Rejoining room:', state.roomId);
      state.socket.emit('join-room', {
        roomId: state.roomId,
        userName: state.myName,
        userToken: getUserToken()
      }, res => {
        if (res && res.success) {
          state.isHost = res.isHost;
          applyMode();
          renderUsers(res.room.users);
          showToast('🟢 Reconnected to room');
        }
      });
    }
  });

  state.socket.on('disconnect', (reason) => {
    if (reason === 'io client disconnect') return;
    setSyncStatus('error');
    showToast('⚠️ Disconnected from server. Reconnecting…');
  });

  state.socket.on('user-status-changed', ({ users }) => {
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
  state.socket.on('track-loaded', ({ trackName, audioUrl, playlist, currentTrackIndex, autoPlay, playAt }) => {
    opProgressWrap.classList.add('hidden');
    state.roomIsPlaying = !!autoPlay;
    state.offsetHistory = [];
    setPlayingVisuals(!!autoPlay);
    loadAudio(audioUrl, trackName, 0, () => {
      if (autoPlay && playAt) {
        schedulePlay(0, playAt);
      }
    });
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
    if (!audioEngine.isPlaying || !isPlaying || state.isSeeking || !audioEngine.duration) return;

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


// ─── Load audio ───────────────────────────────────────────────────────────────
async function loadAudio(url, trackName, startPos = 0, onReadyCallback = null) {
  clearTimeout(state.sync.playTimeout);
  audioEngine.pause();
  setPlayingVisuals(false);
  state.sync.active = false;

  hostPrompt.classList.add('hidden');
  guestWaiting.classList.add('hidden');
  playerCard.classList.remove('hidden');
  controlsBar.classList.remove('hidden');
  displayTrack.textContent = trackName || 'Unknown Track';

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

volumeSlider.addEventListener('input', () => { audioEngine.setVolume(parseFloat(volumeSlider.value)); });

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
offsetSlider.addEventListener('input', () => {
  state.audioOffsetMs = parseInt(offsetSlider.value, 10);
  const sign = state.audioOffsetMs > 0 ? '+' : '';
  offsetDisplay.textContent = `${sign}${state.audioOffsetMs} ms`;
  state.offsetHistory = []; // Reset moving average so new offset takes effect immediately
});

audioEngine.onEnded = () => {
  setPlayingVisuals(false);
  state.roomIsPlaying = false;
  if (state.isHost && state.playlist && state.currentTrackIndex + 1 < state.playlist.length) {
    const nextIdx = state.currentTrackIndex + 1;
    showToast(`▶ Auto-playing next in queue: "${state.playlist[nextIdx].trackName}"`);
    state.socket.emit('select-track', { index: nextIdx, autoPlay: true });
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
  if (!file.type.startsWith('audio/') && file.type !== 'video/mp4') {
    showErr(fileError, 'Please select an audio file.'); return;
  }
  if (file.size > 50 * 1024 * 1024) { showErr(fileError, 'Max 50 MB.'); return; }

  fileError.classList.add('hidden');
  opProgressWrap.classList.remove('hidden');
  opProgressBar.style.width = '0%';
  opProgressLabel.textContent = 'Uploading…';

  const fd = new FormData();
  fd.append('audio', file);

  const xhr = new XMLHttpRequest();
  xhr.open('POST', `/upload/${state.roomId}`);
  xhr.setRequestHeader('x-user-name', state.myName);

  xhr.upload.addEventListener('progress', e => {
    if (e.lengthComputable) {
      const pct = Math.round(e.loaded / e.total * 100);
      opProgressBar.style.width = pct + '%';
      opProgressLabel.textContent = `Uploading… ${pct}%`;
    }
  });
  xhr.addEventListener('load', () => {
    opProgressWrap.classList.add('hidden');
    if (xhr.status !== 200) {
      const err = JSON.parse(xhr.responseText || '{}');
      showErr(fileError, err.error || 'Upload failed.');
    }
    // track-loaded socket event handles the rest
  });
  xhr.addEventListener('error', () => {
    opProgressWrap.classList.add('hidden');
    showErr(fileError, 'Network error during upload.');
  });
  xhr.send(fd);
}


// ─── Render Users & Playlist ──────────────────────────────────────────────────

function renderUsers(users) {
  usersList.innerHTML = '';
  userCount.textContent = users.length;
  users.forEach(u => {
    const li = document.createElement('li');
    li.className = 'user-item' + (u.offline ? ' user-offline' : '');
    const isMe = u.id === state.socket?.id || u.userToken === getUserToken();
    li.innerHTML = `
      <div class="user-avatar">${esc(u.name.slice(0,2).toUpperCase())}</div>
      <span class="user-name">${esc(u.name)}</span>
      ${u.isHost  ? '<span class="user-tag tag-host">Host</span>' : ''}
      ${u.offline ? '<span class="user-tag tag-offline">Reconnecting…</span>' : ''}
      ${isMe      ? '<span class="user-tag tag-you">You</span>'   : ''}
    `;
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

    li.innerHTML = `
      <div class="playlist-icon">${isCurrent ? '▶' : (idx + 1)}</div>
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
      showToast(`Switching to "${track.trackName}"…`);
      state.socket.emit('select-track', { index: idx, autoPlay: true });
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
  if (!state.socket || !state.socket.connected) { connectSocket(); await syncClock(); }
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
  if (!state.socket || !state.socket.connected) { connectSocket(); await syncClock(); }
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
    });
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

btnLeave.addEventListener('click', () => {
  audioEngine.stop();
  clearTimeout(state.sync.playTimeout);
  clearInterval(state.progressInterval);
  clearInterval(state.driftInterval);
  state.sync.active = false;
  if (state.socket) {
    state.socket.emit('leave-room');
  }

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
  usersList.innerHTML = '';
  artBars.classList.remove('playing');
  setPlayingVisuals(false);
  state.roomId = null; state.isHost = false;

  showPage('landing');
  connectSocket(); syncClock();
});


// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('touchstart', () => audioEngine.init(), { once: true });
document.addEventListener('click', () => audioEngine.init(), { once: true });
connectSocket();
syncClock();
setInterval(syncClock, 60000); // Background NTP clock re-sync every 60 seconds
