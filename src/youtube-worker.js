// YouTube Worker — Zero dependencies, pure fetch() + inlined po_token
// Replaces youtubei.js (1.5MB), @cf-wasm/quickjs (500KB), bgutils-js (50KB)

// ============== D1 CACHE HELPERS ==============

async function d1Get(db, key) {
  if (!db) return null;
  try {
    const row = await db.prepare('SELECT value, expires_at FROM cache WHERE key = ?').bind(key).first();
    if (!row || row.expires_at < Math.floor(Date.now() / 1000)) return null;
    return JSON.parse(row.value);
  } catch { return null; }
}

async function d1Set(db, key, value, ttlSeconds) {
  if (!db) return;
  try {
    const expires = Math.floor(Date.now() / 1000) + ttlSeconds;
    await db.prepare('INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)')
      .bind(key, JSON.stringify(value), expires).run();
  } catch { /* silent */ }
}

// ============== PO_TOKEN (inlined from bgutils-js) ==============

function generateVisitorData() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let id = '';
  for (let i = 0; i < 11; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

function generateColdStartToken(identifier) {
  const enc = new TextEncoder().encode(identifier);
  const ts = Math.floor(Date.now() / 1000);
  const k1 = Math.floor(Math.random() * 256);
  const k2 = Math.floor(Math.random() * 256);
  const header = [k1, k2, 0, 1,
    (ts >> 24) & 0xFF, (ts >> 16) & 0xFF, (ts >> 8) & 0xFF, ts & 0xFF];
  const packet = new Uint8Array(2 + header.length + enc.length);
  packet[0] = 0x22;
  packet[1] = header.length + enc.length;
  packet.set(header, 2);
  packet.set(enc, 2 + header.length);
  const payload = packet.subarray(2);
  for (let i = 2; i < payload.length; i++) payload[i] ^= payload[i % 2];
  return btoa(String.fromCharCode(...packet)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

// ============== INNERTUBE PLAYER API (replaces youtubei.js) ==============

const INNERTUBE_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8';

// Apple ecosystem clients — all return HLS, different rate limit pools
const CLIENTS = [
  {
    name: 'IOS', id: '5',
    clientName: 'iOS', clientVersion: '20.11.6',
    ua: 'com.google.ios.youtube/20.11.6 (iPhone10,4; U; CPU iOS 16_7_7 like Mac OS X)',
    extra: { deviceMake: 'Apple', deviceModel: 'iPhone10,4', osName: 'iOS', osVersion: '16.7.7.20H330', platform: 'MOBILE' },
  },
  {
    name: 'TVOS', id: '5',
    clientName: 'iOS', clientVersion: '20.11.6',
    ua: 'com.google.ios.youtube/20.11.6 (AppleTV11,1; U; CPU OS 18_3 like Mac OS X)',
    extra: { deviceMake: 'Apple', deviceModel: 'AppleTV11,1', osName: 'tvOS', osVersion: '18.3', platform: 'MOBILE' },
  },
  {
    name: 'IPADOS', id: '5',
    clientName: 'iOS', clientVersion: '20.11.6',
    ua: 'com.google.ios.youtube/20.11.6 (iPad13,18; U; CPU OS 17_7_6 like Mac OS X)',
    extra: { deviceMake: 'Apple', deviceModel: 'iPad13,18', osName: 'iPadOS', osVersion: '17.7.6', platform: 'MOBILE' },
  },
];

// ============== OAUTH TOKEN MANAGEMENT ==============

const OAUTH_CLIENT_ID = '861556708454-d6dlm3lh05idd8npek18k6be8ba3oc68.apps.googleusercontent.com';
const OAUTH_CLIENT_SECRET = 'SboVhoG9s0rNafixCSGGKXAT';

// In-memory token cache (lives until worker eviction, ~5-30 min)
let cachedAccessToken = null;
let tokenExpiresAt = 0;

async function getAccessToken(refreshToken) {
  if (cachedAccessToken && Date.now() < tokenExpiresAt) return cachedAccessToken;

  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${OAUTH_CLIENT_ID}&client_secret=${OAUTH_CLIENT_SECRET}&refresh_token=${refreshToken}&grant_type=refresh_token`,
  });
  const data = await resp.json();
  if (!data.access_token) return null;

  cachedAccessToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in - 60) * 1000; // refresh 60s early
  return cachedAccessToken;
}

// ============== PLAYER FETCH ==============

function buildPlayerBody(videoId, visitorData, poToken, client) {
  const body = {
    context: {
      client: {
        hl: 'en', gl: 'US', visitorData,
        clientName: client.clientName, clientVersion: client.clientVersion,
        ...client.extra,
      },
      user: { enableSafetyMode: false },
      request: { useSsl: true, internalExperimentFlags: [] },
    },
    videoId,
    racyCheckOk: true,
    contentCheckOk: true,
  };
  if (poToken) body.serviceIntegrityDimensions = { poToken };
  return body;
}

async function fetchPlayer(videoId, visitorData, poToken, client, accessToken) {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 8000);
  try {
    const hdrs = {
      'Content-Type': 'application/json',
      'User-Agent': client.ua,
      'X-Goog-Visitor-Id': visitorData,
      'X-Youtube-Client-Name': client.id,
      'X-Youtube-Client-Version': client.clientVersion,
    };
    // OAuth Bearer token for authenticated requests
    if (accessToken) {
      hdrs['Authorization'] = `Bearer ${accessToken}`;
    }
    const resp = await fetch(
      `https://www.youtube.com/youtubei/v1/player?prettyPrint=false&alt=json&key=${INNERTUBE_KEY}`, {
        method: 'POST',
        headers: hdrs,
        body: JSON.stringify(buildPlayerBody(videoId, visitorData, poToken, client)),
        signal: controller.signal,
      }
    );
    clearTimeout(tid);
    return await resp.json();
  } catch {
    clearTimeout(tid);
    return null;
  }
}

// ============== YOUTUBE RESOLVER ==============

function extractResult(sd) {
  // Priority 1: Muxed MP4 (video+audio, direct play)
  const formats = sd.formats || [];
  let bestMuxed = null;
  for (const f of formats) {
    if (f.url && (!bestMuxed || (f.height || 0) > bestMuxed.height)) {
      bestMuxed = { url: f.url, height: f.height || 0, width: f.width || 0, bitrate: f.bitrate || 0, qualityLabel: f.qualityLabel };
    }
  }
  if (bestMuxed) {
    return {
      url: bestMuxed.url,
      provider: `YouTube ${bestMuxed.qualityLabel || bestMuxed.height + 'p'}`,
      bitrate: Math.round((bestMuxed.bitrate || 0) / 1000),
      width: bestMuxed.width, height: bestMuxed.height,
    };
  }

  // Priority 2: HLS manifest (AVFoundation native)
  if (sd.hlsManifestUrl) {
    const adaptive = (sd.adaptiveFormats || [])
      .filter(f => f.mimeType?.startsWith('video/'))
      .sort((a, b) => (b.height || 0) - (a.height || 0));
    const best = adaptive[0];
    return {
      url: sd.hlsManifestUrl,
      provider: `YouTube ${best?.qualityLabel || 'HLS'}`,
      bitrate: best?.bitrate ? Math.round(best.bitrate / 1000) : 0,
      width: best?.width || 0, height: best?.height || 0,
    };
  }
  return null;
}

async function resolveYouTube(youtubeKey, db, refreshToken) {
  if (!youtubeKey) return null;

  // Check D1 cache (cached trailer URLs, not tokens)
  const cached = await d1Get(db, `yt:v2:${youtubeKey}`);
  if (cached) return cached;

  // Get OAuth access token if refresh token is configured
  const accessToken = refreshToken ? await getAccessToken(refreshToken) : null;

  // Priority 1: Authenticated Apple clients (OAuth + HLS 1080p+)
  if (accessToken) {
    for (const client of CLIENTS) {
      try {
        const visitorData = generateVisitorData();
        const poToken = generateColdStartToken(visitorData);
        const data = await fetchPlayer(youtubeKey, visitorData, poToken, client, accessToken);
        if (!data) continue;

        const status = data.playabilityStatus?.status;
        if (status !== 'OK' || !data.streamingData) continue;

        const result = extractResult(data.streamingData);
        if (result) {
          await d1Set(db, `yt:v2:${youtubeKey}`, result, 21600);
          return result;
        }
      } catch { /* try next client */ }
    }
  }

  // Priority 2: Anonymous Apple clients (fallback)
  for (const client of CLIENTS) {
    try {
      const visitorData = generateVisitorData();
      const poToken = generateColdStartToken(visitorData);
      const data = await fetchPlayer(youtubeKey, visitorData, poToken, client);
      if (!data) continue;

      const status = data.playabilityStatus?.status;
      if (status === 'LOGIN_REQUIRED' || status === 'ERROR') continue;
      if (status !== 'OK' || !data.streamingData) continue;

      const result = extractResult(data.streamingData);
      if (result) {
        await d1Set(db, `yt:v2:${youtubeKey}`, result, 21600);
        return result;
      }
    } catch { /* try next client */ }
  }

  return null;
}

// ============== DEBUG ENDPOINT ==============

async function resolveYouTubeDebug(videoId, db, refreshToken) {
  const accessToken = refreshToken ? await getAccessToken(refreshToken) : null;
  const stages = { videoId, timestamp: new Date().toISOString(), oauth: !!accessToken, clients: {} };

  // Test authenticated Apple clients
  if (accessToken) {
    for (const client of CLIENTS) {
      const visitorData = generateVisitorData();
      const poToken = generateColdStartToken(visitorData);
      const data = await fetchPlayer(videoId, visitorData, poToken, client, accessToken);
      if (!data) { stages.clients[`${client.name}_AUTH`] = { error: 'fetch failed' }; continue; }

      const sd = data.streamingData;
      stages.clients[`${client.name}_AUTH`] = {
        playability: data.playabilityStatus?.status || 'unknown',
        reason: data.playabilityStatus?.reason || null,
        muxedFormats: sd?.formats?.length || 0,
        adaptiveFormats: (sd?.adaptiveFormats || []).filter(f => f.mimeType?.startsWith('video/')).length,
        hlsManifestUrl: sd?.hlsManifestUrl ? 'yes' : 'no',
      };
    }
  }

  // Test anonymous Apple clients
  for (const client of CLIENTS) {
    const visitorData = generateVisitorData();
    const poToken = generateColdStartToken(visitorData);
    const data = await fetchPlayer(videoId, visitorData, poToken, client);
    if (!data) { stages.clients[client.name] = { error: 'fetch failed' }; continue; }

    const sd = data.streamingData;
    stages.clients[client.name] = {
      playability: data.playabilityStatus?.status || 'unknown',
      reason: data.playabilityStatus?.reason || null,
      muxedFormats: sd?.formats?.length || 0,
      adaptiveFormats: (sd?.adaptiveFormats || []).filter(f => f.mimeType?.startsWith('video/')).length,
      hlsManifestUrl: sd?.hlsManifestUrl ? 'yes' : 'no',
    };
  }

  const result = await resolveYouTube(videoId, db, refreshToken);
  stages.resolverResult = result ? result.provider : 'null';
  return stages;
}

// ============== REQUEST HANDLER ==============

export default {
  async fetch(request, env) {
    const headers = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json',
    };

    if (request.method === 'OPTIONS') return new Response(null, { headers });

    const url = new URL(request.url);
    const pathname = url.pathname;

    try {
      const refreshToken = env.YOUTUBE_REFRESH_TOKEN || null;

      if (pathname === '/resolve' && request.method === 'POST') {
        const { key } = await request.json();
        const result = await resolveYouTube(key, env.DB, refreshToken);
        return new Response(JSON.stringify(result), { headers });
      }

      const debugMatch = pathname.match(/^\/debug\/(.+)$/);
      if (debugMatch) {
        const result = await resolveYouTubeDebug(debugMatch[1], env.DB, refreshToken);
        return new Response(JSON.stringify(result, null, 2), { headers });
      }

      if (pathname === '/health') {
        return new Response(JSON.stringify({ status: 'ok', worker: 'youtube-lean', edge: request.cf?.colo }), { headers });
      }

      return new Response(JSON.stringify({ error: 'Not found' }), { status: 404, headers });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message || 'Internal error' }), { status: 500, headers });
    }
  }
};
