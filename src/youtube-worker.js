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
const ORIGIN = 'https://www.youtube.com';

// Multiple client configs — different rate limit pools
const CLIENTS = [
  {
    name: 'IOS', id: '5',
    clientName: 'iOS', clientVersion: '20.11.6',
    ua: 'com.google.ios.youtube/20.11.6 (iPhone10,4; U; CPU iOS 16_7_7 like Mac OS X)',
    extra: { deviceMake: 'Apple', deviceModel: 'iPhone10,4', osName: 'iOS', osVersion: '16.7.7.20H330', platform: 'MOBILE' },
  },
  {
    name: 'ANDROID', id: '3',
    clientName: 'ANDROID', clientVersion: '21.03.36',
    ua: 'com.google.android.youtube/21.03.36(Linux; U; Android 16; en_US; SM-S908E Build/TP1A.220624.014) gzip',
    extra: { deviceMake: 'Samsung', deviceModel: 'SM-S908E', osName: 'Android', osVersion: '16', platform: 'MOBILE', androidSdkVersion: 36 },
  },
  {
    name: 'TV', id: '7',
    clientName: 'TVHTML5_SIMPLY_EMBEDDED_PLAYER', clientVersion: '2.0',
    ua: 'Mozilla/5.0 (SMART-TV; LINUX; Tizen 5.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/5.0 TV Safari/537.36',
    extra: { platform: 'TV' },
  },
];

// WEB client — used with auth cookies for highest reliability
const WEB_CLIENT = {
  name: 'WEB_AUTH', id: '1',
  clientName: 'WEB', clientVersion: '2.20250320.01.00',
  ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
  extra: { platform: 'DESKTOP', browserName: 'Chrome', browserVersion: '134.0.0.0', osName: 'Windows', osVersion: '10.0' },
};

// ============== SAPISIDHASH AUTH ==============

async function generateSapisidHash(sapisid) {
  const ts = Math.floor(Date.now() / 1000);
  const input = `${ts} ${sapisid} ${ORIGIN}`;
  const hash = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(input));
  const hex = [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, '0')).join('');
  return `SAPISIDHASH ${ts}_${hex}`;
}

function parseCookies(cookieJson) {
  try {
    const cookies = JSON.parse(cookieJson);
    const map = {};
    for (const c of cookies) map[c.name] = c.value;
    return map;
  } catch { return null; }
}

function buildCookieHeader(cookieMap) {
  const needed = ['SID', 'HSID', 'SSID', 'APISID', 'SAPISID', '__Secure-1PSID', '__Secure-3PSID', 'LOGIN_INFO'];
  return needed.filter(n => cookieMap[n]).map(n => `${n}=${cookieMap[n]}`).join('; ');
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

async function fetchPlayer(videoId, visitorData, poToken, client) {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 8000);
  try {
    const resp = await fetch(
      `https://www.youtube.com/youtubei/v1/player?prettyPrint=false&alt=json&key=${INNERTUBE_KEY}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': client.ua,
          'X-Goog-Visitor-Id': visitorData,
          'X-Youtube-Client-Name': client.id,
          'X-Youtube-Client-Version': client.clientVersion,
        },
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

async function fetchPlayerAuth(videoId, cookieMap) {
  const sapisid = cookieMap.SAPISID;
  if (!sapisid) return null;

  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 8000);
  try {
    const authHeader = await generateSapisidHash(sapisid);
    const visitorData = generateVisitorData();
    const resp = await fetch(
      `https://www.youtube.com/youtubei/v1/player?prettyPrint=false&alt=json&key=${INNERTUBE_KEY}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': WEB_CLIENT.ua,
          'X-Goog-Visitor-Id': visitorData,
          'X-Youtube-Client-Name': WEB_CLIENT.id,
          'X-Youtube-Client-Version': WEB_CLIENT.clientVersion,
          'Authorization': authHeader,
          'Cookie': buildCookieHeader(cookieMap),
          'Origin': ORIGIN,
          'Referer': `${ORIGIN}/`,
          'X-Origin': ORIGIN,
        },
        body: JSON.stringify(buildPlayerBody(videoId, visitorData, null, WEB_CLIENT)),
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

async function resolveYouTube(youtubeKey, db, cookieMap) {
  if (!youtubeKey) return null;

  // Check D1 cache (cached trailer URLs, not tokens)
  const cached = await d1Get(db, `yt:v2:${youtubeKey}`);
  if (cached) return cached;

  // Priority 1: Authenticated WEB client (if cookies configured)
  if (cookieMap?.SAPISID) {
    try {
      const data = await fetchPlayerAuth(youtubeKey, cookieMap);
      if (data?.playabilityStatus?.status === 'OK' && data.streamingData) {
        const result = extractResult(data.streamingData);
        if (result) {
          await d1Set(db, `yt:v2:${youtubeKey}`, result, 21600);
          return result;
        }
      }
    } catch { /* fall through to anonymous clients */ }
  }

  // Priority 2: Anonymous clients — fresh visitor per attempt (different rate limit pools)
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

async function resolveYouTubeDebug(videoId, db, cookieMap) {
  const stages = { videoId, timestamp: new Date().toISOString(), auth: !!cookieMap?.SAPISID, clients: {} };

  // Test auth client if available
  if (cookieMap?.SAPISID) {
    try {
      const data = await fetchPlayerAuth(videoId, cookieMap);
      const sd = data?.streamingData;
      stages.clients['WEB_AUTH'] = {
        playability: data?.playabilityStatus?.status || 'unknown',
        reason: data?.playabilityStatus?.reason || null,
        muxedFormats: sd?.formats?.length || 0,
        adaptiveFormats: (sd?.adaptiveFormats || []).filter(f => f.mimeType?.startsWith('video/')).length,
        hlsManifestUrl: sd?.hlsManifestUrl ? 'yes' : 'no',
      };
    } catch (e) { stages.clients['WEB_AUTH'] = { error: e.message }; }
  }

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

  const result = await resolveYouTube(videoId, db, cookieMap);
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
      // Parse auth cookies from secret (if configured)
      const cookieMap = env.YOUTUBE_COOKIES ? parseCookies(env.YOUTUBE_COOKIES) : null;

      if (pathname === '/resolve' && request.method === 'POST') {
        const { key } = await request.json();
        const result = await resolveYouTube(key, env.DB, cookieMap);
        return new Response(JSON.stringify(result), { headers });
      }

      const debugMatch = pathname.match(/^\/debug\/(.+)$/);
      if (debugMatch) {
        const result = await resolveYouTubeDebug(debugMatch[1], env.DB, cookieMap);
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
