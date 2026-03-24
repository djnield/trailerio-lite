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
const IOS_UA = 'com.google.ios.youtube/20.11.6 (iPhone10,4; U; CPU iOS 16_7_7 like Mac OS X)';

function buildPlayerBody(videoId, visitorData, poToken) {
  const body = {
    context: {
      client: {
        hl: 'en', gl: 'US', visitorData,
        clientName: 'iOS', clientVersion: '20.11.6',
        deviceMake: 'Apple', deviceModel: 'iPhone10,4',
        osName: 'iOS', osVersion: '16.7.7.20H330',
        platform: 'MOBILE',
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

async function fetchPlayer(videoId, visitorData, poToken) {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 8000);
  try {
    const resp = await fetch(
      `https://www.youtube.com/youtubei/v1/player?prettyPrint=false&alt=json&key=${INNERTUBE_KEY}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': IOS_UA,
          'X-Goog-Visitor-Id': visitorData,
          'X-Youtube-Client-Name': '5',
          'X-Youtube-Client-Version': '20.11.6',
        },
        body: JSON.stringify(buildPlayerBody(videoId, visitorData, poToken)),
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

async function resolveYouTube(youtubeKey, db) {
  if (!youtubeKey) return null;

  // Check D1 cache (cached trailer URLs, not tokens)
  const cached = await d1Get(db, `yt:v1:${youtubeKey}`);
  if (cached) return cached;

  // Fresh visitor + token per call (avoids multi-user token exhaustion)
  const visitorData = generateVisitorData();
  const poToken = generateColdStartToken(visitorData);

  const data = await fetchPlayer(youtubeKey, visitorData, poToken);
  if (!data || data.playabilityStatus?.status !== 'OK') return null;

  const sd = data.streamingData;
  if (!sd) return null;

  let result = null;

  // Priority 1: Muxed MP4 (video+audio, direct play)
  const formats = sd.formats || [];
  let bestMuxed = null;
  for (const f of formats) {
    if (f.url && (!bestMuxed || (f.height || 0) > bestMuxed.height)) {
      bestMuxed = { url: f.url, height: f.height || 0, width: f.width || 0, bitrate: f.bitrate || 0, qualityLabel: f.qualityLabel };
    }
  }

  if (bestMuxed) {
    result = {
      url: bestMuxed.url,
      provider: `YouTube ${bestMuxed.qualityLabel || bestMuxed.height + 'p'}`,
      bitrate: Math.round((bestMuxed.bitrate || 0) / 1000),
      width: bestMuxed.width,
      height: bestMuxed.height,
    };
  }

  // Priority 2: HLS manifest (AVFoundation native)
  if (!result && sd.hlsManifestUrl) {
    const adaptive = (sd.adaptiveFormats || [])
      .filter(f => f.mimeType?.startsWith('video/'))
      .sort((a, b) => (b.height || 0) - (a.height || 0));
    const best = adaptive[0];
    result = {
      url: sd.hlsManifestUrl,
      provider: `YouTube ${best?.qualityLabel || 'HLS'}`,
      bitrate: best?.bitrate ? Math.round(best.bitrate / 1000) : 0,
      width: best?.width || 0,
      height: best?.height || 0,
    };
  }

  // Cache in D1 for 6 hours (YouTube URLs expire in ~6h)
  if (result) d1Set(db, `yt:v1:${youtubeKey}`, result, 21600);
  return result;
}

// ============== DEBUG ENDPOINT ==============

async function resolveYouTubeDebug(videoId, db) {
  const stages = { videoId, timestamp: new Date().toISOString() };

  const visitorData = generateVisitorData();
  const poToken = generateColdStartToken(visitorData);
  stages.visitorData = visitorData;
  stages.poTokenType = 'cold-start-fresh';

  const data = await fetchPlayer(videoId, visitorData, poToken);
  if (!data) { stages.error = 'fetchPlayer returned null'; return stages; }

  stages.playability = data.playabilityStatus?.status || 'unknown';
  stages.reason = data.playabilityStatus?.reason || null;

  const sd = data.streamingData;
  if (sd) {
    stages.muxedFormats = sd.formats?.length || 0;
    stages.adaptiveFormats = (sd.adaptiveFormats || []).filter(f => f.mimeType?.startsWith('video/')).length;
    stages.hlsManifestUrl = sd.hlsManifestUrl ? 'yes' : 'no';

    const bestAdaptive = (sd.adaptiveFormats || [])
      .filter(f => f.mimeType?.startsWith('video/'))
      .sort((a, b) => (b.height || 0) - (a.height || 0))[0];
    stages.bestAdaptive = bestAdaptive ? `${bestAdaptive.qualityLabel} ${bestAdaptive.mimeType?.split(';')[0]}` : 'none';
  }

  // Test resolution
  const result = await resolveYouTube(videoId, db);
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
      if (pathname === '/resolve' && request.method === 'POST') {
        const { key } = await request.json();
        const result = await resolveYouTube(key, env.DB);
        return new Response(JSON.stringify(result), { headers });
      }

      const debugMatch = pathname.match(/^\/debug\/(.+)$/);
      if (debugMatch) {
        const result = await resolveYouTubeDebug(debugMatch[1], env.DB);
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
