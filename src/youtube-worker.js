// YouTube Worker - Dedicated Cloudflare Worker for YouTube trailer resolution
// Separated from main fusion worker to isolate memory-heavy dependencies

import { Innertube, Platform } from 'youtubei.js/web';
import { getQuickJSWASMModule } from '@cf-wasm/quickjs/workerd';
import { BG } from 'bgutils-js';

// ============== UTILITIES ==============

async function fetchWithTimeout(url, options = {}, timeout = 6000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
}

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

// ============== PO_TOKEN GENERATION (BotGuard attestation) ==============

function createBotGuardEnv() {
  const el = () => ({
    style: {}, dataset: {}, classList: { add() {}, remove() {}, contains() { return false; } },
    appendChild(c) { return c; }, removeChild(c) { return c; }, remove() {},
    setAttribute() {}, getAttribute() { return null; }, getElementsByTagName() { return []; },
    querySelector() { return null; }, querySelectorAll() { return []; },
    addEventListener() {}, removeEventListener() {}, dispatchEvent() { return true; },
    innerHTML: '', textContent: '', innerText: '', offsetWidth: 1, offsetHeight: 1,
    getBoundingClientRect() { return { top: 0, left: 0, bottom: 0, right: 0, width: 1, height: 1 }; },
  });

  const globalObj = {
    document: {
      createElement: () => el(), createElementNS: () => el(), createTextNode: () => el(),
      getElementById: () => null, querySelector: () => null, querySelectorAll: () => [],
      getElementsByTagName: () => [], getElementsByClassName: () => [],
      documentElement: { style: {}, getAttribute() { return null; } },
      head: el(), body: el(), cookie: '',
      addEventListener() {}, removeEventListener() {},
    },
    navigator: {
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      language: 'en-US', languages: ['en-US', 'en'], platform: 'MacIntel',
      webdriver: false, plugins: { length: 0 }, mimeTypes: { length: 0 },
      hardwareConcurrency: 8, maxTouchPoints: 0,
      connection: { effectiveType: '4g' },
    },
    performance: { now: () => Date.now(), mark() {}, measure() {}, getEntriesByName() { return []; } },
    location: { href: 'https://www.youtube.com', origin: 'https://www.youtube.com', protocol: 'https:', hostname: 'www.youtube.com' },
    screen: { width: 1920, height: 1080, colorDepth: 24 },
    history: { length: 1 },
    console, setTimeout, clearTimeout, setInterval, clearInterval,
    atob, btoa, fetch: (input, init) => fetch(input, init),
    TextEncoder, TextDecoder, URL, URLSearchParams,
    Uint8Array, Int32Array, ArrayBuffer, DataView,
    Object, Array, String, Number, Boolean, RegExp, Date, Math, JSON, Promise, Symbol, Map, Set, WeakMap, WeakSet, Proxy, Reflect,
    Error, TypeError, RangeError, SyntaxError,
    parseInt, parseFloat, isNaN, isFinite, encodeURIComponent, decodeURIComponent, encodeURI, decodeURI,
    crypto: { getRandomValues: (arr) => { for (let i = 0; i < arr.length; i++) arr[i] = Math.floor(Math.random() * 256); return arr; } },
    requestAnimationFrame: (cb) => setTimeout(cb, 16),
    cancelAnimationFrame: (id) => clearTimeout(id),
    getComputedStyle: () => new Proxy({}, { get: () => '' }),
    matchMedia: () => ({ matches: false, addListener() {}, removeListener() {} }),
    MutationObserver: class { observe() {} disconnect() {} },
    ResizeObserver: class { observe() {} disconnect() {} },
    IntersectionObserver: class { observe() {} disconnect() {} },
    HTMLElement: class {}, HTMLDivElement: class {}, HTMLScriptElement: class {},
    Event: class { constructor() {} preventDefault() {} stopPropagation() {} },
    CustomEvent: class { constructor() {} },
    XMLHttpRequest: class { open() {} send() {} setRequestHeader() {} addEventListener() {} },
  };

  globalObj.window = globalObj;
  globalObj.self = globalObj;
  globalObj.top = globalObj;
  globalObj.parent = globalObj;
  globalObj.globalThis = globalObj;
  return globalObj;
}

let _poToken = null;
let _poTokenExpiry = 0;
let _poTokenType = 'none';
let _poTokenError = null;
let _visitorData = null;

async function getPoToken(db = null) {
  // Return in-memory cached token if still valid (with 5 min buffer)
  if (_poToken && Date.now() < _poTokenExpiry - 300000) {
    return { poToken: _poToken, visitorData: _visitorData };
  }

  // Check D1 for cached po_token (survives cold starts)
  const d1Cached = await d1Get(db, 'yt:potoken');
  if (d1Cached && d1Cached.expiry > Date.now()) {
    _poToken = d1Cached.poToken;
    _visitorData = d1Cached.visitorData;
    _poTokenExpiry = d1Cached.expiry;
    _poTokenType = d1Cached.type || 'd1-cached';
    return { poToken: _poToken, visitorData: _visitorData };
  }

  // Generate fresh visitor_data
  if (!_visitorData) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < 11; i++) id += chars[Math.floor(Math.random() * chars.length)];
    _visitorData = id;
  }

  // Try full BotGuard minting (produces long-lived tokens ~1h)
  try {
    const bgEnv = createBotGuardEnv();
    const bgConfig = {
      fetch: (input, init) => fetch(input, init),
      requestKey: 'O43z0dpjhgX20SCx4KAo',
      globalObj: bgEnv,
      identifier: _visitorData,
      useYouTubeAPI: true,
    };

    const challenge = await BG.Challenge.create(bgConfig);

    let interpreterScript = challenge.interpreterJavascript.privateDoNotAccessOrElseSafeScriptWrappedValue;
    if (!interpreterScript) {
      const scriptUrl = challenge.interpreterJavascript.privateDoNotAccessOrElseTrustedResourceUrlWrappedValue;
      if (!scriptUrl) throw new Error('No interpreter script in challenge');
      const res = await fetch(scriptUrl);
      interpreterScript = await res.text();
    }

    if (!_quickjs) _quickjs = await getQuickJSWASMModule();
    const qjsVm = _quickjs.newContext();

    let poToken, estimatedTtlSecs;
    try {
      const mockDomCode = `
        var window = globalThis; var self = globalThis; var top = globalThis; var parent = globalThis;
        var document = {
          createElement: function() { return { style: {}, appendChild: function(c){return c}, removeChild: function(c){return c},
            setAttribute: function(){}, getAttribute: function(){return null}, addEventListener: function(){},
            removeEventListener: function(){}, getElementsByTagName: function(){return []},
            querySelector: function(){return null}, querySelectorAll: function(){return []},
            dispatchEvent: function(){return true}, remove: function(){},
            innerHTML: '', textContent: '', offsetWidth: 1, offsetHeight: 1, dataset: {},
            classList: {add:function(){},remove:function(){},contains:function(){return false}},
            getBoundingClientRect: function(){return {top:0,left:0,bottom:0,right:0,width:1,height:1}} }},
          createElementNS: function(){return document.createElement()},
          createTextNode: function(){return document.createElement()},
          getElementById: function(){return null}, querySelector: function(){return null},
          querySelectorAll: function(){return []}, getElementsByTagName: function(){return []},
          getElementsByClassName: function(){return []},
          documentElement: {style:{}, getAttribute:function(){return null}},
          head: {appendChild:function(c){return c}}, body: {appendChild:function(c){return c}},
          cookie: '', addEventListener: function(){}, removeEventListener: function(){}
        };
        var navigator = {
          userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          language: 'en-US', languages: ['en-US','en'], platform: 'MacIntel',
          webdriver: false, plugins: {length:0}, mimeTypes: {length:0},
          hardwareConcurrency: 8, maxTouchPoints: 0, connection: {effectiveType:'4g'}
        };
        var performance = {now: function(){return Date.now()}, mark:function(){}, measure:function(){}, getEntriesByName:function(){return []}};
        var location = {href:'https://www.youtube.com', origin:'https://www.youtube.com', protocol:'https:', hostname:'www.youtube.com'};
        var screen = {width:1920, height:1080, colorDepth:24};
        var history = {length:1};
        var crypto = {getRandomValues: function(a){for(var i=0;i<a.length;i++) a[i]=Math.floor(Math.random()*256);return a}};
        var requestAnimationFrame = function(cb){return setTimeout(cb,16)};
        var cancelAnimationFrame = function(id){clearTimeout(id)};
        var getComputedStyle = function(){return new Proxy({},{get:function(){return ''}})};
        var matchMedia = function(){return {matches:false,addListener:function(){},removeListener:function(){}}};
        var MutationObserver = function(){this.observe=function(){};this.disconnect=function(){}};
        var ResizeObserver = function(){this.observe=function(){};this.disconnect=function(){}};
        var IntersectionObserver = function(){this.observe=function(){};this.disconnect=function(){}};
        var HTMLElement = function(){}; var HTMLDivElement = function(){}; var HTMLScriptElement = function(){};
        var Event = function(){this.preventDefault=function(){};this.stopPropagation=function(){}};
        var CustomEvent = function(){}; var XMLHttpRequest = function(){this.open=function(){};this.send=function(){};this.setRequestHeader=function(){};this.addEventListener=function(){}};
      `;

      const program = challenge.program;
      const globalName = challenge.globalName;
      const snapshotCode = `
        var __webPoSignalOutput = [];
        var __syncSnapshot = null;
        try {
          var __vm = globalThis['${globalName}'];
          if (__vm && __vm.a) {
            __syncSnapshot = __vm.a(${JSON.stringify(program)}, function(asyncFn, shutdownFn, passFn, checkFn) {}, true, undefined, function(){}, [[], []])[0];
          }
        } catch(e) {}
        var __bgResponse = '';
        if (__syncSnapshot) {
          try { __bgResponse = __syncSnapshot([undefined, undefined, __webPoSignalOutput, undefined]) || ''; } catch(e) {}
        }
        JSON.stringify({r: __bgResponse, hasMinter: typeof __webPoSignalOutput[0] === 'function'});
      `;

      const fullCode = mockDomCode + ';\n' + interpreterScript + ';\n' + snapshotCode;
      const result = qjsVm.evalCode(fullCode);

      if (result.error) {
        const err = qjsVm.dump(result.error);
        result.error.dispose();
        throw new Error(`QuickJS BotGuard: ${JSON.stringify(err)}`);
      }

      const output = JSON.parse(qjsVm.dump(result.value));
      result.value.dispose();
      const botguardResponse = output.r;
      const hasMinter = output.hasMinter;
      if (!botguardResponse) throw new Error('BotGuard snapshot returned empty');

      // Exchange botguardResponse for integrity token
      const itPayload = [bgConfig.requestKey, botguardResponse];
      const itResponse = await fetch('https://www.youtube.com/api/jnn/v1/GenerateIT', {
        method: 'POST',
        headers: { 'content-type': 'application/json+protobuf', 'x-goog-api-key': 'AIzaSyDyT5W0Jh49F30Pqqtyfdf7pDLFKLJoAnw', 'x-user-agent': 'grpc-web-javascript/0.1', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36(KHTML, like Gecko)' },
        body: JSON.stringify(itPayload),
      });
      const itJson = await itResponse.json();
      const [integrityToken, estTtl, , websafeFallbackToken] = itJson;
      estimatedTtlSecs = estTtl;

      // Mint po_token inside QuickJS context
      if (hasMinter && integrityToken) {
        const b64 = integrityToken.replace(/-/g, '+').replace(/_/g, '/').replace(/\./g, '=');
        const raw = atob(b64);
        const tokenBytes = [];
        for (let i = 0; i < raw.length; i++) tokenBytes.push(raw.charCodeAt(i));
        const idBytes = [];
        for (let i = 0; i < _visitorData.length; i++) idBytes.push(_visitorData.charCodeAt(i));

        const mintCode = `
          (function() {
            var getMinter = __webPoSignalOutput[0];
            if (!getMinter) return JSON.stringify({err: 'no_minter'});
            try {
              var tokenBytes = new Uint8Array(${JSON.stringify(tokenBytes)});
              var mintCb = getMinter(tokenBytes);
              if (typeof mintCb === 'object' && mintCb && typeof mintCb.then === 'function') return JSON.stringify({err: 'async_minter'});
              if (typeof mintCb !== 'function') return JSON.stringify({err: 'mintcb_type_' + typeof mintCb});
              var idBytes = new Uint8Array(${JSON.stringify(idBytes)});
              var result = mintCb(idBytes);
              if (!result) return JSON.stringify({err: 'mint_null'});
              var arr = [];
              for (var i = 0; i < result.length; i++) arr.push(result[i]);
              return JSON.stringify({ok: arr});
            } catch(e) { return JSON.stringify({err: 'mint_error: ' + (e.message || e)}); }
          })();
        `;

        const mintResult = qjsVm.evalCode(mintCode);
        if (!mintResult.error) {
          const mintOutput = JSON.parse(qjsVm.dump(mintResult.value));
          mintResult.value.dispose();
          if (mintOutput.ok) {
            const bytes = String.fromCharCode(...mintOutput.ok);
            poToken = btoa(bytes).replace(/\+/g, '-').replace(/\//g, '_');
            _poTokenType = 'minted';
          } else {
            _poTokenError = mintOutput.err;
            poToken = websafeFallbackToken || integrityToken;
            _poTokenType = 'websafe-fallback';
          }
        } else {
          const err = qjsVm.dump(mintResult.error);
          mintResult.error.dispose();
          _poTokenError = `mint_eval: ${JSON.stringify(err)}`;
          poToken = websafeFallbackToken || integrityToken;
          _poTokenType = 'websafe-fallback';
        }
      } else {
        poToken = websafeFallbackToken || integrityToken;
        _poTokenType = hasMinter ? 'websafe-fallback' : 'no-minter-fallback';
      }

    } finally {
      qjsVm.dispose();
    }

    _poToken = poToken;
    _poTokenExpiry = Date.now() + ((estimatedTtlSecs || 3600) * 1000);
    if (_poTokenType === 'minted') _poTokenError = null;

    // Cache minted token in D1 (survives instance recycling)
    d1Set(db, 'yt:potoken', { poToken: _poToken, visitorData: _visitorData, expiry: _poTokenExpiry, type: _poTokenType }, Math.floor((_poTokenExpiry - Date.now()) / 1000));
    return { poToken: _poToken, visitorData: _visitorData };
  } catch (e) {
    _poTokenError = e.message || String(e);
    // Fallback: cold-start token (works for ~3 requests during SPS=2 grace period)
    const coldToken = BG.PoToken.generateColdStartToken(_visitorData);
    _poToken = coldToken;
    _poTokenExpiry = Date.now() + 600000; // 10 min
    _poTokenType = 'cold-start';
    d1Set(db, 'yt:potoken', { poToken: coldToken, visitorData: _visitorData, expiry: _poTokenExpiry, type: 'cold-start' }, 600);
    return { poToken: coldToken, visitorData: _visitorData };
  }
}

// ============== YOUTUBE RESOLVER ==============

const YOUTUBE_CLIENTS = ['IOS'];

async function getFormatUrl(f, player) {
  const raw = f.url || String(await f.decipher(player));
  return raw || null;
}

let _innertube = null;
let _innertubeRefresh = 0;
let _quickjs = null;

async function getInnertube(db = null) {
  const now = Date.now();
  if (_innertube && now - _innertubeRefresh < 15 * 60 * 1000) return _innertube;

  Platform.shim.fetch = (input, init) => fetch(input, init);

  const { poToken, visitorData } = await getPoToken(db);

  const createOpts = {
    retrieve_player: false, // IOS returns pre-signed URLs + HLS; player JS blows CPU budget on free plan
    generate_session_locally: true,
    enable_safety_mode: false,
  };
  if (poToken) createOpts.po_token = poToken;
  if (visitorData) createOpts.visitor_data = visitorData;

  _innertube = await Innertube.create(createOpts);
  _innertubeRefresh = now;
  return _innertube;
}

function parseHlsBestQuality(masterPlaylist) {
  const lines = masterPlaylist.split('\n');
  let bestBandwidth = 0;
  let bestRes = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line.startsWith('#EXT-X-STREAM-INF:')) continue;
    const bwMatch = line.match(/BANDWIDTH=(\d+)/);
    const resMatch = line.match(/RESOLUTION=(\d+)x(\d+)/);
    const bandwidth = bwMatch ? parseInt(bwMatch[1]) : 0;
    if (bandwidth > bestBandwidth) {
      bestBandwidth = bandwidth;
      bestRes = resMatch ? { width: parseInt(resMatch[1]), height: parseInt(resMatch[2]) } : null;
    }
  }

  return bestBandwidth > 0 ? { bandwidth: bestBandwidth, ...bestRes } : null;
}

async function resolveYouTube(youtubeKey, db = null) {
  if (!youtubeKey) return null;
  try {
    const yt = await getInnertube(db);

    let bestMuxed = null;

    for (const client of YOUTUBE_CLIENTS) {
      try {
        const info = await yt.getBasicInfo(youtubeKey, { client });
        if (info.playability_status?.status === 'LOGIN_REQUIRED') {
          _poToken = null; _poTokenExpiry = 0; _visitorData = null; _innertube = null;
          continue;
        }
        if (info.playability_status?.status !== 'OK' || !info.streaming_data) continue;
        const sd = info.streaming_data;

        const muxedRaw = sd.formats || [];
        for (const f of muxedRaw) {
          try {
            const url = await getFormatUrl(f, yt.session.player);
            if (url && (!bestMuxed || (f.height || 0) > bestMuxed.height)) {
              bestMuxed = { url, height: f.height || 0, width: f.width || 0, bitrate: f.bitrate || 0, quality_label: f.quality_label };
            }
          } catch { /* skip */ }
        }

        if (bestMuxed) break;

        // No muxed — try HLS manifest (AVFoundation native, adaptive bitrate, includes audio)
        if (!bestMuxed && sd.hls_manifest_url) {
          const adaptiveVideo = (sd.adaptive_formats || [])
            .filter(f => f.mime_type?.startsWith('video/'))
            .sort((a, b) => (b.height || 0) - (a.height || 0));
          const best = adaptiveVideo[0];
          return {
            url: sd.hls_manifest_url,
            provider: `YouTube ${best?.quality_label || 'HLS'}`,
            bitrate: best?.bitrate ? Math.round(best.bitrate / 1000) : 0,
            width: best?.width || 0,
            height: best?.height || 0,
          };
        }
      } catch { /* try next client */ }
    }

    if (bestMuxed) {
      return {
        url: bestMuxed.url,
        provider: `YouTube ${bestMuxed.quality_label || bestMuxed.height + 'p'}`,
        bitrate: Math.round((bestMuxed.bitrate || 0) / 1000),
        width: bestMuxed.width,
        height: bestMuxed.height,
      };
    }

    return null;
  } catch (e) {
    return null;
  }
}

async function resolveYouTubeDebug(videoId) {
  const stages = { videoId, timestamp: new Date().toISOString() };
  let yt;

  try {
    if (!_quickjs) _quickjs = await getQuickJSWASMModule();
    stages.quickjs = 'ok';
  } catch (e) {
    stages.quickjs = `FAILED: ${e.message}`;
    return stages;
  }

  try {
    Platform.shim.fetch = (input, init) => fetch(input, init);
    Platform.shim.eval = async (data, env) => {
      const vm = _quickjs.newContext();
      try {
        let code = '';
        for (const [key, value] of Object.entries(env || {})) {
          code += `var ${key} = ${JSON.stringify(value)};\n`;
        }
        code += data.output;
        const result = vm.evalCode(code);
        if (result.error) {
          const err = vm.dump(result.error);
          result.error.dispose();
          throw new Error(`QuickJS: ${JSON.stringify(err)}`);
        }
        const value = vm.dump(result.value);
        result.value.dispose();
        return value;
      } finally { vm.dispose(); }
    };
    let debugPot, debugVd;
    try {
      const potResult = await getPoToken();
      debugPot = potResult.poToken;
      debugVd = potResult.visitorData;
    } catch (pe) {
      stages.poTokenCrash = pe.message;
    }
    const debugOpts = { retrieve_player: true, generate_session_locally: true, enable_safety_mode: false };
    if (debugPot) debugOpts.po_token = debugPot;
    if (debugVd) debugOpts.visitor_data = debugVd;
    yt = await Innertube.create(debugOpts);
    stages.innertube = 'ok';
    stages.player = yt.session?.player ? 'loaded' : 'missing';
    stages.poToken = {
      type: _poTokenType,
      expires: _poTokenExpiry ? new Date(_poTokenExpiry).toISOString() : null,
      error: _poTokenError,
    };
  } catch (e) {
    stages.innertube = `FAILED: ${e.message}`;
    return stages;
  }

  let info = null;
  let usedClient = null;
  stages.clients = {};
  for (const client of YOUTUBE_CLIENTS) {
    try {
      const result = await yt.getBasicInfo(videoId, { client });
      const adaptiveVideo = (result.streaming_data?.adaptive_formats || [])
        .filter(f => f.mime_type?.startsWith('video/'));
      const bestAdaptive = adaptiveVideo.sort((a, b) => (b.height || 0) - (a.height || 0))[0];
      stages.clients[client] = {
        playability: result.playability_status?.status || 'unknown',
        reason: result.playability_status?.reason || null,
        hlsManifestUrl: result.streaming_data?.hls_manifest_url ? 'yes' : 'no',
        muxedFormats: result.streaming_data?.formats?.length || 0,
        adaptiveFormats: adaptiveVideo.length,
        bestAdaptive: bestAdaptive ? `${bestAdaptive.quality_label} ${bestAdaptive.mime_type?.split(';')[0]}` : 'none'
      };
      if (!info && result.playability_status?.status === 'OK' && result.streaming_data) {
        info = result;
        usedClient = client;
      }
    } catch (e) {
      stages.clients[client] = { error: e.message };
    }
  }

  if (!info) {
    stages.bestClient = 'none';
    return stages;
  }
  stages.bestClient = usedClient;

  stages.formatPriority = 'muxed MP4 (direct URL) → HLS manifest (AVFoundation native)';

  const muxedFormats = info.streaming_data?.formats || [];
  if (muxedFormats.length > 0) {
    const muxedResults = [];
    for (const f of muxedFormats) {
      try {
        const url = await getFormatUrl(f, yt.session.player);
        muxedResults.push({
          itag: f.itag,
          quality: f.quality_label || `${f.height}p`,
          hasUrl: !!url,
          urlPrefix: url ? url.substring(0, 60) + '...' : null,
        });
      } catch (e) {
        muxedResults.push({ itag: f.itag, error: e.message });
      }
    }
    stages.muxed = muxedResults;
  } else {
    stages.muxed = 'none available';
  }

  if (info.streaming_data?.hls_manifest_url) {
    stages.hlsAvailable = true;
    stages.hlsNote = 'HLS available — AVFoundation native playback (used as fallback when no muxed MP4)';
  }

  if (muxedFormats.length > 0) {
    stages.resolverWouldReturn = `muxed MP4 (${muxedFormats[0]?.quality_label || '720p'}, video+audio, direct URL)`;
  } else {
    stages.resolverWouldReturn = 'null — no playable format (no muxed MP4, no HLS manifest)';
  }

  return stages;
}

// ============== REQUEST HANDLER ==============

export default {
  async fetch(request, env) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json'
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const pathname = url.pathname;

    try {
      // POST /resolve — called by main worker via service binding
      if (pathname === '/resolve' && request.method === 'POST') {
        const { key } = await request.json();
        const result = await resolveYouTube(key, env.DB);
        return new Response(JSON.stringify(result), { headers: corsHeaders });
      }

      // GET /debug/<videoId>
      const debugMatch = pathname.match(/^\/debug\/(.+)$/);
      if (debugMatch) {
        const result = await resolveYouTubeDebug(debugMatch[1]);
        return new Response(JSON.stringify(result, null, 2), { headers: corsHeaders });
      }

      // GET /health
      if (pathname === '/health') {
        return new Response(JSON.stringify({ status: 'ok', worker: 'youtube', edge: request.cf?.colo }), { headers: corsHeaders });
      }

      return new Response(JSON.stringify({ error: 'Not found' }), { status: 404, headers: corsHeaders });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message || 'Internal error' }), { status: 500, headers: corsHeaders });
    }
  }
};
