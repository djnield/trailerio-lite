// Trailerio Lite - Cloudflare Workers Edition
// Zero storage, edge-deployed trailer resolver for Fusion

import { Innertube, Platform } from 'youtubei.js/web';
import { getQuickJSWASMModule } from '@cf-wasm/quickjs/workerd';

// Language configuration - add a new language by adding one line
const LANG_CONFIG = {
  en: { appleCountry: 'us', mubiCountry: 'US', label: 'English' },
  fr: { appleCountry: 'fr', mubiCountry: 'FR', label: 'Français', localSources: ['allocine'], dubbedRe: /\bVF\b/i, originalRe: /\bVO\b|VOSTFR/i },
  de: { appleCountry: 'de', mubiCountry: 'DE', label: 'Deutsch', localSources: ['filmstarts'], dubbedRe: /\bDF\b|deutsch/i, originalRe: /\bOV\b|\bOmU\b/i },
  it: { appleCountry: 'it', mubiCountry: 'IT', label: 'Italiano' },
  es: { appleCountry: 'es', mubiCountry: 'ES', label: 'Español', localSources: ['sensacine'] },
  pt: { appleCountry: 'br', mubiCountry: 'BR', label: 'Português', localSources: ['adorocinema'], dubbedRe: /dublad/i, originalRe: /original|legendad/i },
  ru: { appleCountry: 'ru', mubiCountry: 'RU', label: 'Русский' },
  ja: { appleCountry: 'jp', mubiCountry: 'JP', label: '日本語' },
  ko: { appleCountry: 'kr', mubiCountry: 'KR', label: '한국어' },
  cs: { appleCountry: 'cz', mubiCountry: 'CZ', label: 'Čeština' },
  hi: { appleCountry: 'in', mubiCountry: 'IN', label: 'हिन्दी' },
  tr: { appleCountry: 'tr', mubiCountry: 'TR', label: 'Türkçe' },
  ar: { appleCountry: 'ae', mubiCountry: 'AE', label: 'العربية' },
};

const LANG_CODES = Object.keys(LANG_CONFIG).filter(k => k !== 'en');

function getManifest(lang) {
  const config = LANG_CONFIG[lang] || LANG_CONFIG.en;
  return {
    id: lang === 'en' ? 'io.trailerio.lite' : `io.trailerio.lite.${lang}`,
    version: '1.3.0',
    name: lang === 'en' ? 'Trailerio' : `Trailerio ${config.label}`,
    description: lang === 'en'
      ? 'Trailer addon - Fandango, Apple TV, Rotten Tomatoes, Plex, MUBI, IMDb'
      : `Trailer addon - ${config.label} dubbed trailers`,
    logo: 'https://raw.githubusercontent.com/9mousaa/trailerio-lite/main/icon.png',
    resources: [{ name: 'meta', types: ['movie', 'series'], idPrefixes: ['tt'] }],
    types: ['movie', 'series'],
    idPrefixes: ['tt'],
    catalogs: []
  };
}

const CACHE_TTL = 86400; // 24 hours
const TMDB_API_KEY = 'bfe73358661a995b992ae9a812aa0d2f';

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

// ============== SMIL PARSER ==============

// Parse SMIL XML and return best quality video (highest bitrate)
function parseSMIL(smilXml) {
  const videoTags = [...smilXml.matchAll(/<video[^>]+src="(https:\/\/video\.fandango\.com[^"]+\.mp4)"[^>]*/g)];
  const videos = videoTags.map(m => {
    const tag = m[0];
    const widthMatch = tag.match(/width="(\d+)"/);
    const heightMatch = tag.match(/height="(\d+)"/);
    const bitrateMatch = tag.match(/system-bitrate="(\d+)"/);
    const height = heightMatch ? parseInt(heightMatch[1]) : 0;
    const width = widthMatch ? parseInt(widthMatch[1]) : Math.round(height * 16 / 9);
    return { url: m[1], width, height, bitrate: bitrateMatch ? Math.round(parseInt(bitrateMatch[1]) / 1000) : 0 };
  });
  if (videos.length === 0) return null;
  videos.sort((a, b) => b.bitrate - a.bitrate || b.width - a.width);
  return videos[0];
}

// ============== TMDB METADATA ==============

async function getTMDBMetadata(imdbId, type = 'movie') {
  try {
    const findRes = await fetchWithTimeout(
      `https://api.themoviedb.org/3/find/${imdbId}?api_key=${TMDB_API_KEY}&external_source=imdb_id`
    );
    const findData = await findRes.json();

    // Check requested type first, then fallback to other type
    let results = type === 'series'
      ? findData.tv_results
      : findData.movie_results;
    let actualType = type;

    // Fallback: if not found in requested type, check the other
    if (!results || results.length === 0) {
      results = type === 'series'
        ? findData.movie_results
        : findData.tv_results;
      actualType = type === 'series' ? 'movie' : 'series';
    }

    if (!results || results.length === 0) return null;

    const tmdbId = results[0].id;
    const title = results[0].title || results[0].name;

    // Get external IDs and YouTube trailer keys in parallel
    const endpoint = actualType === 'series' ? 'tv' : 'movie';
    const [extData, videosData] = await Promise.all([
      fetchWithTimeout(`https://api.themoviedb.org/3/${endpoint}/${tmdbId}/external_ids?api_key=${TMDB_API_KEY}`)
        .then(r => r.json()).catch(() => ({})),
      fetchWithTimeout(`https://api.themoviedb.org/3/${endpoint}/${tmdbId}/videos?api_key=${TMDB_API_KEY}&language=en-US`)
        .then(r => r.json()).catch(() => ({ results: [] }))
    ]);

    // Pick YouTube trailer keys, preferring official trailers
    const ytVideos = (videosData.results || [])
      .filter(v => v.site === 'YouTube' && v.key)
      .sort((a, b) => {
        const score = v => (v.type === 'Trailer' ? 0 : v.type === 'Teaser' ? 1 : 2);
        return score(a) - score(b);
      });
    const youtubeKeys = ytVideos.slice(0, 3).map(v => v.key);

    return {
      tmdbId,
      title,
      wikidataId: extData.wikidata_id,
      imdbId,
      actualType,
      youtubeKeys
    };
  } catch (e) {
    return null;
  }
}

// Get Apple TV / RT / Fandango / MUBI IDs from Wikidata entity
async function getWikidataIds(wikidataId) {
  if (!wikidataId) return {};

  try {
    const res = await fetchWithTimeout(
      `https://www.wikidata.org/wiki/Special:EntityData/${wikidataId}.json`,
      { headers: { 'Accept': 'application/json', 'User-Agent': 'TrailerioLite/1.0' } },
      6000
    );
    const data = await res.json();
    const entity = data.entities?.[wikidataId];
    if (!entity) return {};

    // P9586 = Apple TV movie ID, P9751 = Apple TV show ID
    const appleTvMovieId = entity.claims?.P9586?.[0]?.mainsnak?.datavalue?.value;
    const appleTvShowId = entity.claims?.P9751?.[0]?.mainsnak?.datavalue?.value;

    return {
      appleTvId: appleTvMovieId || appleTvShowId,
      isAppleTvShow: !!appleTvShowId && !appleTvMovieId,
      rtSlug: entity.claims?.P1258?.[0]?.mainsnak?.datavalue?.value,
      fandangoId: entity.claims?.P5693?.[0]?.mainsnak?.datavalue?.value,
      mubiId: entity.claims?.P7299?.[0]?.mainsnak?.datavalue?.value,
      // Webedia network (AlloCiné ID also works on SensaCine + Beyazperde)
      allocineId: entity.claims?.P1265?.[0]?.mainsnak?.datavalue?.value,
      filmstartsId: entity.claims?.P8531?.[0]?.mainsnak?.datavalue?.value,
      adoroCinemaId: entity.claims?.P7777?.[0]?.mainsnak?.datavalue?.value
    };
  } catch (e) {
    return {};
  }
}

// ============== SOURCE RESOLVERS ==============

// 1. Apple TV - 4K HLS trailers (localized by country code)
async function resolveAppleTV(imdbId, meta, lang = 'en') {
  try {
    let appleId = meta?.wikidataIds?.appleTvId;
    if (!appleId) return null;

    const country = LANG_CONFIG[lang]?.appleCountry || 'us';
    const isShow = meta?.wikidataIds?.isAppleTvShow;
    const pageUrl = isShow
      ? `https://tv.apple.com/${country}/show/${appleId}`
      : `https://tv.apple.com/${country}/movie/${appleId}`;

    const pageRes = await fetchWithTimeout(
      pageUrl,
      {
        headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' },
        redirect: 'follow'
      }
    );
    const html = await pageRes.text();

    // Extract all m3u8 URLs, sorted by context preference
    const hlsRaw = [...html.matchAll(/https:\/\/play[^"]*\.m3u8[^"]*/g)];
    const junk = /teaser|clip|behind|featurette|sneak|opening/i;
    const candidates = hlsRaw.map(m => ({
      url: m[0].replace(/&amp;/g, '&'),
      ctx: html.substring(Math.max(0, m.index - 500), m.index).toLowerCase()
    }));
    // Sort: full trailer context first, then any trailer context, then rest
    candidates.sort((a, b) => {
      const score = v => {
        if (v.ctx.includes('trailer') && !junk.test(v.ctx)) return 0;
        if (v.ctx.includes('trailer')) return 1;
        return 2;
      };
      return score(a) - score(b);
    });

    // Try each candidate, use feature.duration from master m3u8 to filter
    // Skip teasers (<60s) and full episodes (>300s)
    for (const candidate of candidates.slice(0, 3)) {
      try {
        const m3u8Res = await fetchWithTimeout(candidate.url, {}, 4000);
        const m3u8Text = await m3u8Res.text();

        // Check duration from master playlist metadata (no extra fetch needed)
        if (candidates.length > 1) {
          const durMatch = m3u8Text.match(/com\.apple\.hls\.feature\.duration.*?VALUE="([\d.]+)"/);
          if (durMatch) {
            const dur = parseFloat(durMatch[1]);
            if (dur < 60 || dur > 300) continue; // Skip teasers and full episodes
          }
        }

        const streamMatches = [...m3u8Text.matchAll(/#EXT-X-STREAM-INF:.*?BANDWIDTH=(\d+)(?:.*?RESOLUTION=(\d+)x(\d+))?/g)];
        if (streamMatches.length === 0) continue;

        streamMatches.sort((a, b) => parseInt(b[1]) - parseInt(a[1]));
        const maxBandwidth = parseInt(streamMatches[0][1]);
        const width = streamMatches[0][2] ? parseInt(streamMatches[0][2]) : 0;
        const height = streamMatches[0][3] ? parseInt(streamMatches[0][3]) : 0;
        const bitrate = Math.round(maxBandwidth / 1000);

        // Detect DV/HDR from VIDEO-RANGE and CODECS across all streams
        const hasDV = /dvh1/i.test(m3u8Text) || /VIDEO-RANGE=PQ/i.test(m3u8Text);
        const hasHDR = hasDV || /VIDEO-RANGE=HLG/i.test(m3u8Text) || /hev1\.\d+\.\d+\.L\d+/i.test(m3u8Text);
        // Detect Atmos/Surround from audio groups
        const hasAtmos = /atmos|ec-3/i.test(m3u8Text);
        const hasSurround = hasAtmos || /CHANNELS="6"|CHANNELS="8"|ac-3/i.test(m3u8Text);

        let quality = width >= 3840 ? '4K' : width >= 1900 ? '1080p' : width >= 1200 ? '720p' : '1080p';
        if (hasDV) quality += ' DV';
        else if (hasHDR) quality += ' HDR';
        if (hasAtmos) quality += ' Atmos';
        else if (hasSurround) quality += ' 5.1';

        return { url: candidate.url, provider: `Apple TV ${quality}`, bitrate, width, height, localized: lang !== 'en' };
      } catch (e) { continue; }
    }
    // Last resort: return first URL without quality info
    if (candidates.length > 0) {
      return { url: candidates[0].url, provider: 'Apple TV', bitrate: 0, width: 0, height: 0, localized: lang !== 'en' };
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 2. Plex - IVA CDN 1080p
async function resolvePlex(imdbId, meta) {
  try {
    const tokenRes = await fetchWithTimeout('https://plex.tv/api/v2/users/anonymous', {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'X-Plex-Client-Identifier': 'trailerio-lite',
        'X-Plex-Product': 'Plex Web',
        'X-Plex-Version': '4.141.1'
      }
    });
    const { authToken } = await tokenRes.json();
    if (!authToken) return null;

    // type=1 for movies, type=2 for TV shows
    const plexType = meta?.actualType === 'series' ? 2 : 1;

    const matchRes = await fetchWithTimeout(
      `https://metadata.provider.plex.tv/library/metadata/matches?type=${plexType}&guid=imdb://${imdbId}`,
      { headers: { 'Accept': 'application/json', 'X-Plex-Token': authToken } }
    );
    const matchData = await matchRes.json();
    const plexId = matchData.MediaContainer?.Metadata?.[0]?.ratingKey;
    if (!plexId) return null;

    const extrasRes = await fetchWithTimeout(
      `https://metadata.provider.plex.tv/library/metadata/${plexId}/extras`,
      { headers: { 'Accept': 'application/json', 'X-Plex-Token': authToken } }
    );
    const extrasData = await extrasRes.json();
    const extras = extrasData.MediaContainer?.Metadata || [];
    // Prefer full trailers, fall back to teasers/clips/BTS if no trailer exists
    const trailer = extras.find(m => m.subtype === 'trailer' && !/teaser|clip|behind|featurette/i.test(m.title))
      || extras.find(m => m.subtype === 'trailer')
      || extras[0];
    const url = trailer?.Media?.[0]?.url;

    if (url) {
      const kbrateMatch = url.match(/videokbrate=(\d+)/);
      const bitrate = kbrateMatch ? parseInt(kbrateMatch[1]) : 5000;
      return { url, provider: 'Plex 1080p', bitrate, width: 1920, height: 1080 };
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 3. Rotten Tomatoes - Fandango CDN (via SMIL resolution)
async function resolveRottenTomatoes(imdbId, meta) {
  try {
    let rtSlug = meta?.wikidataIds?.rtSlug;
    if (!rtSlug) return null;

    // Handle both "m/slug" and "slug" formats
    const isTV = rtSlug.startsWith('tv/');
    rtSlug = rtSlug.replace(/^(m|tv)\//, '');

    // Go directly to videos page (handle TV vs movie)
    const videosUrl = isTV
      ? `https://www.rottentomatoes.com/tv/${rtSlug}/videos`
      : `https://www.rottentomatoes.com/m/${rtSlug}/videos`;
    const pageRes = await fetchWithTimeout(videosUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    });
    if (!pageRes.ok) return null;

    const html = await pageRes.text();

    // Extract JSON from <script id="videos"> tag
    const scriptMatch = html.match(/<script\s+id="videos"[^>]*>([\s\S]*?)<\/script>/i);
    if (!scriptMatch) return null;

    let videos;
    try {
      videos = JSON.parse(scriptMatch[1]);
    } catch (e) {
      return null;
    }

    if (!Array.isArray(videos) || videos.length === 0) return null;

    // Sort: full trailers first, then teasers, then clips/BTS as fallback
    const junk = /teaser|clip|behind|featurette|sneak peek|opening|sequence/i;
    const priority = v => {
      const t = (v.title || '').toLowerCase();
      if (v.videoType === 'TRAILER' && t.includes('trailer') && !junk.test(t)) return 0;
      if (v.videoType === 'TRAILER' && !junk.test(t)) return 1;
      if (v.videoType === 'TRAILER') return 2;
      return 3;
    };
    videos.sort((a, b) => priority(a) - priority(b));

    // Try to resolve via SMIL to get direct fandango.com URL
    for (const trailer of videos) {
      if (!trailer.file) continue;

      let videoUrl = trailer.file;

      // Resolve theplatform URLs via SMIL
      if (videoUrl.includes('theplatform.com') || videoUrl.includes('link.theplatform')) {
        try {
          const smilUrl = videoUrl.split('?')[0] + '?format=SMIL';
          const smilRes = await fetchWithTimeout(smilUrl, {
            headers: { 'Accept': 'application/smil+xml' }
          }, 4000);

          if (smilRes.ok) {
            const smilXml = await smilRes.text();
            const best = parseSMIL(smilXml);
            if (best) {
              const quality = best.width >= 1900 ? '1080p' : `${best.height}p`;
              return { url: best.url, provider: `Rotten Tomatoes ${quality}`, bitrate: best.bitrate || 5000, width: best.width, height: best.height };
            }
          }
        } catch (e) { /* try next trailer */ }
      }
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 4. Fandango - Direct theplatform.com (up to 1080p @ 8Mbps)
async function resolveFandango(imdbId, meta) {
  try {
    const fandangoId = meta?.wikidataIds?.fandangoId;
    if (!fandangoId) return null;

    // Fetch movie overview page (shorthand URL redirects to canonical)
    const pageRes = await fetchWithTimeout(
      `https://www.fandango.com/x-${fandangoId}/movie-overview`,
      {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
        redirect: 'follow'
      }
    );
    if (!pageRes.ok) return null;

    const html = await pageRes.text();

    // Extract jwPlayerData JSON from page
    const jwMatch = html.match(/jwPlayerData\s*=\s*(\{[\s\S]*?\});/);
    if (!jwMatch) return null;

    let jwData;
    try {
      jwData = JSON.parse(jwMatch[1]);
    } catch (e) {
      return null;
    }

    const contentURL = jwData.contentURL;
    if (!contentURL || !contentURL.includes('theplatform.com')) return null;

    // Resolve via SMIL for best quality MP4
    const smilUrl = contentURL.split('?')[0] + '?format=SMIL&formats=mpeg4';
    const smilRes = await fetchWithTimeout(smilUrl, {
      headers: { 'Accept': 'application/smil+xml' }
    }, 4000);

    if (!smilRes.ok) return null;

    const smilXml = await smilRes.text();
    const best = parseSMIL(smilXml);
    if (best) {
      const quality = best.width >= 1900 ? '1080p' : `${best.height}p`;
      return { url: best.url, provider: `Fandango ${quality}`, bitrate: best.bitrate || 8000, width: best.width, height: best.height };
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 5. MUBI - Direct API with MP4 trailers (localized by country)
async function resolveMUBI(imdbId, meta, lang = 'en') {
  try {
    const mubiId = meta?.wikidataIds?.mubiId;
    if (!mubiId) return null;

    const country = LANG_CONFIG[lang]?.mubiCountry || 'US';
    const res = await fetchWithTimeout(
      `https://api.mubi.com/v3/films/${mubiId}`,
      { headers: { 'CLIENT': 'web', 'CLIENT_COUNTRY': country } }
    );
    if (!res.ok) return null;

    const data = await res.json();

    // Pick highest quality from optimised_trailers
    const trailers = data.optimised_trailers;
    if (!trailers || trailers.length === 0) return null;

    // Sort by profile (1080p > 720p > 240p)
    const profileOrder = { '1080p': 1080, '720p': 720, '480p': 480, '360p': 360, '240p': 240 };
    trailers.sort((a, b) => (profileOrder[b.profile] || 0) - (profileOrder[a.profile] || 0));

    const best = trailers[0];
    const height = profileOrder[best.profile] || 0;
    const width = Math.round(height * 16 / 9);

    return { url: best.url, provider: `MUBI ${best.profile}`, bitrate: 0, width, height, localized: lang !== 'en' };
  } catch (e) { /* silent fail */ }
  return null;
}

// 6. IMDb - Fallback
async function resolveIMDb(imdbId) {
  try {
    const pageRes = await fetchWithTimeout(
      `https://www.imdb.com/title/${imdbId}/`,
      { headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept-Language': 'en-US,en'
      }}
    );
    const html = await pageRes.text();

    const videoMatch = html.match(/\/video\/(vi\d+)/);
    if (!videoMatch) return null;

    const videoRes = await fetchWithTimeout(
      `https://www.imdb.com/video/${videoMatch[1]}/`,
      { headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept-Language': 'en-US,en'
      }}
    );
    const videoHtml = await videoRes.text();

    const urlMatch = videoHtml.match(/"url":"(https:\/\/imdb-video\.media-imdb\.com[^"]+\.mp4[^"]*)"/);
    if (urlMatch) {
      return { url: urlMatch[1].replace(/\\u0026/g, '&'), provider: 'IMDb', bitrate: 0, width: 0, height: 0 };
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// ============== YOUTUBE RESOLVER ==============

// Uses youtubei.js library with QuickJS WASM as JS interpreter for cipher/nsig deciphering
// IOS/ANDROID clients are less blocked from datacenter IPs than WEB; TV_EMBEDDED always fails
const YOUTUBE_CLIENTS = ['IOS', 'ANDROID', 'WEB'];

// Get URL from format — use direct URL if available, decipher only when needed
async function getFormatUrl(f, player) {
  if (f.url) return f.url;
  return String(await f.decipher(player));
}

let _innertube = null;
let _innertubeRefresh = 0;
let _quickjs = null;

async function getInnertube() {
  const now = Date.now();
  // Refresh session every 15 minutes (player JS changes, sessions expire)
  if (_innertube && now - _innertubeRefresh < 15 * 60 * 1000) return _innertube;

  // Initialize QuickJS WASM once
  if (!_quickjs) _quickjs = await getQuickJSWASMModule();

  // Fix Cloudflare Workers "Illegal invocation" — wrap fetch to preserve `this` binding
  Platform.shim.fetch = (input, init) => fetch(input, init);

  // Set up custom eval shim — replaces blocked native eval()/new Function()
  // YouTube.js uses Platform.shim.eval(data, env) for sig/nsig deciphering
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
    } finally {
      vm.dispose();
    }
  };

  // Try fetching real session from YouTube (proper visitor_data, looks legitimate)
  // Fall back to local generation if the network call fails
  try {
    _innertube = await Innertube.create({
      retrieve_player: true,
      generate_session_locally: false,
      enable_safety_mode: false,
    });
  } catch {
    _innertube = await Innertube.create({
      retrieve_player: true,
      generate_session_locally: true,
      enable_safety_mode: false,
    });
  }
  _innertubeRefresh = now;
  return _innertube;
}

async function resolveYouTube(youtubeKey) {
  if (!youtubeKey) return null;
  try {
    const yt = await getInnertube();

    // Try multiple client types — datacenter IPs get blocked on WEB but not TV/mobile
    let info = null;
    for (const client of YOUTUBE_CLIENTS) {
      try {
        info = await yt.getBasicInfo(youtubeKey, { client });
        if (info.playability_status?.status === 'OK' && info.streaming_data) break;
      } catch { /* try next client */ }
    }

    if (!info || info.playability_status?.status !== 'OK') return null;

    const streamingData = info.streaming_data;
    if (!streamingData) return null;

    // Try ADAPTIVE first — 720p/1080p/4K video-only (much better quality)
    // Prefer H.264 for iOS/tvOS compatibility, then sort by resolution
    const adaptiveRaw = (streamingData.adaptive_formats || [])
      .filter(f => f.mime_type?.startsWith('video/'));
    const adaptive = [];
    for (const f of adaptiveRaw) {
      try {
        const url = await getFormatUrl(f, yt.session.player);
        if (url) adaptive.push({ ...f, url });
      } catch { /* skip */ }
    }
    adaptive.sort((a, b) => {
      const aH264 = a.mime_type?.includes('avc1') ? 1 : 0;
      const bH264 = b.mime_type?.includes('avc1') ? 1 : 0;
      if (aH264 !== bH264) return bH264 - aH264;
      return (b.height || 0) - (a.height || 0);
    });

    if (adaptive.length > 0) {
      const best = adaptive[0];
      const codec = best.mime_type?.includes('avc1') ? 'H.264' : best.mime_type?.includes('vp9') ? 'VP9' : '';
      return {
        url: best.url,
        provider: `YouTube ${best.quality_label || '720p'}${codec ? ' ' + codec : ''}`,
        bitrate: Math.round((best.bitrate || 0) / 1000),
        width: best.width || 0,
        height: best.height || 0
      };
    }

    // Fallback: muxed formats (video+audio combined, 360p max but has audio)
    const muxedRaw = streamingData.formats || [];
    const muxed = [];
    for (const f of muxedRaw) {
      try {
        const url = await getFormatUrl(f, yt.session.player);
        if (url) muxed.push({ ...f, url });
      } catch { /* skip failed decipher */ }
    }
    muxed.sort((a, b) => (b.height || 0) - (a.height || 0));

    if (muxed.length > 0) {
      const best = muxed[0];
      return {
        url: best.url,
        provider: `YouTube ${best.quality_label || '360p'}`,
        bitrate: Math.round((best.bitrate || 0) / 1000),
        width: best.width || 0,
        height: best.height || 0
      };
    }

    return null;
  } catch (e) {
    return null;
  }
}

// Debug: step-by-step YouTube resolution to identify failures
async function resolveYouTubeDebug(videoId) {
  const stages = { videoId, timestamp: new Date().toISOString() };
  let yt;

  // Stage 1: QuickJS WASM
  try {
    if (!_quickjs) _quickjs = await getQuickJSWASMModule();
    stages.quickjs = 'ok';
  } catch (e) {
    stages.quickjs = `FAILED: ${e.message}`;
    return stages;
  }

  // Stage 2: Innertube session
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
    try {
      yt = await Innertube.create({ retrieve_player: true, generate_session_locally: false, enable_safety_mode: false });
      stages.session = 'remote';
    } catch {
      yt = await Innertube.create({ retrieve_player: true, generate_session_locally: true, enable_safety_mode: false });
      stages.session = 'local-fallback';
    }
    stages.innertube = 'ok';
    stages.player = yt.session?.player ? 'loaded' : 'missing';
  } catch (e) {
    stages.innertube = `FAILED: ${e.message}`;
    return stages;
  }

  // Stage 3: getBasicInfo — try each client type, report details
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

  // Stage 4: test best adaptive format (what the resolver actually uses)
  const adaptiveVideo = (info.streaming_data?.adaptive_formats || [])
    .filter(f => f.mime_type?.startsWith('video/'))
    .sort((a, b) => {
      const aH264 = a.mime_type?.includes('avc1') ? 1 : 0;
      const bH264 = b.mime_type?.includes('avc1') ? 1 : 0;
      if (aH264 !== bH264) return bH264 - aH264;
      return (b.height || 0) - (a.height || 0);
    });

  if (adaptiveVideo.length > 0) {
    const best = adaptiveVideo[0];
    try {
      const url = await getFormatUrl(best, yt.session.player);
      const codec = best.mime_type?.includes('avc1') ? 'H.264' : best.mime_type?.includes('vp9') ? 'VP9' : '';
      stages.bestFormat = `${best.quality_label} ${codec} ${Math.round((best.bitrate || 0) / 1000)}kbps`;
      stages.decipherAdaptive = url ? `ok (${url.substring(0, 80)}...)` : 'null url';
    } catch (e) {
      stages.decipherAdaptive = `FAILED: ${e.message}`;
    }
  }

  // Also test muxed as fallback info
  if (info.streaming_data?.formats?.length > 0) {
    const f = info.streaming_data.formats[0];
    try {
      const url = await getFormatUrl(f, yt.session.player);
      stages.decipherMuxed = url ? `ok (${f.quality_label || '360p'})` : 'null url';
    } catch (e) {
      stages.decipherMuxed = `FAILED: ${e.message}`;
    }
  }

  return stages;
}

// ============== DAILYMOTION RESOLVER (shared utility) ==============

async function resolveDailymotion(dmVideoId, providerLabel) {
  try {
    const metaRes = await fetchWithTimeout(
      `https://www.dailymotion.com/player/metadata/video/${dmVideoId}`,
      { headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Cookie': 'family_filter=off; ff=off'
      }}
    );
    if (!metaRes.ok) return null;
    const data = await metaRes.json();

    const qualities = data.qualities;
    if (!qualities) return null;

    // Try resolution-specific MP4 first
    for (const res of ['1080', '720', '480', '380', '240']) {
      const streams = qualities[res];
      if (!streams) continue;
      const mp4 = streams.find(s => s.type === 'video/mp4');
      if (mp4?.url) {
        const height = parseInt(res);
        return {
          url: mp4.url,
          provider: `${providerLabel} ${res}p`,
          bitrate: 0,
          width: Math.round(height * 16 / 9),
          height
        };
      }
    }

    // Fallback: HLS stream (Dailymotion often only provides m3u8 now)
    const autoStreams = qualities['auto'];
    if (autoStreams) {
      const hls = autoStreams.find(s => s.type === 'application/x-mpegURL');
      if (hls?.url) {
        return { url: hls.url, provider: `${providerLabel}`, bitrate: 0, width: 0, height: 0 };
      }
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// ============== WEBEDIA NETWORK RESOLVERS ==============
// AlloCiné, SensaCine, AdoroCinema, Filmstarts, Beyazperde all use Dailymotion

const UA = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' };
const decodeEntities = s => s.replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&#039;/g, "'");

function pickBestVersion(pool, dubbedRe, originalRe) {
  const dubbed = pool.find(e => dubbedRe && dubbedRe.test(e.title));
  const nonOrig = pool.find(e => !originalRe || !originalRe.test(e.title));
  const best = dubbed || nonOrig || (originalRe ? null : pool[0]);
  return { best, dubbed: !!dubbed };
}

async function resolveWebedia(pageUrl, filmId, label, dubbedRe, originalRe) {
  try {
    const pageRes = await fetchWithTimeout(pageUrl, { headers: UA });
    if (!pageRes.ok) return null;
    let html = decodeEntities(await pageRes.text());

    // Strategy 1: Find DM IDs with titles directly on page (SensaCine, AdoroCinema)
    const entries = [...html.matchAll(/"idDailymotion"\s*:\s*"([a-zA-Z0-9]+)"[^}]*?"title"\s*:\s*"([^"]+)"/g)]
      .map(m => ({ id: m[1], title: m[2] }));
    const entriesRev = [...html.matchAll(/"title"\s*:\s*"([^"]+)"[^}]*?"idDailymotion"\s*:\s*"([a-zA-Z0-9]+)"/g)]
      .map(m => ({ id: m[2], title: m[1] }));
    const all = [...entries, ...entriesRev];

    // Filter to trailer-related entries only
    const trailers = all.filter(e => /trailer|bande|teaser|tráiler|fragman/i.test(e.title));
    const pool = trailers.length > 0 ? trailers : all;

    if (pool.length > 0) {
      const { best, dubbed } = pickBestVersion(pool, dubbedRe, originalRe);
      if (!best) return null;
      const result = await resolveDailymotion(best.id, dubbed ? `${label} dubbed` : label);
      if (result) return { ...result, localized: true };
    }

    // Strategy 2: Find cmedia IDs for this film, fetch player pages (AlloCiné, Filmstarts)
    if (filmId) {
      const cmediaIds = [...new Set(
        [...html.matchAll(new RegExp(`cmedia=(\\d+)[^"]*cfilm=${filmId}`, 'g'))].map(m => m[1])
      )];
      if (cmediaIds.length > 0) {
        const baseUrl = new URL(pageUrl).origin;
        const playerPages = await Promise.all(
          cmediaIds.slice(0, 3).map(async (cmedia) => {
            try {
              const res = await fetchWithTimeout(
                `${baseUrl}/video/player_gen_cmedia=${cmedia}&cfilm=${filmId}.html`,
                { headers: UA }, 4000
              );
              if (!res.ok) return null;
              let ph = decodeEntities(await res.text());
              const dm = ph.match(/idDailymotion[^a-zA-Z0-9]*([a-zA-Z0-9]{5,12})/);
              const title = ph.match(/<title>([^<]+)/);
              return dm ? { id: dm[1], title: title ? title[1] : '' } : null;
            } catch { return null; }
          })
        );
        const videos = playerPages.filter(Boolean);
        if (videos.length > 0) {
          const { best, dubbed } = pickBestVersion(videos, dubbedRe, originalRe);
          if (!best) return null;
          const result = await resolveDailymotion(best.id, dubbed ? `${label} dubbed` : label);
          if (result) return { ...result, localized: true };
        }
      }
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 7. AlloCiné (French)
function resolveAllocine(meta) {
  const id = meta?.wikidataIds?.allocineId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.allocine.fr/film/fichefilm_gen_cfilm=${id}.html`,
    id, 'AlloCiné', /\bVF\b/i, /\bVO\b|VOSTFR/i
  );
}

// 8. Filmstarts (German)
function resolveFilmstarts(meta) {
  const id = meta?.wikidataIds?.filmstartsId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.filmstarts.de/kritiken/${id}.html`,
    id, 'Filmstarts', /\bDF\b|deutsch/i, /\bOV\b|\bOmU\b|\bOmdU\b/i
  );
}

// 9. SensaCine (Spanish) - uses same AlloCiné ID (P1265)
function resolveSensaCine(meta) {
  const id = meta?.wikidataIds?.allocineId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.sensacine.com/peliculas/pelicula-${id}/`,
    id, 'SensaCine', /doblad|tráiler/i, /\bVO\b|VOSE|subtitulad/i
  );
}

// 10. AdoroCinema (Brazilian Portuguese) - P7777
function resolveAdoroCinema(meta) {
  const id = meta?.wikidataIds?.adoroCinemaId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.adorocinema.com/filmes/filme-${id}/`,
    id, 'AdoroCinema', /dublad/i, /original|legendad/i
  );
}

// ============== MAIN RESOLVER ==============

// Deferred promise: lets downstream consumers await a value that upstream will resolve later
function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}

async function resolveTrailers(imdbId, type, cache, lang = 'en') {
  const cacheKey = `trailer:v44:${lang}:${imdbId}`;
  const cached = await cache.match(new Request(`https://cache/${cacheKey}`));
  if (cached) {
    return await cached.json();
  }

  // Shared deferred signals - sources await these instead of blocking in phases
  const metaReady = deferred();     // resolves with { tmdbMeta, wikidataIds }
  const tmdbReady = deferred();     // resolves with tmdbMeta (for Plex actualType)

  // ---------- METADATA PIPELINE (runs as one task) ----------
  // TMDB find → external_ids → Wikidata: chained but non-blocking to sources
  const metaPipeline = (async () => {
    try {
      const tmdbMeta = await getTMDBMetadata(imdbId, type);
      tmdbReady.resolve(tmdbMeta);  // unblocks Plex immediately

      const wikidataIds = tmdbMeta?.wikidataId
        ? await getWikidataIds(tmdbMeta.wikidataId)
        : {};
      metaReady.resolve({ tmdbMeta, wikidataIds });  // unblocks all Phase 3 sources
      return { tmdbMeta, wikidataIds };
    } catch (e) {
      // Always resolve to unblock downstream sources (they'll gracefully get null/empty)
      tmdbReady.resolve(null);
      metaReady.resolve({ tmdbMeta: null, wikidataIds: {} });
      return { tmdbMeta: null, wikidataIds: {} };
    }
  })();

  // ---------- ALL SOURCES IN PARALLEL (no phases, no waterfall) ----------
  const sources = [
    // IMDb - needs nothing, starts immediately
    resolveIMDb(imdbId),

    // YouTube - needs youtubeKeys from TMDB, tries ANDROID client (direct URLs)
    (async () => {
      const tmdbMeta = await tmdbReady.promise;
      const keys = tmdbMeta?.youtubeKeys;
      if (!keys?.length) return null;
      // Try first key (best trailer from TMDB)
      return resolveYouTube(keys[0]);
    })(),

    // Plex - only needs actualType from TMDB, starts as soon as TMDB find completes
    (async () => {
      const tmdbMeta = await tmdbReady.promise;
      return resolvePlex(imdbId, tmdbMeta);
    })(),

    // Apple TV, MUBI, RT, Fandango - need wikidataIds, start as soon as Wikidata completes
    (async () => {
      const { tmdbMeta, wikidataIds } = await metaReady.promise;
      return resolveAppleTV(imdbId, { ...tmdbMeta, wikidataIds }, lang);
    })(),
    (async () => {
      const { tmdbMeta, wikidataIds } = await metaReady.promise;
      return resolveMUBI(imdbId, { ...tmdbMeta, wikidataIds }, lang);
    })(),
    (async () => {
      const { tmdbMeta, wikidataIds } = await metaReady.promise;
      return resolveRottenTomatoes(imdbId, { ...tmdbMeta, wikidataIds });
    })(),
    (async () => {
      const { tmdbMeta, wikidataIds } = await metaReady.promise;
      return resolveFandango(imdbId, { ...tmdbMeta, wikidataIds });
    })(),
  ];

  // Language-specific local sources (Webedia network) - also await metaReady
  const localSources = LANG_CONFIG[lang]?.localSources || [];
  if (localSources.includes('allocine')) sources.push((async () => {
    const { tmdbMeta, wikidataIds } = await metaReady.promise;
    return resolveAllocine({ ...tmdbMeta, wikidataIds });
  })());
  if (localSources.includes('filmstarts')) sources.push((async () => {
    const { tmdbMeta, wikidataIds } = await metaReady.promise;
    return resolveFilmstarts({ ...tmdbMeta, wikidataIds });
  })());
  if (localSources.includes('sensacine')) sources.push((async () => {
    const { tmdbMeta, wikidataIds } = await metaReady.promise;
    return resolveSensaCine({ ...tmdbMeta, wikidataIds });
  })());
  if (localSources.includes('adorocinema')) sources.push((async () => {
    const { tmdbMeta, wikidataIds } = await metaReady.promise;
    return resolveAdoroCinema({ ...tmdbMeta, wikidataIds });
  })());

  // Wait for everything - allSettled so one source crash doesn't kill others
  const settled = await Promise.allSettled([metaPipeline, ...sources]);
  const metaResult = settled[0].status === 'fulfilled' ? settled[0].value : { tmdbMeta: null, wikidataIds: {} };
  const allResults = settled.slice(1)
    .filter(r => r.status === 'fulfilled')
    .map(r => r.value);

  // Quality tier from largest dimension (aspect-ratio agnostic)
  const tier = (w, h) => { const m = Math.max(w, h); return m >= 3840 ? 3 : m >= 1900 ? 2 : m >= 1200 ? 1 : 0; };

  // Sort: localized sources first, then by quality tier, then bitrate
  const isLocalized = lang !== 'en';
  const seen = new Set();
  const links = allResults
    .filter(r => r !== null)
    .sort((a, b) => {
      if (isLocalized) {
        if (a.localized && !b.localized) return -1;
        if (!a.localized && b.localized) return 1;
      }
      return tier(b.width, b.height) - tier(a.width, a.height) || b.bitrate - a.bitrate;
    })
    .filter(r => {
      if (seen.has(r.url)) return false;
      seen.add(r.url);
      return true;
    })
    .map((r, index) => ({
      trailers: r.url,
      provider: index === 0 ? `⭐ ${r.provider}` : r.provider
    }));

  const result = {
    title: metaResult?.tmdbMeta?.title || imdbId,
    links: links
  };

  if (links.length > 0) {
    const response = new Response(JSON.stringify(result), {
      headers: { 'Cache-Control': `max-age=${CACHE_TTL}` }
    });
    await cache.put(new Request(`https://cache/${cacheKey}`), response.clone());
  }

  return result;
}

// ============== REQUEST HANDLER ==============

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const cache = caches.default;

    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json'
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Parse optional language prefix: /{lang}/...
    let lang = 'en';
    let pathname = url.pathname;
    const langRegex = new RegExp(`^\\/(${LANG_CODES.join('|')})\\/`);
    const langMatch = pathname.match(langRegex);
    if (langMatch) {
      lang = langMatch[1];
      pathname = pathname.slice(langMatch[0].length - 1); // strip prefix, keep leading /
    }

    // Manifest
    if (pathname === '/manifest.json') {
      return new Response(JSON.stringify(getManifest(lang)), { headers: corsHeaders });
    }

    // Health check
    if (pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', edge: request.cf?.colo, lang }), { headers: corsHeaders });
    }

    // YouTube debug endpoint
    const ytDebugMatch = pathname.match(/^\/debug\/youtube\/(.+)$/);
    if (ytDebugMatch) {
      const result = await resolveYouTubeDebug(ytDebugMatch[1]);
      return new Response(JSON.stringify(result, null, 2), { headers: corsHeaders });
    }

    // Meta endpoint: /meta/{type}/{id}.json
    const metaMatch = pathname.match(/^\/meta\/(movie|series)\/(.+)\.json$/);
    if (metaMatch) {
      const [, type, id] = metaMatch;
      const imdbId = id.split(':')[0];

      const result = await resolveTrailers(imdbId, type, cache, lang);

      return new Response(JSON.stringify({
        meta: {
          id: imdbId,
          type: type,
          name: result.title,
          links: result.links
        }
      }), { headers: corsHeaders });
    }

    // 404
    return new Response(JSON.stringify({ error: 'Not found' }), {
      status: 404,
      headers: corsHeaders
    });
  }
};
