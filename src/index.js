// Trailerio Lite - Cloudflare Workers Edition
// Zero storage, edge-deployed trailer resolver for Fusion

// Language configuration - add a new language by adding one line
const LANG_CONFIG = {
  en: { appleCountry: 'us', mubiCountry: 'US', label: 'English', dmChannel: 'x1xbudh' },
  fr: { appleCountry: 'fr', mubiCountry: 'FR', label: 'Français', localSources: ['allocine'], dmChannel: 'allocine', dubbedRe: /\bVF\b/i, originalRe: /\bVO\b|VOSTFR/i },
  de: { appleCountry: 'de', mubiCountry: 'DE', label: 'Deutsch', localSources: ['filmstarts'], dmChannel: 'FILMSTARTS', dubbedRe: /\bDF\b|deutsch/i, originalRe: /\bOV\b|\bOmU\b/i },
  it: { appleCountry: 'it', mubiCountry: 'IT', label: 'Italiano', dmChannel: 'mymoviesit' },
  es: { appleCountry: 'es', mubiCountry: 'ES', label: 'Español', localSources: ['sensacine'], dmChannel: 'sensacine' },
  pt: { appleCountry: 'br', mubiCountry: 'BR', label: 'Português', localSources: ['adorocinema'], dmChannel: 'adorocinema', dubbedRe: /dublad/i, originalRe: /original|legendad/i },
  ru: { appleCountry: 'ru', mubiCountry: 'RU', label: 'Русский' },
  ja: { appleCountry: 'jp', mubiCountry: 'JP', label: '日本語' },
  ko: { appleCountry: 'kr', mubiCountry: 'KR', label: '한국어' },
  cs: { appleCountry: 'cz', mubiCountry: 'CZ', label: 'Čeština' },
  hi: { appleCountry: 'in', mubiCountry: 'IN', label: 'हिन्दी' },
  tr: { appleCountry: 'tr', mubiCountry: 'TR', label: 'Türkçe', dmChannel: 'beyazperde' },
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

async function fetchWithTimeout(url, options = {}, timeout = 8000) {
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

    // Get external IDs including Wikidata
    const extRes = await fetchWithTimeout(
      `https://api.themoviedb.org/3/${actualType === 'series' ? 'tv' : 'movie'}/${tmdbId}/external_ids?api_key=${TMDB_API_KEY}`
    );
    const extData = await extRes.json();

    return {
      tmdbId,
      title,
      wikidataId: extData.wikidata_id,
      imdbId,
      actualType
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
      10000
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
    for (const candidate of candidates) {
      try {
        const m3u8Res = await fetchWithTimeout(candidate.url, {}, 5000);
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
        const quality = width >= 3840 ? '4K' : width >= 1900 ? '1080p' : width >= 1200 ? '720p' : '1080p';
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
          }, 5000);

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
    }, 5000);

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

async function resolveWebedia(pageUrl, filmId, label, dubbedRe, originalRe) {
  try {
    const pageRes = await fetchWithTimeout(pageUrl, { headers: UA });
    if (!pageRes.ok) return null;
    let html = await pageRes.text();
    html = html.replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&#039;/g, "'");

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
      const dubbed = pool.find(e => dubbedRe && dubbedRe.test(e.title));
      const nonOrig = pool.find(e => !originalRe || !originalRe.test(e.title));
      const best = dubbed || nonOrig || pool[0];
      const tag = dubbed ? `${label} dubbed` : label;
      const result = await resolveDailymotion(best.id, tag);
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
                { headers: UA }, 5000
              );
              if (!res.ok) return null;
              let ph = await res.text();
              ph = ph.replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&#039;/g, "'");
              const dm = ph.match(/idDailymotion[^a-zA-Z0-9]*([a-zA-Z0-9]{5,12})/);
              const title = ph.match(/<title>([^<]+)/);
              return dm ? { id: dm[1], title: title ? title[1] : '' } : null;
            } catch { return null; }
          })
        );
        const videos = playerPages.filter(Boolean);
        if (videos.length > 0) {
          const dubbed = videos.find(v => dubbedRe && dubbedRe.test(v.title));
          const nonOrig = videos.find(v => !originalRe || !originalRe.test(v.title));
          const best = dubbed || nonOrig || videos[0];
          const tag = dubbed ? `${label} dubbed` : label;
          const result = await resolveDailymotion(best.id, tag);
          if (result) return { ...result, localized: true };
        }
      }
    }
  } catch (e) { /* silent fail */ }
  return null;
}

// 7. AlloCiné (French)
function resolveAllocine(imdbId, meta) {
  const id = meta?.wikidataIds?.allocineId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.allocine.fr/film/fichefilm_gen_cfilm=${id}.html`,
    id, 'AlloCiné', /\bVF\b/i, /\bVO\b|VOSTFR/i
  );
}

// 8. Filmstarts (German)
function resolveFilmstarts(imdbId, meta) {
  const id = meta?.wikidataIds?.filmstartsId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.filmstarts.de/kritiken/${id}.html`,
    id, 'Filmstarts', /\bDF\b|deutsch/i, /\bOV\b|\bOmU\b|\bOmdU\b/i
  );
}

// 9. SensaCine (Spanish) - uses same AlloCiné ID (P1265)
function resolveSensaCine(imdbId, meta) {
  const id = meta?.wikidataIds?.allocineId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.sensacine.com/peliculas/pelicula-${id}/`,
    id, 'SensaCine', /doblad|tráiler/i, /\bVO\b|VOSE|subtitulad/i
  );
}

// 10. AdoroCinema (Brazilian Portuguese) - P7777
function resolveAdoroCinema(imdbId, meta) {
  const id = meta?.wikidataIds?.adoroCinemaId;
  if (!id) return Promise.resolve(null);
  return resolveWebedia(
    `https://www.adorocinema.com/filmes/filme-${id}/`,
    id, 'AdoroCinema', /dublad/i, /original|legendad/i
  );
}

// ============== DAILYMOTION CHANNEL SEARCH (title-based fallback) ==============

async function resolveDMChannel(channel, searchTitle, label, dubbedRe, originalRe) {
  try {
    const searchUrl = `https://api.dailymotion.com/user/${channel}/videos?search=${encodeURIComponent(searchTitle)}&fields=id,title,language&limit=10&sort=relevance`;
    const res = await fetchWithTimeout(searchUrl, {}, 5000);
    if (!res.ok) return null;
    const data = await res.json();
    const videos = data.list || [];
    if (videos.length === 0) return null;

    // Strict title matching: normalize and verify the movie title appears in video title
    const normalize = s => s.toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
    const movieTitle = normalize(searchTitle);
    const matched = videos.filter(v => normalize(v.title).includes(movieTitle));
    if (matched.length === 0) return null;

    // Filter to trailer-like entries
    const trailerRe = /trailer|bande|teaser|tráiler|fragman/i;
    const junkRe = /clip|behind|featurette|interview|review|crítica/i;
    const trailers = matched.filter(v => trailerRe.test(v.title) && !junkRe.test(v.title));
    const pool = trailers.length > 0 ? trailers : matched;

    // Prefer dubbed version
    const dubbed = dubbedRe ? pool.find(v => dubbedRe.test(v.title)) : null;
    const nonOrig = originalRe ? pool.find(v => !originalRe.test(v.title)) : pool[0];
    const best = dubbed || nonOrig || pool[0];

    const tag = dubbed ? `${label} dubbed` : label;
    const result = await resolveDailymotion(best.id, tag);
    if (result) return { ...result, localized: true };
  } catch (e) { /* silent fail */ }
  return null;
}

// ============== MAIN RESOLVER ==============

async function resolveTrailers(imdbId, type, cache, lang = 'en') {
  const cacheKey = `trailer:v33:${lang}:${imdbId}`;
  const cached = await cache.match(new Request(`https://cache/${cacheKey}`));
  if (cached) {
    return await cached.json();
  }

  // PHASE 1: TMDB + IMDb in parallel (always both)
  const [tmdbMeta, imdbResult] = await Promise.all([
    getTMDBMetadata(imdbId, type),
    resolveIMDb(imdbId)
  ]);

  // PHASE 2: Wikidata + Plex in parallel (always both)
  const [wikidataIds, plexResult] = await Promise.all([
    tmdbMeta?.wikidataId ? getWikidataIds(tmdbMeta.wikidataId) : Promise.resolve({}),
    resolvePlex(imdbId, tmdbMeta)
  ]);

  const meta = { ...tmdbMeta, wikidataIds };

  // PHASE 3: ALL sources in parallel - localized + English
  const phase3 = [
    resolveAppleTV(imdbId, meta, lang),
    resolveMUBI(imdbId, meta, lang),
    resolveRottenTomatoes(imdbId, meta),
    resolveFandango(imdbId, meta),
  ];

  // Add language-specific local sources (Webedia network)
  const localSources = LANG_CONFIG[lang]?.localSources || [];
  if (localSources.includes('allocine')) phase3.push(resolveAllocine(imdbId, meta));
  if (localSources.includes('filmstarts')) phase3.push(resolveFilmstarts(imdbId, meta));
  if (localSources.includes('sensacine')) phase3.push(resolveSensaCine(imdbId, meta));
  if (localSources.includes('adorocinema')) phase3.push(resolveAdoroCinema(imdbId, meta));

  // DM channel search fallback (title-based, works when Wikidata IDs are missing)
  const dmConfig = LANG_CONFIG[lang];
  if (dmConfig?.dmChannel && meta?.title) {
    phase3.push(resolveDMChannel(dmConfig.dmChannel, meta.title, dmConfig.label, dmConfig.dubbedRe, dmConfig.originalRe));
  }

  const phase3Results = await Promise.all(phase3);

  // Collect all results
  const allResults = [...phase3Results];
  if (plexResult) allResults.push(plexResult);
  if (imdbResult) allResults.push(imdbResult);

  // Quality tier from largest dimension (aspect-ratio agnostic)
  const tier = (w, h) => { const m = Math.max(w, h); return m >= 3840 ? 3 : m >= 1900 ? 2 : m >= 1200 ? 1 : 0; };

  // Sort: localized sources first, then by quality tier, then bitrate
  const isLocalized = lang !== 'en';
  const seen = new Set();
  const links = allResults
    .filter(r => r !== null)
    .sort((a, b) => {
      // Localized sources always first when not English
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
    title: meta?.title || imdbId,
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
