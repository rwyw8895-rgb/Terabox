import path from "path";
import fs from "fs";
import { pipeline } from "stream/promises";
import unzipper from "unzipper";
import { execFile } from "child_process";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

export const VIDEO_EXTENSIONS = new Set([
  ".mp4",
  ".mkv",
  ".webm",
  ".mov",
  ".avi",
  ".m4v",
  ".mpeg",
  ".mpg",
  ".3gp",
  ".ts",
  ".flv",
]);

export const TERABOX_DOMAINS_PATTERN =
  /(?:terabox|terashare|terafileshare|1024tera|1024-tera|tera-box|nephobox|mirrobox|mirrorbox|momerybox|tibibox|gibibox|pebibox|4funbox|dubox|bestclouddrive)/i;

export const DISKWALA_DOMAINS_PATTERN =
  /(?:diskwala|thediskwala|diskwala\.fun|dw\.link|diskflow\.me)/i;

export function isTeraboxUrl(text: string): boolean {
  if (!text) return false;
  if (TERABOX_DOMAINS_PATTERN.test(text)) return true;
  if (
    /(?:\/s\/|\/share\/init|\/sharing\/link)\?.*?(?:surl=|s\/1)[a-zA-Z0-9_-]+|\/s\/1[a-zA-Z0-9_-]{10,}/i.test(
      text
    )
  ) {
    return true;
  }
  return false;
}

export function isDiskwalaUrl(text: string): boolean {
  if (!text) return false;
  const trimmed = text.trim();
  if (DISKWALA_DOMAINS_PATTERN.test(trimmed)) return true;
  if (/^[a-fA-F0-9]{20,32}$/.test(trimmed)) return true;
  if (/https?:\/\/.*/i.test(trimmed) && /(?:diskwala|dw\.link|diskflow)/i.test(trimmed)) return true;
  return false;
}

export function extractDiskwalaId(rawUrl: string): string | null {
  const trimmed = rawUrl.trim();
  if (/^[a-fA-F0-9]{20,32}$/.test(trimmed)) return trimmed;

  const patterns = [
    /https?:\/\/(?:www\.)?diskwala\.com\/(?:app|file)\/([a-zA-Z0-9_\-]+)/i,
    /https?:\/\/(?:www\.)?diskwala\.com\/([a-zA-Z0-9_\-]+)/i,
    /https?:\/\/(?:www\.)?thediskwala\.com\/(?:app|file)?\/([a-zA-Z0-9_\-]+)/i,
    /https?:\/\/(?:www\.)?diskwala\.fun\/(?:diskwala\/)?([a-zA-Z0-9_\-]+)/i,
    /https?:\/\/(?:www\.)?dw\.link\/([a-zA-Z0-9_\-]+)/i,
    /https?:\/\/(?:www\.)?diskflow\.me\/([a-zA-Z0-9_\-]+)/i,
  ];

  for (const pattern of patterns) {
    const match = trimmed.match(pattern);
    if (match?.[1]) return match[1];
  }

  try {
    const urlObj = new URL(trimmed);
    const pathParts = urlObj.pathname.split("/").filter(Boolean);
    const lastPart = pathParts[pathParts.length - 1];
    if (lastPart && lastPart.length >= 8 && !/[?#]/.test(lastPart)) return lastPart;
  } catch {
    // ignore invalid URLs
  }

  return null;
}

export function extractUrlFromText(text: string): string | null {
  const match = text.match(/https?:\/\/\S+/i);
  if (match) return match[0].replace(/[).,\]]+$/, "");
  const match2 = text.match(/(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\/\S+/i);
  if (match2) return "https://" + match2[0].replace(/^[a-z]+:\/\//i, "").replace(/[).,\]]+$/, "");
  return null;
}

export function cleanFilename(filename: string): string {
  if (!filename) return "terabox_download";
  try {
    filename = decodeURIComponent(filename);
  } catch {
    // ignore decode error
  }
  filename = path.basename(filename);
  filename = filename.replace(/[<>:"/\\|?*\x00-\x1f]/g, "_");
  filename = filename.trim().replace(/^[. ]+|[. ]+$/g, "");
  return filename || "terabox_download";
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

export function detectExtensionFromBuffer(buffer: Buffer): string | null {
  if (!buffer || buffer.length < 12) return null;

  // ftyp -> mp4 or mov
  if (buffer.length >= 12 && buffer.subarray(4, 8).toString("ascii") === "ftyp") {
    const brand = buffer.subarray(8, 12).toString("ascii");
    if (brand === "qt  " || brand === "moov") return ".mov";
    return ".mp4";
  }

  // Matroska / WebM
  if (
    buffer[0] === 0x1a &&
    buffer[1] === 0x45 &&
    buffer[2] === 0xdf &&
    buffer[3] === 0xa3
  ) {
    const textHeader = buffer.subarray(0, 64).toString("binary");
    if (textHeader.includes("webm")) return ".webm";
    return ".mkv";
  }

  // AVI
  if (
    buffer.subarray(0, 4).toString("ascii") === "RIFF" &&
    buffer.subarray(8, 12).toString("ascii") === "AVI "
  ) {
    return ".avi";
  }

  // MP3
  if (
    buffer.subarray(0, 3).toString("ascii") === "ID3" ||
    (buffer[0] === 0xff && (buffer[1] === 0xfb || buffer[1] === 0xf3 || buffer[1] === 0xf2))
  ) {
    return ".mp3";
  }

  // ZIP
  if (buffer[0] === 0x50 && buffer[1] === 0x4b && buffer[2] === 0x03 && buffer[3] === 0x04) {
    return ".zip";
  }

  // RAR
  if (
    buffer.subarray(0, 4).toString("ascii") === "Rar!" &&
    buffer[4] === 0x1a &&
    buffer[5] === 0x07
  ) {
    return ".rar";
  }

  // 7z
  if (
    buffer[0] === 0x37 &&
    buffer[1] === 0x7a &&
    buffer[2] === 0xbc &&
    buffer[3] === 0xaf &&
    buffer[4] === 0x27 &&
    buffer[5] === 0x1c
  ) {
    return ".7z";
  }

  // PDF
  if (buffer.subarray(0, 4).toString("ascii") === "%PDF") {
    return ".pdf";
  }

  // JPEG
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
    return ".jpg";
  }

  // PNG
  if (
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47 &&
    buffer[4] === 0x0d &&
    buffer[5] === 0x0a &&
    buffer[6] === 0x1a &&
    buffer[7] === 0x0a
  ) {
    return ".png";
  }

  return null;
}

export function extractSurl(rawUrl: string): string | null {
  try {
    const urlObj = new URL(rawUrl);
    const surlParam = urlObj.searchParams.get("surl");
    if (surlParam) {
      return surlParam.replace(/^1/, "");
    }
    const match = rawUrl.match(/\/s\/(?:1)?([a-zA-Z0-9_-]+)/);
    if (match) {
      return match[1].replace(/^1/, "");
    }
  } catch {
    const m = rawUrl.match(/(?:surl=|s\/)(?:1)?([a-zA-Z0-9_-]+)/i);
    if (m) return m[1].replace(/^1/, "");
  }
  return null;
}

export interface ResolvedTeraboxFile {
  filename: string;
  sizeBytes: number;
  sizeFormatted: string;
  isVideo: boolean;
  isZip: boolean;
  downloadUrl?: string;
  fsId?: string;
  path?: string;
  streamUrl?: string;
  thumbs?: Record<string, string>;
  sign?: string;
  timestamp?: string;
  duration?: number;
}

export interface ResolvedMetadata {
  shareId?: string;
  uk?: string;
  sign?: string;
  timestamp?: string;
  randsk?: string;
  title?: string;
  files: ResolvedTeraboxFile[];
  directDownloadPossible: boolean;
  cookies?: string;
  refererUrl?: string;
}

async function resolveDiskwalaViaPublicPage(normalizedUrl: string, id: string): Promise<ResolvedMetadata | null> {
  const candidateUrls = [
    normalizedUrl,
    `https://www.diskwala.com/file/${id}`,
    `https://www.diskwala.com/app/${id}`,
  ];

  for (const pageUrl of candidateUrls) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const pageRes = await fetch(pageUrl, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        redirect: "follow",
        signal: controller.signal,
      });
      clearTimeout(timeout);

      if (!pageRes.ok) continue;

      const html = await pageRes.text();
      const titleMatch =
        html.match(/<meta property="og:title" content="([^"]+)"/i) ||
        html.match(/<meta name="title" content="([^"]+)"/i) ||
        html.match(/<title>([^<]+)<\/title>/i);
      const descriptionMatch = html.match(/<meta property="og:description" content="([^"]+)"/i);
      const directDownloadMatch = html.match(/https?:\/\/[^\s"'<>]+\.(?:mp4|mkv|webm|avi|mov|zip|rar|pdf|mp3|m4a|jpg|jpeg|png)[^\s"'<>]*/i);
      const rawTitle = titleMatch ? titleMatch[1].trim() : descriptionMatch ? descriptionMatch[1].trim() : `Diskwala_File_${id}`;
      const fileTitle = rawTitle.replace(/\s*[-|–]\s*DiskWala.*$/i, "").trim() || `Diskwala_File_${id}`;
      const fileName = cleanFilename(fileTitle || `${id}.bin`);
      const downloadUrl = directDownloadMatch ? directDownloadMatch[0].replace(/[),.;]+$/, "") : pageUrl;

      return {
        title: fileName,
        files: [{
          filename: fileName,
          sizeBytes: 0,
          sizeFormatted: "Unknown size",
          isVideo: /\.(mp4|mkv|webm|avi|mov|m4v|ts|flv)$/i.test(fileName),
          isZip: /\.(zip|rar|7z|tar|gz)$/i.test(fileName),
          downloadUrl,
        }],
        directDownloadPossible: Boolean(downloadUrl),
        cookies: undefined,
        refererUrl: pageUrl,
      };
    } catch (err) {
      console.warn("Diskwala public page fallback failed:", err);
    }
  }

  return null;
}

export async function resolveDiskwalaLink(rawUrl: string): Promise<ResolvedMetadata> {
  const cleanUrl = rawUrl.trim();
  const id = extractDiskwalaId(cleanUrl);

  if (!id) {
    throw new Error("Invalid Diskwala URL format. Supported examples: diskwala.com/app/<id>, dw.link/<id>");
  }

  const normalizedUrl = `https://www.diskwala.com/app/${id}`;
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    Accept: "application/json,text/html,application/xhtml+xml,*/*",
    Origin: "https://diskwala.fun",
    Referer: "https://diskwala.fun/",
  };

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    const apiRes = await fetch("https://diskwala.fun/api/resolve", {
      method: "POST",
      headers: {
        ...headers,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ url: normalizedUrl }),
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (apiRes.ok) {
      const data = await apiRes.json() as any;
      if (data?.ok && data?.result) {
        const result = data.result;
        const fileName = cleanFilename(result.fileName || result.title || `${id}.bin`);
        const size = Number(result.fileSizeBytes || 0);
        const downloadUrl = result.downloadUrl || result.streamUrl || result.watchUrl || normalizedUrl;
        const streamUrl = result.streamUrl || result.watchUrl || result.downloadUrl;

        return {
          title: result.title || fileName,
          files: [{
            filename: fileName,
            sizeBytes: size,
            sizeFormatted: formatBytes(size),
            isVideo: /\.(mp4|mkv|webm|avi|mov|flv|m4v|3gp|ts)$/i.test(fileName),
            isZip: /\.(zip|rar|7z|tar|gz)$/i.test(fileName),
            downloadUrl,
            streamUrl,
          }],
          directDownloadPossible: Boolean(downloadUrl || streamUrl),
          cookies: undefined,
          refererUrl: "https://diskwala.fun/",
        };
      }

      if (data?.error === "quota_exceeded") {
        console.warn("Diskwala shared resolver quota reached, trying public web fallback:", data.message);
      }
    }
  } catch (err) {
    console.warn("Diskwala API resolve failed:", err);
  }

  const publicPageFallback = await resolveDiskwalaViaPublicPage(normalizedUrl, id);
  if (publicPageFallback) {
    return publicPageFallback;
  }

  return {
    title: `Diskwala_File_${id}`,
    files: [{
      filename: `Diskwala_File_${id}`,
      sizeBytes: 0,
      sizeFormatted: "Unknown size",
      isVideo: false,
      isZip: false,
      downloadUrl: normalizedUrl,
    }],
    directDownloadPossible: true,
    cookies: undefined,
    refererUrl: normalizedUrl,
  };
}

export async function resolveTeraboxLink(rawUrl: string): Promise<ResolvedMetadata> {
  const cleanUrl = rawUrl.trim();
  const shortCode = extractSurl(cleanUrl);

  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    Referer: "https://www.terabox.app/",
  };

  let pageText = "";
  let finalUrl = cleanUrl;
  let cookieHeader = "";

  try {
    const response = await fetch(cleanUrl, {
      headers,
      redirect: "follow",
    });

    finalUrl = response.url || cleanUrl;
    pageText = await response.text();

    const rawCookies = response.headers.get("set-cookie") || "";
    if (rawCookies) {
      cookieHeader = rawCookies
        .split(",")
        .map((c) => c.split(";")[0].trim())
        .filter(Boolean)
        .join("; ");
    }
  } catch (fetchErr) {
    console.warn("Error fetching initial TeraBox page:", fetchErr);
  }

  // Extract surl from final URL if available
  const finalSurl = extractSurl(finalUrl) || shortCode || "";
  
  // Extract embedded jsToken if found
  const jsTokenMatch = pageText.match(/fn\("([A-F0-9]+)"\)/i) ||
    pageText.match(/window\.jsToken\s*=\s*["']([^"']+)["']/i);
  const jsToken = jsTokenMatch ? jsTokenMatch[1] : "";

  // Strategy 1: Call TeraBox api/shorturlinfo & share/list API endpoint
  if (finalSurl) {
    // 1A. Try api/shorturlinfo first as it returns full sign, timestamp, randsk, and duration
    try {
      const shortUrlVariations = [
        finalSurl.startsWith("1") ? finalSurl : `1${finalSurl}`,
        finalSurl.replace(/^1/, "")
      ];

      for (const surlVariant of shortUrlVariations) {
        const infoApiUrl = `https://www.terabox.app/api/shorturlinfo?app_id=250528&shorturl=${surlVariant}&root=1&web=1&channel=dubox&clienttype=0&jsToken=${jsToken}`;
        const apiRes = await fetch(infoApiUrl, {
          headers: {
            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            Referer: finalUrl,
            Cookie: cookieHeader,
          },
        });

        if (apiRes.ok) {
          const apiData = (await apiRes.json()) as any;
          if (apiData.errno === 0 && Array.isArray(apiData.list) && apiData.list.length > 0) {
            const shareId = apiData.shareid ? String(apiData.shareid) : undefined;
            const uk = apiData.uk ? String(apiData.uk) : undefined;
            const sign = apiData.sign;
            const timestamp = apiData.timestamp ? String(apiData.timestamp) : undefined;
            const randsk = apiData.randsk ? decodeURIComponent(apiData.randsk) : undefined;

            const files: ResolvedTeraboxFile[] = [];

            for (const item of apiData.list) {
              const filename = cleanFilename(item.server_filename || item.filename || "file");
              const size = parseInt(item.size || "0", 10);
              const ext = path.extname(filename).toLowerCase();
              const duration = item.duration ? parseInt(String(item.duration), 10) : undefined;

              let streamUrl: string | undefined;
              if (shareId && uk && sign && timestamp && item.fs_id) {
                streamUrl = `https://www.terabox.app/share/streaming?app_id=250528&web=1&channel=dubox&clienttype=0&shareid=${shareId}&uk=${uk}&fid=${item.fs_id}&sign=${encodeURIComponent(
                  sign
                )}&timestamp=${timestamp}&type=M3U8_AUTO_480`;
              }

              files.push({
                filename,
                sizeBytes: size,
                sizeFormatted: formatBytes(size),
                isVideo: VIDEO_EXTENSIONS.has(ext),
                isZip: ext === ".zip" || ext === ".rar" || ext === ".7z",
                downloadUrl: item.dlink,
                fsId: item.fs_id ? String(item.fs_id) : undefined,
                path: item.path,
                streamUrl,
                thumbs: item.thumbs,
                sign,
                timestamp,
                duration,
              });
            }

            return {
              shareId,
              uk,
              sign,
              timestamp,
              randsk,
              title: files[0]?.filename || "TeraBox Files",
              files,
              directDownloadPossible: files.some((f) => !!f.downloadUrl || !!f.streamUrl),
              cookies: cookieHeader,
              refererUrl: finalUrl,
            };
          }
        }
      }
    } catch (shortInfoErr) {
      console.warn("TeraBox api/shorturlinfo attempt failed:", shortInfoErr);
    }

    // 1B. Fallback to share/list endpoint
    try {
      const listApiUrl = `https://www.terabox.app/share/list?app_id=250528&web=1&channel=dubox&clienttype=0&jsToken=${jsToken}&shorturl=${finalSurl}&root=1`;
      const apiRes = await fetch(listApiUrl, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          Referer: finalUrl,
          Cookie: cookieHeader,
        },
      });

      if (apiRes.ok) {
        const apiData = (await apiRes.json()) as any;
        if (apiData.errno === 0 && Array.isArray(apiData.list) && apiData.list.length > 0) {
          const shareId = apiData.share_id ? String(apiData.share_id) : undefined;
          const uk = apiData.uk ? String(apiData.uk) : undefined;

          const files: ResolvedTeraboxFile[] = [];

          for (const item of apiData.list) {
            const filename = cleanFilename(item.server_filename || item.filename || "file");
            const size = parseInt(item.size || "0", 10);
            const ext = path.extname(filename).toLowerCase();

            let sign: string | undefined;
            let timestamp: string | undefined;

            if (item.thumbs) {
              const anyThumb = item.thumbs.url3 || item.thumbs.url2 || item.thumbs.url1;
              if (anyThumb) {
                try {
                  const parsedThumb = new URL(anyThumb);
                  sign = parsedThumb.searchParams.get("sign") || undefined;
                  timestamp = parsedThumb.searchParams.get("time") || undefined;
                } catch {
                  // ignore url parse error
                }
              }
            }

            let streamUrl: string | undefined;
            if (shareId && uk && sign && timestamp && item.fs_id) {
              streamUrl = `https://www.terabox.app/share/streaming?app_id=250528&web=1&channel=dubox&clienttype=0&shareid=${shareId}&uk=${uk}&fid=${item.fs_id}&sign=${encodeURIComponent(
                sign
              )}&timestamp=${timestamp}&type=M3U8_AUTO_480`;
            }

            files.push({
              filename,
              sizeBytes: size,
              sizeFormatted: formatBytes(size),
              isVideo: VIDEO_EXTENSIONS.has(ext),
              isZip: ext === ".zip" || ext === ".rar" || ext === ".7z",
              downloadUrl: item.dlink,
              fsId: item.fs_id ? String(item.fs_id) : undefined,
              path: item.path,
              streamUrl,
              thumbs: item.thumbs,
              sign,
              timestamp,
            });
          }

          return {
            shareId,
            uk,
            title: files[0]?.filename || apiData.title || "TeraBox Files",
            files,
            directDownloadPossible: files.some((f) => !!f.downloadUrl || !!f.streamUrl),
            cookies: cookieHeader,
            refererUrl: finalUrl,
          };
        }
      }
    } catch (apiErr) {
      console.warn("TeraBox share/list API attempt failed:", apiErr);
    }
  }

  // Strategy 2: Check embedded initData in HTML
  const listMatch =
    pageText.match(/window\.initData\s*=\s*({.*?});/s) ||
    pageText.match(/list:\s*(\[\{.*?\}\])/s) ||
    pageText.match(/"list":\s*(\[\{.*?\}\])/s);

  const files: ResolvedTeraboxFile[] = [];

  if (listMatch) {
    try {
      const parsed = JSON.parse(listMatch[1]);
      const items = Array.isArray(parsed) ? parsed : parsed.list || [];
      for (const item of items) {
        const filename = cleanFilename(item.server_filename || item.filename || "file");
        const size = parseInt(item.size || "0", 10);
        const ext = path.extname(filename).toLowerCase();
        files.push({
          filename,
          sizeBytes: size,
          sizeFormatted: formatBytes(size),
          isVideo: VIDEO_EXTENSIONS.has(ext),
          isZip: ext === ".zip" || ext === ".rar" || ext === ".7z",
          downloadUrl: item.dlink,
          fsId: item.fs_id,
        });
      }
    } catch {
      // ignore
    }
  }

  // Strategy 3: HTML Title fallback
  if (files.length === 0) {
    const titleMatch = pageText.match(/<title>(.*?)<\/title>/i);
    let pageTitle = titleMatch ? titleMatch[1].replace(/[-_]\s*TeraBox.*$/i, "").trim() : "";
    pageTitle = cleanFilename(pageTitle) || (finalSurl ? `TeraBox_File_${finalSurl}` : "TeraBox_Shared_Content");

    const ext = path.extname(pageTitle).toLowerCase();
    files.push({
      filename: pageTitle,
      sizeBytes: 0,
      sizeFormatted: "Determining on download",
      isVideo: VIDEO_EXTENSIONS.has(ext),
      isZip: ext === ".zip" || ext === ".rar" || ext === ".7z",
    });
  }

  return {
    title: files[0]?.filename || "TeraBox Files",
    files,
    directDownloadPossible: files.some((f) => !!f.downloadUrl || !!f.streamUrl),
    cookies: cookieHeader,
    refererUrl: finalUrl,
  };
}

/**
 * Downloads an HLS / M3U8 stream into a local MP4 file.
 * To bypass TeraBox's 30-second preview limit on unauthenticated/web streams,
 * this function scans across the entire video timeline (in increments of 25 seconds)
 * using the `time` parameter to discover all chunks for the entire video duration,
 * expands each segment's byte-range to download the full chunk, and muxes them into a complete MP4.
 */
export async function downloadM3u8Stream(
  m3u8Url: string,
  outputMp4Path: string,
  refererUrl: string,
  cookieHeader?: string,
  onProgress?: (percent: number, current: number, total: number) => void,
  metadata?: {
    duration?: number;
    shareId?: string;
    uk?: string;
    sign?: string;
    timestamp?: string;
    fsId?: string;
    randsk?: string;
  }
): Promise<void> {
  const headers: Record<string, string> = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    Referer: refererUrl || "https://www.terabox.app/",
  };
  
  const effectiveCookies = [
    cookieHeader || "",
    metadata?.randsk ? `TSID=${metadata.randsk}` : ""
  ].filter(Boolean).join("; ");

  if (effectiveCookies) {
    headers["Cookie"] = effectiveCookies;
  }

  // Map to store unique video chunks by index: chunkIndex -> { url: string; size: number }
  const discoveredChunks = new Map<number, { url: string; size: number }>();

  // Helper to extract segments from an M3U8 response text
  const parseSegmentsFromM3u8 = (playlistText: string) => {
    const lines = playlistText
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l.length > 0 && !l.startsWith("#"));

    for (const segUrl of lines) {
      try {
        const parsed = new URL(segUrl);
        // Look for chunk index in URL path (e.g. _1_ts/, _2_ts/, etc.)
        const match = parsed.pathname.match(/_(\d+)_ts\b/i);
        const chunkIdx = match ? parseInt(match[1], 10) : discoveredChunks.size + 1;
        const tsSizeStr = parsed.searchParams.get("ts_size");
        const tsSize = tsSizeStr ? parseInt(tsSizeStr, 10) : 0;

        if (!discoveredChunks.has(chunkIdx)) {
          if (tsSize > 0) {
            // Expand the byte-range to download the full segment instead of a 30s slice
            parsed.searchParams.set("range", `0-${tsSize - 1}`);
            parsed.searchParams.set("len", String(tsSize));
          }
          discoveredChunks.set(chunkIdx, {
            url: parsed.toString(),
            size: tsSize,
          });
        }
      } catch {
        // Fallback for relative or non-standard URLs
        const chunkIdx = discoveredChunks.size + 1;
        if (!discoveredChunks.has(chunkIdx)) {
          discoveredChunks.set(chunkIdx, { url: segUrl, size: 0 });
        }
      }
    }
  };

  // 1. Fetch initial M3U8
  const res = await fetch(m3u8Url, { headers });
  if (!res.ok) {
    throw new Error(`Failed to fetch M3U8 playlist: HTTP ${res.status}`);
  }
  const initialPlaylistText = await res.text();
  parseSegmentsFromM3u8(initialPlaylistText);

  // 2. Multi-chunk discovery across video timeline if streaming parameters are available
  const parsedM3u8Url = new URL(m3u8Url);
  const uk = metadata?.uk || parsedM3u8Url.searchParams.get("uk");
  const shareid = metadata?.shareId || parsedM3u8Url.searchParams.get("shareid");
  const fid = metadata?.fsId || parsedM3u8Url.searchParams.get("fid");
  const sign = metadata?.sign || parsedM3u8Url.searchParams.get("sign");
  const timestamp = metadata?.timestamp || parsedM3u8Url.searchParams.get("timestamp");

  if (uk && shareid && fid && sign && timestamp) {
    // If we have duration, probe in 25-second increments to cover the full duration
    // If duration is unknown, probe up to 7200s (2 hours) or until 3 consecutive steps yield no new chunks
    const maxScanTime = metadata?.duration && metadata.duration > 0
      ? metadata.duration + 30
      : 7200;

    let emptyStreak = 0;
    const stepSeconds = 25;

    for (let t = stepSeconds; t <= maxScanTime; t += stepSeconds) {
      try {
        const timeStreamUrl = `https://www.terabox.app/share/streaming?app_id=250528&web=1&channel=dubox&clienttype=0&shareid=${shareid}&uk=${uk}&fid=${fid}&sign=${encodeURIComponent(
          sign
        )}&timestamp=${timestamp}&type=M3U8_AUTO_480&time=${t}&esl=1&isplayer=1&ehps=1`;

        const timeRes = await fetch(timeStreamUrl, { headers });
        if (timeRes.ok) {
          const timeText = await timeRes.text();
          const prevCount = discoveredChunks.size;
          parseSegmentsFromM3u8(timeText);
          if (discoveredChunks.size > prevCount) {
            emptyStreak = 0;
          } else {
            emptyStreak++;
            // If duration wasn't known and we haven't found any new chunks for 4 consecutive intervals (~100s), stop scanning
            if (!metadata?.duration && emptyStreak >= 4 && discoveredChunks.size > 0) {
              break;
            }
          }
        }
      } catch (scanErr) {
        console.warn(`Timeline probe failed at t=${t}:`, scanErr);
      }
    }
  }

  // Sort discovered chunks by index
  const sortedChunkIndices = Array.from(discoveredChunks.keys()).sort((a, b) => a - b);
  if (sortedChunkIndices.length === 0) {
    throw new Error("M3U8 playlist contained no video segments");
  }

  console.log(`Discovered ${sortedChunkIndices.length} video chunks across timeline`);

  const tempTsPath = `${outputMp4Path}.temp.ts`;
  const outWriteStream = fs.createWriteStream(tempTsPath);

  try {
    for (let i = 0; i < sortedChunkIndices.length; i++) {
      const idx = sortedChunkIndices[i];
      const chunk = discoveredChunks.get(idx)!;

      const segRes = await fetch(chunk.url, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
          Referer: refererUrl || "https://www.terabox.app/",
        },
      });

      if (!segRes.ok) {
        throw new Error(`Failed to fetch segment ${i + 1}/${sortedChunkIndices.length}`);
      }

      const arrayBuffer = await segRes.arrayBuffer();
      outWriteStream.write(Buffer.from(arrayBuffer));

      if (onProgress) {
        const percent = Math.round(((i + 1) / sortedChunkIndices.length) * 100);
        onProgress(percent, i + 1, sortedChunkIndices.length);
      }
    }
  } finally {
    await new Promise<void>((resolve) => {
      outWriteStream.end(() => resolve());
    });
  }

  // Remux .ts into .mp4 using ffmpeg for optimal streaming and compatibility
  try {
    await execFileAsync("ffmpeg", ["-y", "-i", tempTsPath, "-c", "copy", outputMp4Path]);
  } catch (ffmpegErr) {
    console.warn("FFmpeg remux warning, keeping original ts file:", ffmpegErr);
    fs.copyFileSync(tempTsPath, outputMp4Path);
  } finally {
    if (fs.existsSync(tempTsPath)) {
      fs.unlinkSync(tempTsPath);
    }
  }
}

export async function unpackZipArchive(
  zipPath: string,
  targetDir: string
): Promise<{ path: string; filename: string; size: number }[]> {
  const results: { path: string; filename: string; size: number }[] = [];
  try {
    fs.mkdirSync(targetDir, { recursive: true });
    const directory = await unzipper.Open.file(zipPath);
    const usedNames = new Set<string>();

    for (const entry of directory.files) {
      if (entry.type === "Directory" || entry.path.startsWith("__MACOSX/")) continue;

      const baseName = cleanFilename(path.basename(entry.path));
      let filename = baseName;
      let suffix = 1;
      while (usedNames.has(filename)) {
        const extension = path.extname(baseName);
        const stem = extension ? baseName.slice(0, -extension.length) : baseName;
        filename = `${stem}_${suffix++}${extension}`;
      }
      usedNames.add(filename);

      const outputPath = path.join(targetDir, filename);
      await pipeline(entry.stream(), fs.createWriteStream(outputPath));
      const stats = fs.statSync(outputPath);
      results.push({
        path: outputPath,
        filename,
        size: stats.size,
      });
    }
  } catch (err: any) {
    console.error("ZIP Unpack error:", err);
  }
  return results;
}
