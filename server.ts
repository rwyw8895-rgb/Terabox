import express from "express";
import path from "path";
import fs from "fs";
import { pipeline } from "stream/promises";
import { Readable } from "stream";
import {
  isTeraboxUrl,
  isDiskwalaUrl,
  extractDiskwalaId,
  extractUrlFromText,
  resolveTeraboxLink,
  resolveDiskwalaLink,
  cleanFilename,
  formatBytes,
  detectExtensionFromBuffer,
  unpackZipArchive,
  downloadM3u8Stream,
  VIDEO_EXTENSIONS,
} from "./server/terabox.ts";
import {
  MAX_TELEGRAM_FILE_SIZE,
  splitVideo,
  splitBinaryFile,
} from "./server/splitter.ts";
import { TelegramService, TelegramBotInfo } from "./server/telegram.ts";
import { MTProtoService } from "./server/mtproto.ts";
import type { DownloadJob, ProcessedFile, BotStatus } from "./src/types.ts";
import { formatLinkCounter, getLinkCounterForUrl, normalizeLink } from "./src/linkCounter.ts";
import { classifyQueueTier, shouldPauseLargeJobForSmallerQueue } from "./src/queuePriority.ts";

const PORT = Number(process.env.PORT) || 3000;
const MAX_QUEUE_SIZE = Number(process.env.MAX_QUEUE_SIZE) || 100;
const MAX_RETRY_ATTEMPTS = 5;
const RETRY_DELAY_MS = 1500;
const DOWNLOAD_STATUS_REFRESH_MS = 1000;
const MAX_FILES_PER_LINK = 25;
const MAX_SOURCE_FILE_SIZE_BYTES = 512 * 1024 * 1024;
const MAX_ZIP_SIZE_BYTES = 250 * 1024 * 1024;
const app = express();
app.use(express.json());

async function streamResponseToFile(response: Response, filePath: string) {
  if (!response.body) {
    throw new Error("Download response did not contain a readable body");
  }

  await pipeline(
    Readable.fromWeb(response.body as import("stream/web").ReadableStream),
    fs.createWriteStream(filePath)
  );
}

async function withRetries<T>(operation: () => Promise<T>, label: string): Promise<T> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      console.warn(`${label} failed (attempt ${attempt}/${MAX_RETRY_ATTEMPTS}):`, error);
      if (attempt < MAX_RETRY_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS * attempt));
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`${label} failed`);
}

// Track public base URL for direct download links
let appPublicUrl =
  process.env.APP_URL ||
  "https://ais-dev-jya4lggt2drjhja3yk5vs7-856843567695.asia-east1.run.app";

app.use((req, res, next) => {
  if (req.headers.host) {
    const proto = (req.headers["x-forwarded-proto"] as string) || req.protocol || "http";
    const detected = `${proto}://${req.headers.host}`;
    if (!appPublicUrl || appPublicUrl.includes("localhost")) {
      appPublicUrl = detected;
    }
  }
  next();
});

// Set up data directories
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), "data");
const DOWNLOADS_DIR = path.join(DATA_DIR, "downloads");
const UNPACKED_DIR = path.join(DATA_DIR, "unpacked");
const POLLING_OFFSET_FILE = path.join(DATA_DIR, "telegram-offset.json");
fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
fs.mkdirSync(UNPACKED_DIR, { recursive: true });

function getDirectorySize(directory: string): number {
  let totalBytes = 0;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      totalBytes += getDirectorySize(entryPath);
    } else if (entry.isFile()) {
      totalBytes += fs.statSync(entryPath).size;
    }
  }
  return totalBytes;
}

function cleanupJobFiles(jobId: string) {
  for (const baseDir of [DOWNLOADS_DIR, UNPACKED_DIR]) {
    try {
      fs.rmSync(path.join(baseDir, jobId), { recursive: true, force: true });
    } catch (cleanupErr) {
      console.warn(`Could not clean temporary files for ${jobId}:`, cleanupErr);
    }
  }
}

function cleanupTemporaryDirectories() {
  for (const directory of [DOWNLOADS_DIR, UNPACKED_DIR]) {
    try {
      for (const entry of fs.readdirSync(directory)) {
        fs.rmSync(path.join(directory, entry), { recursive: true, force: true });
      }
    } catch (cleanupErr) {
      console.warn(`Could not clean temporary directory ${directory}:`, cleanupErr);
    }
  }
}

cleanupTemporaryDirectories();

// Bot configuration state
let botToken = process.env.TELEGRAM_BOT_TOKEN || "";
let apiId = process.env.TELEGRAM_API_ID || "";
let apiHash = process.env.TELEGRAM_API_HASH || "";
let telegramService: TelegramService | null = botToken ? new TelegramService(botToken) : null;
let mtprotoService: MTProtoService | null = null;

function initMTProto() {
  if (apiId && apiHash && botToken) {
    const numericApiId = parseInt(apiId, 10);
    if (!isNaN(numericApiId)) {
      mtprotoService = new MTProtoService({
        apiId: numericApiId,
        apiHash,
        botToken,
      });
      mtprotoService.connect().catch((err) => {
        console.warn("MTProto initialization warning:", err);
      });
    }
  } else {
    mtprotoService = null;
  }
}
initMTProto();
let botInfo: TelegramBotInfo | null = null;
let isPolling = false;
let pollingTimeoutId: NodeJS.Timeout | null = null;

function loadPollingOffset(): number {
  try {
    const saved = JSON.parse(fs.readFileSync(POLLING_OFFSET_FILE, "utf-8"));
    return Number.isInteger(saved.offset) && saved.offset >= 0 ? saved.offset : 0;
  } catch {
    return 0;
  }
}

function savePollingOffset(offset: number): void {
  try {
    fs.writeFileSync(POLLING_OFFSET_FILE, JSON.stringify({ offset }), "utf-8");
  } catch (err) {
    console.warn("Could not save Telegram polling offset:", err);
  }
}

let pollingOffset = loadPollingOffset();

const botCommands = [
  { command: "start", description: "Start the bot" },
  { command: "queue", description: "View the current download queue" },
  { command: "space", description: "Check server storage" },
  { command: "status", description: "Check bot status" },
  { command: "help", description: "Show help" },
];

// Jobs persistence file
const JOBS_FILE = path.join(DATA_DIR, "jobs.json");

function loadJobs(): DownloadJob[] {
  try {
    if (fs.existsSync(JOBS_FILE)) {
      const data = fs.readFileSync(JOBS_FILE, "utf-8");
      return JSON.parse(data);
    }
  } catch (err) {
    console.warn("Failed to load jobs from disk:", err);
  }
  return [];
}

function saveJobs() {
  try {
    fs.writeFileSync(JOBS_FILE, JSON.stringify(jobs.slice(0, 50), null, 2), "utf-8");
  } catch (err) {
    console.warn("Failed to save jobs to disk:", err);
  }
}

// In-memory + persisted jobs store
const jobs: DownloadJob[] = loadJobs();

type DownloadQueueTask = {
  url: string;
  chatId?: number | string;
  fileNames: string[] | null;
  sizeBytes?: number;
  sizeIsEstimated?: boolean;
  cancelRequested?: boolean;
  retryCount?: number;
  isLarge?: boolean;
  resolve: (job: DownloadJob) => void;
  reject: (error: unknown) => void;
};

type RetryQueueItem = {
  url: string;
  chatId?: number | string;
  fileNames: string[] | null;
  retryCount: number;
  maxRetries: number;
};

const LARGE_FILE_SIZE_THRESHOLD_BYTES = 200 * 1024 * 1024;
const q1DownloadQueue: DownloadQueueTask[] = [];
const q2DownloadQueue: DownloadQueueTask[] = [];
const q3DownloadQueue: DownloadQueueTask[] = [];
const q4DownloadQueue: DownloadQueueTask[] = [];
const retryQueue: RetryQueueItem[] = [];
let isDownloadInProgress = false;
let activeQueueTier: "q1" | "q2" | "q3" | "q4" | null = null;
let activeTask: DownloadQueueTask | null = null;
const activeCancellationTasks = new Map<string, DownloadQueueTask>();
let pausedActiveJob: {
  task: DownloadQueueTask;
  tier: "q1" | "q2" | "q3" | "q4";
  resume: () => void;
} | null = null;

function getQueueCounts(): { q1: number; q2: number; q3: number; q4: number } {
  return {
    q1: q1DownloadQueue.length,
    q2: q2DownloadQueue.length,
    q3: q3DownloadQueue.length,
    q4: q4DownloadQueue.length,
  };
}

function getDownloadQueueLength(): number {
  return q1DownloadQueue.length + q2DownloadQueue.length + q3DownloadQueue.length + q4DownloadQueue.length;
}

function compareQueueTasks(left: DownloadQueueTask, right: DownloadQueueTask): number {
  return (left.sizeBytes ?? Number.MAX_SAFE_INTEGER) - (right.sizeBytes ?? Number.MAX_SAFE_INTEGER);
}

function getNextQueuedTask(): DownloadQueueTask | undefined {
  const queues = [q1DownloadQueue, q2DownloadQueue, q3DownloadQueue, q4DownloadQueue];
  for (const queue of queues) {
    queue.sort(compareQueueTasks);
    if (queue.length > 0) return queue.shift();
  }
  return undefined;
}

function pushTaskToCorrectQueue(task: DownloadQueueTask): void {
  const queueTier = classifyQueueTier(task.sizeBytes ?? 0);
  const queueMap = {
    q1: q1DownloadQueue,
    q2: q2DownloadQueue,
    q3: q3DownloadQueue,
    q4: q4DownloadQueue,
  };
  queueMap[queueTier].push(task);
}

function shouldPauseActiveLargeJob(): boolean {
  if (!activeQueueTier || activeQueueTier === 'q1' || activeQueueTier === 'q2' || activeQueueTier === 'q3') {
    return false;
  }
  return shouldPauseLargeJobForSmallerQueue(activeQueueTier, getQueueCounts());
}

function hasHigherPriorityQueuedTask(): boolean {
  return q1DownloadQueue.length > 0 || q2DownloadQueue.length > 0 || q3DownloadQueue.length > 0;
}

async function yieldActiveLargeJob(task: DownloadQueueTask): Promise<void> {
  if (task.cancelRequested) {
    throw new Error("Download cancelled by user");
  }
  if (activeTask !== task || !shouldPauseActiveLargeJob() || !activeQueueTier) return;

  const tier = activeQueueTier;
  await new Promise<void>((resume) => {
    pausedActiveJob = { task, tier, resume };
    activeTask = null;
    activeQueueTier = null;
    isDownloadInProgress = false;
    processDownloadQueue();
  });
}

function getRetryDisplayName(fileNames: string[] | null, url: string): string {
  const firstName = fileNames?.find((name) => !!name?.trim());
  if (firstName) return firstName;

  try {
    const parsed = new URL(url);
    const fallback = decodeURIComponent(parsed.pathname).split("/").filter(Boolean).pop();
    return fallback || "download";
  } catch {
    return "download";
  }
}

function getRetryLabel(item: Pick<RetryQueueItem, "fileNames" | "url" | "retryCount" | "maxRetries">): string {
  return `${getRetryDisplayName(item.fileNames, item.url)} (retry ${item.retryCount}/${item.maxRetries})`;
}

function shouldRetryLink(retryCount: number, maxRetries: number): boolean {
  return retryCount < maxRetries;
}

function queueRetryJob(url: string, chatId?: number | string, fileNames: string[] | null = null, retryCount = 1): void {
  const nextRetryCount = retryCount;
  const item: RetryQueueItem = {
    url,
    chatId,
    fileNames,
    retryCount: nextRetryCount,
    maxRetries: MAX_RETRY_ATTEMPTS,
  };

  retryQueue.push(item);
}

function getKnownDownloadedSize(url: string): number | undefined {
  const normalizedUrl = normalizeLink(url);
  const previousJob = jobs.find((job) => {
    return job.status === "completed" && normalizeLink(job.url) === normalizedUrl && job.files.length > 0;
  });
  if (!previousJob) return undefined;

  return previousJob.files.reduce((total, file) => total + file.sizeBytes, 0);
}

async function resolveQueuedFileNames(task: DownloadQueueTask) {
  try {
    const resolver = isDiskwalaUrl(task.url) ? resolveDiskwalaLink : resolveTeraboxLink;
    const metadata = await withRetries(
      () => resolver(task.url),
      "Queued link inspection"
    );
    task.fileNames = metadata.files.map((file) => cleanFilename(file.filename));
    const totalEstimatedBytes = metadata.files.reduce((sum, file) => sum + (file.sizeBytes || 0), 0);
    const knownDownloadedSize = getKnownDownloadedSize(task.url);
    task.sizeBytes = knownDownloadedSize ?? totalEstimatedBytes;
    task.sizeIsEstimated = knownDownloadedSize === undefined;
    task.isLarge =
      task.sizeBytes >= LARGE_FILE_SIZE_THRESHOLD_BYTES ||
      metadata.files.some((file) => (file.sizeBytes || 0) >= LARGE_FILE_SIZE_THRESHOLD_BYTES);

    const tier = classifyQueueTier(task.sizeBytes || 0);
    const queueMap = { q1: q1DownloadQueue, q2: q2DownloadQueue, q3: q3DownloadQueue, q4: q4DownloadQueue };
    const currentQueue = queueMap[tier];
    if (!currentQueue.includes(task)) currentQueue.push(task);
  } catch {
    task.fileNames = [];
    task.sizeBytes = 0;
    task.sizeIsEstimated = false;
    task.isLarge = false;
    if (!q1DownloadQueue.includes(task)) q1DownloadQueue.push(task);
  }
}

function processDownloadQueue() {
  if (isDownloadInProgress) return;

  if (pausedActiveJob && !hasHigherPriorityQueuedTask()) {
    const paused = pausedActiveJob;
    pausedActiveJob = null;
    activeTask = paused.task;
    activeQueueTier = paused.tier;
    isDownloadInProgress = true;
    paused.resume();
    return;
  }

  if (getDownloadQueueLength() === 0 && retryQueue.length > 0) {
    const retryItem = retryQueue.shift()!;
    pushTaskToCorrectQueue({
      url: retryItem.url,
      chatId: retryItem.chatId,
      fileNames: retryItem.fileNames,
      retryCount: retryItem.retryCount,
      isLarge: false,
      resolve: () => undefined,
      reject: () => undefined,
    });
  }

  const task = getNextQueuedTask();
  if (!task) return;

  activeQueueTier = classifyQueueTier(task.sizeBytes ?? 0);
  activeTask = task;
  isDownloadInProgress = true;

  processDownloadJob(task, task.url, task.chatId, task.retryCount ?? 0)
    .then((job) => {
      if (task.resolve) task.resolve(job);
    })
    .catch((error) => {
      if (task.reject) task.reject(error);
    })
    .finally(() => {
      if (activeTask === task) {
        activeTask = null;
        activeQueueTier = null;
        isDownloadInProgress = false;
        processDownloadQueue();
      }
    });
}

function enqueueDownloadJob(url: string, chatId?: number | string): Promise<DownloadJob> {
  if (getDownloadQueueLength() >= MAX_QUEUE_SIZE) {
    return Promise.reject(new Error("The download queue is full. Please try again in a few minutes."));
  }

  return new Promise<DownloadJob>((resolve, reject) => {
    const task: DownloadQueueTask = {
      url,
      chatId,
      fileNames: null,
      isLarge: false,
      resolve,
      reject,
    };
    resolveQueuedFileNames(task)
      .then(() => {
        processDownloadQueue();
      })
      .catch((error) => {
        reject(error);
      });
  });
}

async function updateBotInfo() {
  if (!botToken) {
    botInfo = null;
    telegramService = null;
    return;
  }
  try {
    telegramService = new TelegramService(botToken);
    botInfo = await telegramService.getMe();
    await telegramService.setMyCommands(botCommands);
    console.log(`🤖 Telegram Bot authenticated: @${botInfo.username}`);
  } catch (err: any) {
    console.warn(`⚠️ Telegram authentication notice: ${err.message}`);
    botInfo = null;
  }
}

async function discardPendingTelegramUpdates(): Promise<void> {
  if (!telegramService || pollingOffset > 0) return;

  try {
    const pendingUpdates = await telegramService.getUpdates(-1, 0);
    const latestUpdate = pendingUpdates.at(-1);
    if (latestUpdate) {
      pollingOffset = latestUpdate.update_id + 1;
      savePollingOffset(pollingOffset);
    }
  } catch (err) {
    console.warn("Could not discard old Telegram updates:", err);
  }
}

// Background Telegram polling loop
async function pollTelegramUpdates() {
  if (!isPolling || !telegramService) return;

  try {
    const updates = await telegramService.getUpdates(pollingOffset, 5);
    for (const update of updates) {
      pollingOffset = update.update_id + 1;
      savePollingOffset(pollingOffset);
      const callbackQuery = update.callback_query;
      if (callbackQuery) {
        await telegramService.answerCallbackQuery(callbackQuery.id, "Cancellation requested");
        const [action, jobId] = (callbackQuery.data || "").split(":", 2);
        if (action === "cancel" && jobId) {
          const task = activeCancellationTasks.get(jobId);
          const callbackChatId = callbackQuery.message?.chat.id;
          if (task) {
            task.cancelRequested = true;
            if (callbackChatId) {
              await telegramService.sendMessage(
                callbackChatId,
                "🛑 Cancellation requested. The download will stop at the next safe checkpoint."
              );
            }
          } else if (callbackChatId) {
            await telegramService.sendMessage(callbackChatId, "ℹ️ This download has already finished.");
          }
        }
        continue;
      }
      const msg = update.message;
      if (!msg || !msg.text) continue;

      const chatId = msg.chat.id;
      const text = msg.text.trim();
      const command = text.split(/\s+/, 1)[0].split("@", 1)[0].toLowerCase();

      if (command === "/start") {
        await telegramService.sendMessage(
          chatId,
          `👋 *TeraBox Downloader Bot Active*\n\n` +
            `Send me any TeraBox link to download and unpack files!\n\n` +
            `🌐 *Supported mirrors:*\n` +
            `• terabox.com, terabox.app, teraboxlink.com\n` +
            `• 1024tera.com, 1024terabox.com, terafileshare.com\n` +
            `• nephobox, mirrobox, 4funbox, dubox, and all shortlinks!\n\n` +
            `_ZIP archives are automatically unpacked and videos are formatted for streaming._`
        );
        continue;
      }

      if (command === "/help") {
        await telegramService.sendMessage(
          chatId,
          `📖 *Help & Instructions*\n\n` +
            `1. Paste any TeraBox share link.\n` +
            `2. I’ll download the files and check that they are valid.\n` +
            `3. ZIP files are unpacked automatically.\n` +
            `4. Large files are sent in full when possible, or split into smaller parts.\n\n` +
            `📋 /queue - See the current and waiting downloads.\n` +
            `⚡ /status - See bot health and queue counts.`
        );
        continue;
      }

      if (command === "/status") {
        const activeCount = jobs.filter(
          (j) => j.status !== "completed" && j.status !== "failed"
        ).length;
        await telegramService.sendMessage(
          chatId,
          `⚡ *Bot Status:* Online\n` +
            `📥 *Active Jobs:* ${activeCount}\n` +
            `📦 *Q1:* ${q1DownloadQueue.length}\n` +
            `🟦 *Q2:* ${q2DownloadQueue.length}\n` +
            `🟧 *Q3:* ${q3DownloadQueue.length}\n` +
            `🧱 *Q4:* ${q4DownloadQueue.length}\n` +
            `🔁 *Retry Queue:* ${retryQueue.length}\n` +
            `📁 *Total Processed:* ${jobs.length}`
        );
        continue;
      }

      if (command === "/queue") {
        const activeJob = jobs.find(
          (j) => j.status !== "completed" && j.status !== "failed"
        );
        const queueGroups = [
          { label: "Q1", values: [...q1DownloadQueue].sort(compareQueueTasks) },
          { label: "Q2", values: [...q2DownloadQueue].sort(compareQueueTasks) },
          { label: "Q3", values: [...q3DownloadQueue].sort(compareQueueTasks) },
          { label: "Q4", values: [...q4DownloadQueue].sort(compareQueueTasks) },
        ];
        const queueLines = queueGroups.map(({ label, values }) => {
          const entries = values.length
            ? values.map((task, index) => {
              const fileLabel = task.fileNames === null
                ? "Checking file names..."
                : task.fileNames.length > 0
                  ? task.fileNames.join(", ")
                  : "File names unavailable";
              const sizeLabel = task.sizeBytes && task.sizeBytes > 0
                ? task.sizeIsEstimated === false
                  ? formatBytes(task.sizeBytes)
                  : `~${formatBytes(task.sizeBytes)} estimated`
                : "Size unavailable";
              return `${index + 1}. ${fileLabel} (${sizeLabel})`;
            }).join("\n")
            : "Empty";
          return `${label} (${values.length} waiting):\n${entries}`;
        });
        const activeTaskName = activeTask?.fileNames?.length
          ? activeTask.fileNames.join(", ")
          : null;
        const activeTaskSize = activeTask?.sizeBytes && activeTask.sizeBytes > 0
          ? activeTask.sizeIsEstimated === false
            ? formatBytes(activeTask.sizeBytes)
            : `~${formatBytes(activeTask.sizeBytes)} estimated`
          : null;
        const activeLine = activeTaskName
          ? `🔄 *Now processing:* ${activeTaskName}${activeTaskSize ? ` (${activeTaskSize})` : ""}\n`
          : activeJob
            ? `🔄 *Now processing:* ${activeJob.files.length > 0
              ? activeJob.files.map((file) => file.filename).join(", ")
              : "Checking file names..."}\n`
          : "🔄 *Now processing:* Nothing\n";
        const waitingLines = `\n⏳ *Queues:*\n${queueLines.join("\n\n")}`;

        await telegramService.sendMessage(
          chatId,
          `📋 *Download Queue*\n\n` +
            activeLine +
            waitingLines
        );
        continue;
      }

      if (command === "/space" || command === "/disk") {
        const filesystem = fs.statfsSync(DATA_DIR);
        const blockSize = Number(filesystem.bsize);
        const totalBytes = Number(filesystem.blocks) * blockSize;
        const freeBytes = Number(filesystem.bavail) * blockSize;
        const usedBytes = totalBytes - Number(filesystem.bfree) * blockSize;
        const usedPercent = totalBytes > 0 ? Math.round((usedBytes / totalBytes) * 100) : 0;
        const botFilesBytes = getDirectorySize(DATA_DIR);
        const temporaryDownloadsBytes = getDirectorySize(DOWNLOADS_DIR);
        const unpackedFilesBytes = getDirectorySize(UNPACKED_DIR);

        await telegramService.sendMessage(
          chatId,
          `💾 *Bot Storage*\n\n` +
            `📦 *Bot files:* ${formatBytes(botFilesBytes)}\n` +
            `⬇️ *Temporary downloads:* ${formatBytes(temporaryDownloadsBytes)}\n` +
            `🗜️ *Unpacked files:* ${formatBytes(unpackedFilesBytes)}\n` +
            `🧹 *Cleanup:* after every job and on startup\n` +
            `⏳ *Queue:* ${isDownloadInProgress ? "1 active" : "No active job"}, ${getDownloadQueueLength()} waiting (Q1 ${q1DownloadQueue.length} / Q2 ${q2DownloadQueue.length} / Q3 ${q3DownloadQueue.length} / Q4 ${q4DownloadQueue.length})\n\n` +
            `🖥️ *Container filesystem reference*\n` +
            `• *Used:* ${formatBytes(usedBytes)} (${usedPercent}%)\n` +
            `• *Free:* ${formatBytes(freeBytes)}\n` +
            `• *Total:* ${formatBytes(totalBytes)}\n` +
            `• *Location:* \`${DATA_DIR}\``
        );
        continue;
      }

      if (isTeraboxUrl(text) || isDiskwalaUrl(text)) {
        const adjustedText = extractUrlFromText(text);
        const taskUrl = adjustedText || text;
        const queuePosition = getDownloadQueueLength() + (isDownloadInProgress ? 1 : 0);
        const queueLimit = MAX_QUEUE_SIZE;

        if (queuePosition >= queueLimit) {
          await telegramService.sendMessage(
            chatId,
            `⚠️ *Queue is full.* Please try again in a few minutes.`
          );
          continue;
        }

        enqueueDownloadJob(taskUrl, chatId)
          .then((job) => {
            console.log("job enqueued", job);
          })
          .catch((error) => {
            console.error("queueing failed", error);
          });
        continue;
      }

      if (command === "/start") {
        await telegramService.sendMessage(
          chatId,
          `🤖 Bot is running. Send a TeraBox link to download.`
        );
      }

      if (command === "/cancel") {
        await telegramService.sendMessage(
          chatId,
          `🚫 Cancel is not supported in this queue.`
        );
      }
    }

  } catch (err) {
    console.warn("Polling error:", err);
  }

  if (isPolling) {
    pollingTimeoutId = setTimeout(pollTelegramUpdates, 1500);
  }
}

async function startPolling() {
  if (isPolling) return;
  isPolling = true;
  await discardPendingTelegramUpdates();
  pollTelegramUpdates();
  console.log("▶️ Telegram Polling started");
}

function stopPolling() {
  isPolling = false;
  if (pollingTimeoutId) {
    clearTimeout(pollingTimeoutId);
    pollingTimeoutId = null;
  }
  console.log("⏹️ Telegram Polling stopped");
}

// Visual progress bar generator for Telegram messages
function createProgressBar(percent: number, length: number = 10): string {
  const bounded = Math.max(0, Math.min(100, Math.round(percent)));
  const filledCount = Math.round((bounded / 100) * length);
  const emptyCount = length - filledCount;
  return "■".repeat(filledCount) + "□".repeat(emptyCount);
}

// Core execution engine
async function processDownloadJob(
  queueTask: DownloadQueueTask,
  url: string,
  chatId?: number | string,
  retryCount = 0
): Promise<DownloadJob> {
  const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
  const jobDir = path.join(DOWNLOADS_DIR, jobId);
  fs.mkdirSync(jobDir, { recursive: true });

  const linkCounter = getLinkCounterForUrl(url);
  const job: DownloadJob = {
    id: jobId,
    url,
    linkCounter,
    status: "resolving",
    progress: 10,
    statusText: "Analyzing TeraBox share link...",
    files: [],
    chatId: chatId ? String(chatId) : undefined,
    retryCount,
    maxRetries: MAX_RETRY_ATTEMPTS,
    createdAt: Date.now(),
    logs: [`[${new Date().toLocaleTimeString()}] Job initialized for ${url} (link ${formatLinkCounter(url)})`],
  };

  jobs.unshift(job);
  activeCancellationTasks.set(jobId, queueTask);
  if (jobs.length > 50) jobs.pop();
  saveJobs();

  let tgStatusMsgId: number | null = null;
  const cancelReplyMarkup = {
    inline_keyboard: [[
      { text: "Cancel download", callback_data: `cancel:${jobId}` },
    ]],
  };
  if (chatId && telegramService) {
    try {
      const counterLabel = formatLinkCounter(url);
      const sent = await telegramService.sendMessage(
        chatId,
        `⏳ *TeraBox Downloader*\n\n` +
          `🔗 *Link:* ${counterLabel}\n` +
          `\`${createProgressBar(10)}\` 10%\n` +
          `• *Status:* Getting everything ready...`,
        "Markdown",
        cancelReplyMarkup
      );
      tgStatusMsgId = sent.message_id;
    } catch {
      // ignore
    }
  }

  let lastTgUpdateTime = 0;
  let lastTgProgress = -1;

  const removeTelegramStatusMessage = async () => {
    if (chatId && telegramService && tgStatusMsgId) {
      const messageId = tgStatusMsgId;
      for (let attempt = 0; attempt < 5; attempt++) {
        if (await telegramService.deleteMessage(chatId, messageId)) {
          tgStatusMsgId = null;
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      await telegramService.editMessageText(
        chatId,
        messageId,
        "✅ Completed",
        "Markdown",
        { inline_keyboard: [] }
      );
      console.warn(`Could not delete Telegram processing message ${messageId}; removed its cancel button instead.`);
      tgStatusMsgId = null;
    }
  };

  const updateStatus = async (
    status: DownloadJob["status"],
    progress: number,
    text: string,
    sizeLabel?: string,
    forceTelegramUpdate: boolean = false
  ) => {
    job.status = status;
    job.progress = progress;
    job.statusText = text;
    job.logs.push(`[${new Date().toLocaleTimeString()}] ${text}`);

    if (chatId && telegramService && tgStatusMsgId) {
      const now = Date.now();
      const roundedProgress = Math.round(progress);
      // Throttle telegram message edits to max once every 1.5 seconds or on major milestone to prevent Telegram 429 rate limit
      if (
        forceTelegramUpdate ||
        (now - lastTgUpdateTime > DOWNLOAD_STATUS_REFRESH_MS && Math.abs(roundedProgress - lastTgProgress) >= 5) ||
        roundedProgress === 100
      ) {
        lastTgUpdateTime = now;
        lastTgProgress = roundedProgress;
        const bar = createProgressBar(roundedProgress, 12);
        const barLine = sizeLabel
          ? `[${bar}] *${roundedProgress}%* (${sizeLabel})`
          : `[${bar}] *${roundedProgress}%*`;
        try {
          const counterLabel = formatLinkCounter(url);
          await telegramService.editMessageText(
            chatId,
            tgStatusMsgId,
            `⏳ *TeraBox Processing*\n\n` +
              `🔗 *Link:* ${counterLabel}\n\n` +
              `${barLine}\n\n` +
                  `• *Progress:* ${text}`,
              "Markdown",
              cancelReplyMarkup
          );
        } catch {
          // ignore edit conflicts
        }
      }
    }
  };

  try {
    await updateStatus("resolving", 25, isDiskwalaUrl(url) ? "Checking your Diskwala link..." : "Checking your TeraBox link...");
    const metadata = await withRetries(
      () => (isDiskwalaUrl(url) ? resolveDiskwalaLink(url) : resolveTeraboxLink(url)),
      isDiskwalaUrl(url) ? "Diskwala link resolution" : "TeraBox link resolution"
    );

    const processedFiles: ProcessedFile[] = [];
    const sourceFiles = metadata.files.filter((file) => file.downloadUrl || file.streamUrl);
    if (sourceFiles.length === 0) {
      throw new Error("No downloadable files were found in this TeraBox link.");
    }
    if (sourceFiles.length > MAX_FILES_PER_LINK) {
      throw new Error(`This link contains too many files. The maximum is ${MAX_FILES_PER_LINK}.`);
    }

    const totalSourceSize = sourceFiles.reduce((sum, file) => sum + (file.sizeBytes || 0), 0);
    const knownDownloadedSize = getKnownDownloadedSize(url);
    const processingSizeLabel = knownDownloadedSize
      ? formatBytes(knownDownloadedSize)
      : `~${formatBytes(totalSourceSize)} estimated`;
    await updateStatus(
      "downloading",
      45,
      `Downloading ${metadata.title || "your file"}...`,
      processingSizeLabel
    );

    const failedFiles: string[] = [];
    for (let fileIndex = 0; fileIndex < sourceFiles.length; fileIndex++) {
      if (queueTask.cancelRequested) {
        throw new Error("Download cancelled by user");
      }
      const sourceFile = sourceFiles[fileIndex];
      const displayName = cleanFilename(sourceFile.filename || `file_${fileIndex + 1}`);
      let downloadedFilePath = "";

      try {
        if (sourceFile.sizeBytes > MAX_SOURCE_FILE_SIZE_BYTES) {
          throw new Error("This file is too large for the available server memory and storage.");
        }

        await updateStatus(
          "downloading",
          Math.min(80, 20 + Math.round((fileIndex / sourceFiles.length) * 60)),
          `Downloading ${displayName} (${fileIndex + 1}/${sourceFiles.length})...`,
          processingSizeLabel
        );

        let candidateName = displayName;
        let hasDownloadedFile = false;
        downloadedFilePath = path.join(jobDir, `${fileIndex}_${candidateName}`);

        if (sourceFile.downloadUrl) {
          try {
            const streamRes = await withRetries(
              async () => {
                const response = await fetch(sourceFile.downloadUrl!, {
                  headers: {
                    "User-Agent":
                      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0",
                    Referer: metadata.refererUrl || "https://www.terabox.app/",
                    ...(metadata.cookies ? { Cookie: metadata.cookies } : {}),
                  },
                });
                if (!response.ok) {
                  throw new Error(`Download returned HTTP ${response.status}`);
                }
                return response;
              },
              `Direct download for ${displayName}`
            );
            await streamResponseToFile(streamRes, downloadedFilePath);
            hasDownloadedFile = fs.statSync(downloadedFilePath).size > 0;
          } catch (downloadErr) {
            console.warn(`Direct download failed for ${displayName}:`, downloadErr);
          }
        }

        if (!hasDownloadedFile && sourceFile.streamUrl) {
          await updateStatus("downloading", 50, `Preparing ${displayName}...`);
          if (!candidateName.toLowerCase().endsWith(".mp4")) {
            candidateName = `${candidateName.replace(/\.[^.]+$/, "")}.mp4`;
            downloadedFilePath = path.join(jobDir, `${fileIndex}_${candidateName}`);
          }

          await withRetries(
            async () => {
              if (fs.existsSync(downloadedFilePath)) {
                fs.rmSync(downloadedFilePath, { force: true });
              }
              await downloadM3u8Stream(
                sourceFile.streamUrl!,
                downloadedFilePath,
                metadata.refererUrl || "https://www.terabox.app/",
                metadata.cookies,
                async (percent) => {
                  const fileStart = 20 + Math.round((fileIndex / sourceFiles.length) * 60);
                  const fileProgress = Math.round(60 / sourceFiles.length);
                  const calculatedProgress = Math.min(80, fileStart + Math.round((percent / 100) * fileProgress));
                  await updateStatus(
                    "downloading",
                    calculatedProgress,
                    `Downloading ${displayName}... ${percent}%`,
                    processingSizeLabel
                  );
                },
                {
                  duration: sourceFile.duration,
                  shareId: metadata.shareId,
                  uk: metadata.uk,
                  sign: metadata.sign || sourceFile.sign,
                  timestamp: metadata.timestamp || sourceFile.timestamp,
                  fsId: sourceFile.fsId,
                  randsk: metadata.randsk,
                  beforeChunk: () => yieldActiveLargeJob(queueTask),
                }
              );
            },
            `Video download for ${displayName}`
          );

          if (fs.existsSync(downloadedFilePath) && fs.statSync(downloadedFilePath).size > 0) {
            hasDownloadedFile = true;
          }
        }

        if (!hasDownloadedFile) {
          throw new Error("The file could not be downloaded");
        }

        const fileHeader = fs.readFileSync(downloadedFilePath).subarray(0, 64);
        const detectedExt = detectExtensionFromBuffer(fileHeader);
        if (detectedExt && !candidateName.toLowerCase().endsWith(detectedExt)) {
          const newName = `${candidateName}${detectedExt}`;
          const newPath = path.join(jobDir, `${fileIndex}_${newName}`);
          fs.renameSync(downloadedFilePath, newPath);
          downloadedFilePath = newPath;
          candidateName = newName;
        }

        const stats = fs.statSync(downloadedFilePath);
        const isZip = candidateName.toLowerCase().endsWith(".zip") || detectedExt === ".zip";
        const isVideo = VIDEO_EXTENSIONS.has(path.extname(candidateName).toLowerCase());
        let unpackedCount = 0;

        if (isZip && stats.size > MAX_ZIP_SIZE_BYTES) {
          throw new Error("This ZIP archive is too large to unpack safely.");
        }

        if (isZip) {
          await updateStatus("unpacking", 70, `📦 Unpacking ${displayName}...`);
          const unpackDir = path.join(UNPACKED_DIR, jobId, String(fileIndex));
          fs.mkdirSync(unpackDir, { recursive: true });
          const unpackedList = await unpackZipArchive(downloadedFilePath, unpackDir);
          for (const unpackedFile of unpackedList) {
            unpackedCount++;
            const unpackedExt = path.extname(unpackedFile.filename).toLowerCase();
            processedFiles.push({
              filename: unpackedFile.filename,
              sizeBytes: unpackedFile.size,
              sizeFormatted: formatBytes(unpackedFile.size),
              path: unpackedFile.path,
              isVideo: VIDEO_EXTENSIONS.has(unpackedExt),
              isZip: false,
            });
          }
        }

        if (!isZip || unpackedCount === 0) {
          processedFiles.push({
            filename: candidateName,
            sizeBytes: stats.size,
            sizeFormatted: formatBytes(stats.size),
            path: downloadedFilePath,
            isVideo,
            isZip,
          });
        }
      } catch (fileErr) {
        console.warn(`Could not process ${displayName}:`, fileErr);
        if (fs.existsSync(downloadedFilePath)) {
          fs.rmSync(downloadedFilePath, { force: true });
        }
        failedFiles.push(displayName);
      }
    }

    if (processedFiles.length === 0) {
      throw new Error("None of the files in this TeraBox link could be downloaded.");
    }

    if (failedFiles.length > 0 && chatId && telegramService) {
      await telegramService.sendMessage(
        chatId,
        `⚠️ I could not download ${failedFiles.length} file(s): ${failedFiles.join(", ")}. I’ll still send the files that worked.`
      );
    }

    // Assign direct download URLs
    for (let i = 0; i < processedFiles.length; i++) {
      const pf = processedFiles[i];
      pf.downloadUrl = appPublicUrl
        ? `${appPublicUrl}/api/downloads/${job.id}/${i}`
        : `/api/downloads/${job.id}/${i}`;
    }

    job.files = processedFiles;

    // Telegram delivery if requested
    if (chatId && telegramService) {
      const sendTemporaryTelegramNotice = async (text: string) => {
        const notice = await telegramService!.sendMessage(chatId, text);
        setTimeout(() => {
          telegramService?.deleteMessage(chatId, notice.message_id).catch(() => undefined);
        }, 10000);
      };

      await updateStatus(
        "uploading",
        85,
        `📤 Sending ${processedFiles.length} file(s) to you...`
      );

      for (let i = 0; i < processedFiles.length; i++) {
        const pf = processedFiles[i];
        if (!pf.path || !fs.existsSync(pf.path)) continue;

        const isExceedingTelegramLimit = pf.sizeBytes > MAX_TELEGRAM_FILE_SIZE;

        try {
          if (isExceedingTelegramLimit) {
            await sendTemporaryTelegramNotice(`📦 *Large file detected.* Sending...`);

            let uploadedViaMTProto = false;
            if (mtprotoService) {
              try {
                const videoCaption = `🎬 *[Video Preview]* ${formatLinkCounter(url)} \`${pf.filename}\` (${pf.sizeFormatted})`;
                await mtprotoService.sendFile(
                  chatId,
                  pf.path,
                  pf.filename,
                  videoCaption,
                  async (pct) => {
                    const uploadProgress = Math.min(98, 85 + Math.round((pct / 100) * 12));
                    await updateStatus("uploading", uploadProgress, `Sending your video... ${pct}%`);
                  },
                  false
                );
                uploadedViaMTProto = true;
              } catch (mtErr: any) {
                console.warn("MTProto upload failed, falling back to parts:", mtErr.message);
              }
            }

            if (!uploadedViaMTProto) {
              if (pf.isVideo) {
                const parts = await splitVideo(pf.path, jobDir, MAX_TELEGRAM_FILE_SIZE);
                pf.splitPartsCount = parts.length;
                for (let pIdx = 0; pIdx < parts.length; pIdx++) {
                  const part = parts[pIdx];
                  const videoPartCaption = `🎬 *[Video Part ${pIdx + 1}/${parts.length}]* ${formatLinkCounter(url)} \`${part.filename}\` (${formatBytes(part.size)})`;
                  await telegramService.sendVideo(chatId, part.path, part.filename, videoPartCaption);
                }
              } else {
                const parts = await splitBinaryFile(pf.path, jobDir, MAX_TELEGRAM_FILE_SIZE);
                pf.splitPartsCount = parts.length;
                for (let pIdx = 0; pIdx < parts.length; pIdx++) {
                  const part = parts[pIdx];
                  const partCaption = `📦 *[Part ${pIdx + 1}/${parts.length}]* ${formatLinkCounter(url)} \`${part.filename}\` (${formatBytes(part.size)})`;
                  await telegramService.sendDocument(chatId, part.path, part.filename, partCaption);
                }
              }
            }
          } else {
            // File is <= 50 MB: send videos as streamable Telegram videos.
            if (pf.isVideo) {
              const videoCaption = `🎬 *[Video Preview]* ${formatLinkCounter(url)} \`${pf.filename}\` (${pf.sizeFormatted})`;
              await telegramService.sendVideo(chatId, pf.path, pf.filename, videoCaption);
            } else {
              const caption = `📄 *[${i + 1}/${processedFiles.length}]* ${formatLinkCounter(url)} \`${pf.filename}\` (${pf.sizeFormatted})`;
              await telegramService.sendDocument(chatId, pf.path, pf.filename, caption);
            }
          }
        } catch (uploadErr: any) {
          console.error(`Failed to send file ${pf.filename} to telegram:`, uploadErr);

          // Dynamic fallback if Telegram returns 413 Request Entity Too Large
          if (
            uploadErr.message?.includes("Request Entity Too Large") ||
            uploadErr.message?.includes("50MB limit") ||
            uploadErr.message?.includes("too big")
          ) {
            try {
              await sendTemporaryTelegramNotice(`📦 *Large file detected.* Sending it in parts...`);
              const parts = pf.isVideo
                ? await splitVideo(pf.path, jobDir, 45 * 1024 * 1024)
                : await splitBinaryFile(pf.path, jobDir, 45 * 1024 * 1024);
              pf.splitPartsCount = parts.length;
              for (let pIdx = 0; pIdx < parts.length; pIdx++) {
                const part = parts[pIdx];
                const partCaption = `📁 *[Part ${pIdx + 1}/${parts.length}]* ${formatLinkCounter(url)} \`${part.filename}\` (${formatBytes(part.size)})`;
                if (part.isVideo) {
                  await telegramService.sendVideo(chatId, part.path, part.filename, partCaption);
                } else {
                  await telegramService.sendDocument(chatId, part.path, part.filename, partCaption);
                }
              }
            } catch (splitErr: any) {
              await telegramService.sendMessage(
                chatId,
                `⚠️ I couldn’t send \`${pf.filename}\`. Please try again or use a smaller file.`
              );
            }
          } else {
            await telegramService.sendMessage(
              chatId,
              `⚠️ I couldn’t send \`${pf.filename}\`. Please try again.`
            );
          }
        }
      }

      await removeTelegramStatusMessage();

    }

    job.status = "completed";
    job.progress = 100;
    job.statusText = "Completed successfully";
    job.completedAt = Date.now();
    job.logs.push(`[${new Date().toLocaleTimeString()}] Finished job processing`);
    saveJobs();
  } catch (err: any) {
    console.error("Job processing failed:", err);
    if (err?.message === "Download cancelled by user") {
      job.status = "failed";
      job.error = "Cancelled by user";
      job.statusText = "Cancelled by user";
      saveJobs();
      if (chatId && telegramService) {
        await removeTelegramStatusMessage();
        await telegramService.sendMessage(chatId, `🛑 *Download cancelled.*\n\n🔗 ${url}`);
      }
      cleanupJobFiles(jobId);
      activeCancellationTasks.delete(jobId);
      return job;
    }
    const nextRetryCount = (job.retryCount ?? 0) + 1;
    const canRetry = shouldRetryLink(nextRetryCount, MAX_RETRY_ATTEMPTS);

    if (canRetry) {
      job.retryCount = nextRetryCount;
      job.status = "pending";
      job.statusText = `Retrying download (${nextRetryCount}/${MAX_RETRY_ATTEMPTS})...`;
      job.logs.push(`[${new Date().toLocaleTimeString()}] Retrying link (attempt ${nextRetryCount}/${MAX_RETRY_ATTEMPTS})`);
      saveJobs();

      const retryFileNames = job.files.length > 0 ? job.files.map((file) => file.filename) : null;
      queueRetryJob(url, chatId, retryFileNames, nextRetryCount);

      if (chatId && telegramService) {
        await removeTelegramStatusMessage();
        const label = getRetryLabel({
          fileNames: retryFileNames,
          url,
          retryCount: nextRetryCount,
          maxRetries: MAX_RETRY_ATTEMPTS,
        });
        await telegramService.sendMessage(
          chatId,
          `🔁 *Retrying download*\n\n` +
            `• ${label}`
        );
      }

      return job;
    }

    job.status = "failed";
    job.error = err.message || "Unknown download error";
    job.statusText = `Failed: ${job.error}`;
    job.logs.push(`[${new Date().toLocaleTimeString()}] Error: ${job.error}`);
    saveJobs();

    if (chatId && telegramService) {
      await removeTelegramStatusMessage();
      await telegramService.sendMessage(
        chatId,
        `❌ *Unable to download this link*\n\n` +
          `🔗 ${url}\n\n` +
          `I couldn’t download it after ${MAX_RETRY_ATTEMPTS} attempts. It may be expired, private, or protected.`
      );
    }
  }

  cleanupJobFiles(jobId);
  activeCancellationTasks.delete(jobId);

  return job;
}

// Initialize Telegram bot info on startup
updateBotInfo().then(() => {
  if (botToken) {
    startPolling();
  }
});

// ==========================================
// REST API ROUTES
// ==========================================

app.get("/api/status", (req, res) => {
  const status: BotStatus = {
    hasToken: !!botToken,
    tokenMasked: botToken
      ? `${botToken.slice(0, 4)}...${botToken.slice(-4)}`
      : undefined,
    isOnline: !!botInfo,
    isPolling,
    botInfo: botInfo
      ? {
          id: botInfo.id,
          username: botInfo.username,
          first_name: botInfo.first_name,
          can_join_groups: botInfo.can_join_groups,
        }
      : undefined,
    hasApiCredentials: !!(apiId && apiHash),
    uploadLimitMb: apiId && apiHash ? 2000 : 50,
    dataDir: DATA_DIR,
    totalJobsCount: jobs.length,
    completedJobsCount: jobs.filter((j) => j.status === "completed").length,
  };
  res.json(status);
});

app.post("/api/bot/config", async (req, res) => {
  const { token, newApiId, newApiHash } = req.body;
  if (token !== undefined) {
    botToken = token.trim();
  }
  if (newApiId !== undefined) {
    apiId = newApiId.trim();
  }
  if (newApiHash !== undefined) {
    apiHash = newApiHash.trim();
  }

  await updateBotInfo();
  initMTProto();
  if (botToken && !isPolling) {
    startPolling();
  } else if (!botToken && isPolling) {
    stopPolling();
  }

  res.json({ success: true, isOnline: !!botInfo, botInfo });
});

app.post("/api/bot/toggle-polling", (req, res) => {
  if (!botToken) {
    return res.status(400).json({ error: "Telegram Bot Token is not set." });
  }
  if (isPolling) {
    stopPolling();
  } else {
    startPolling();
  }
  res.json({ isPolling });
});

app.post("/api/bot/test", async (req, res) => {
  const { token } = req.body;
  const testToken = token || botToken;
  if (!testToken) {
    return res.status(400).json({ error: "No token provided" });
  }
  try {
    const svc = new TelegramService(testToken);
    const info = await svc.getMe();
    res.json({ success: true, info });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post("/api/terabox/resolve", async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ error: "URL is required" });
  }
  if (!isTeraboxUrl(url) && !isDiskwalaUrl(url)) {
    return res.status(400).json({ error: "URL does not match supported TeraBox or Diskwala domains" });
  }
  try {
    const meta = isDiskwalaUrl(url) ? await resolveDiskwalaLink(url) : await resolveTeraboxLink(url);
    res.json(meta);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/jobs", async (req, res) => {
  const { url, chatId } = req.body;
  if (!url) {
    return res.status(400).json({ error: "URL is required" });
  }
  if (!isTeraboxUrl(url) && !isDiskwalaUrl(url)) {
    return res.status(400).json({ error: "Invalid TeraBox or Diskwala URL format" });
  }

  if (req.headers.host) {
    const proto = (req.headers["x-forwarded-proto"] as string) || req.protocol || "http";
    appPublicUrl = `${proto}://${req.headers.host}`;
  }

  const cleanUrl = extractUrlFromText(url) || url;
  const job = await enqueueDownloadJob(cleanUrl, chatId);
  res.json(job);
});

app.get("/api/jobs", (req, res) => {
  res.json(jobs);
});

app.get("/api/jobs/:id", (req, res) => {
  const job = jobs.find((j) => j.id === req.params.id);
  if (!job) {
    return res.status(400).json({ error: "Job not found" });
  }
  res.json(job);
});

app.get("/api/downloads/:jobId/:fileIndex", (req, res) => {
  try {
    const { jobId, fileIndex } = req.params;
    let job = jobs.find((j) => j.id === jobId);

    // If job not in memory, check persisted jobs on disk
    if (!job) {
      const persisted = loadJobs();
      job = persisted.find((j) => j.id === jobId);
    }

    const idx = parseInt(fileIndex, 10);
    let filePath: string | null = null;
    let filename: string = "download.mp4";

    if (job && job.files && job.files[idx]) {
      const file = job.files[idx];
      filePath = file.path ?? null;
      filename = file.filename;
    } else {
      // Fallback: check download folder directly
      const folderPath = path.join(DOWNLOADS_DIR, jobId);
      if (fs.existsSync(folderPath)) {
        const dirFiles = fs.readdirSync(folderPath);
        if (dirFiles.length > 0) {
          const chosen = dirFiles[idx] || dirFiles[0];
          filePath = path.join(folderPath, chosen);
          filename = chosen;
        }
      }
    }

    if (!filePath || !fs.existsSync(filePath)) {
      return res.status(404).send("Requested file was not found on the server");
    }

    const stat = fs.statSync(filePath);
    res.setHeader("Content-Disposition", `attachment; filename="${encodeURIComponent(filename)}"`);
    res.setHeader("Content-Type", filename.endsWith(".mp4") ? "video/mp4" : "application/octet-stream");
    res.setHeader("Content-Length", stat.size);

    const stream = fs.createReadStream(filePath);
    stream.on("error", (err) => {
      console.error("Stream download error:", err);
      if (!res.headersSent) {
        res.status(500).send("Error streaming file");
      }
    });
    stream.pipe(res);
  } catch (err: any) {
    console.error("Download route error:", err);
    if (!res.headersSent) {
      res.status(500).send(`Server error: ${err.message}`);
    }
  }
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", bot: botInfo?.username || "unknown", uptime: process.uptime() });
});

app.post("/api/telegram/webhook", async (req, res) => {
  // Webhook handler support
  const update = req.body;
  if (update?.message?.text && (isTeraboxUrl(update.message.text) || isDiskwalaUrl(update.message.text))) {
    const chatId = update.message.chat.id;
    const url = extractUrlFromText(update.message.text) || update.message.text;
    enqueueDownloadJob(url, chatId).catch(console.error);
  }
  res.json({ ok: true });
});

async function startServer() {
  app.listen(PORT, "0.0.0.0", async () => {
    console.log(`🚀 TeraBox Telegram Bot Server running on http://0.0.0.0:${PORT}`);
    await updateBotInfo();
    if (botToken) {
      startPolling();
    }
  });
}

startServer();
