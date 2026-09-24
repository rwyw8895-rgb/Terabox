import fs from "fs";
import path from "path";

export interface TelegramBotInfo {
  id: number;
  is_bot: boolean;
  first_name: string;
  username: string;
  can_join_groups: boolean;
  can_read_all_group_messages: boolean;
  supports_inline_queries: boolean;
}

export interface TelegramUpdate {
  update_id: number;
  message?: {
    message_id: number;
    from: {
      id: number;
      is_bot: boolean;
      first_name: string;
      username?: string;
    };
    chat: {
      id: number;
      first_name?: string;
      username?: string;
      type: string;
    };
    date: number;
    text?: string;
  };
  callback_query?: {
    id: string;
    data?: string;
    from: {
      id: number;
    };
    message?: {
      message_id: number;
      chat: {
        id: number;
      };
    };
  };
}

export interface TelegramBotCommand {
  command: string;
  description: string;
}

export class TelegramService {
  private token: string;
  private apiBaseUrl: string;

  constructor(token: string) {
    this.token = token.trim();
    this.apiBaseUrl = `https://api.telegram.org/bot${this.token}`;
  }

  async getMe(): Promise<TelegramBotInfo> {
    const res = await fetch(`${this.apiBaseUrl}/getMe`, { method: "GET" });
    const data = (await res.json()) as any;
    if (!data.ok) {
      throw new Error(data.description || "Failed to fetch bot details");
    }
    return data.result as TelegramBotInfo;
  }

  async setMyCommands(commands: TelegramBotCommand[]): Promise<boolean> {
    const res = await fetch(`${this.apiBaseUrl}/setMyCommands`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ commands }),
    });
    const data = (await res.json()) as any;
    if (!data.ok) {
      throw new Error(data.description || "Failed to register bot commands");
    }
    return true;
  }

  async sendMessage(
    chatId: number | string,
    text: string,
    parseMode: "Markdown" | "HTML" = "Markdown",
    replyMarkup?: object
  ): Promise<{ message_id: number }> {
    const res = await fetch(`${this.apiBaseUrl}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        parse_mode: parseMode,
        ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
      }),
    });
    const data = (await res.json()) as any;
    if (!data.ok) {
      // Fallback without parse mode if markdown parsing failed
      const fallback = await fetch(`${this.apiBaseUrl}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          text,
          ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
        }),
      });
      const fbData = (await fallback.json()) as any;
      if (!fbData.ok) {
        throw new Error(fbData.description || "Failed to send message");
      }
      return fbData.result;
    }
    return data.result;
  }

  async answerCallbackQuery(callbackQueryId: string, text?: string): Promise<boolean> {
    try {
      const res = await fetch(`${this.apiBaseUrl}/answerCallbackQuery`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          callback_query_id: callbackQueryId,
          ...(text ? { text } : {}),
        }),
      });
      const data = (await res.json()) as any;
      if (!data.ok) {
        console.warn(`Telegram deleteMessage failed: ${data.description || "unknown error"}`);
      }
      return !!data.ok;
    } catch {
      return false;
    }
  }

  async editMessageText(
    chatId: number | string,
    messageId: number,
    text: string,
    parseMode: "Markdown" | "HTML" = "Markdown",
    replyMarkup?: object
  ): Promise<boolean> {
    try {
      const res = await fetch(`${this.apiBaseUrl}/editMessageText`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: messageId,
          text,
          parse_mode: parseMode,
          ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
        }),
      });
      const data = (await res.json()) as any;
      if (!data.ok) {
        // Fallback without formatting
        await fetch(`${this.apiBaseUrl}/editMessageText`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId,
            text,
            ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
          }),
        });
      }
      return true;
    } catch {
      return false;
    }
  }

  async deleteMessage(chatId: number | string, messageId: number): Promise<boolean> {
    try {
      const res = await fetch(`${this.apiBaseUrl}/deleteMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: messageId,
        }),
      });
      const data = (await res.json()) as any;
      return !!data.ok;
    } catch {
      return false;
    }
  }

  async sendDocument(
    chatId: number | string,
    filePath: string,
    filename: string,
    caption?: string
  ): Promise<any> {
    const fileBuffer = fs.readFileSync(filePath);
    const blob = new Blob([fileBuffer]);
    const formData = new FormData();
    formData.append("chat_id", String(chatId));
    formData.append("document", blob, filename);
    if (caption) formData.append("caption", caption);

    const res = await fetch(`${this.apiBaseUrl}/sendDocument`, {
      method: "POST",
      body: formData,
    });

    if (res.status === 413) {
      throw new Error("Request Entity Too Large (file exceeds Telegram Bot API 50MB limit)");
    }

    let data: any;
    try {
      data = await res.json();
    } catch {
      throw new Error(`Telegram sendDocument HTTP ${res.status}: ${res.statusText}`);
    }

    if (!data.ok) {
      throw new Error(data.description || "Failed to send document to Telegram");
    }
    return data.result;
  }

  async sendVideo(
    chatId: number | string,
    filePath: string,
    filename: string,
    caption?: string
  ): Promise<any> {
    try {
      const fileBuffer = fs.readFileSync(filePath);
      const blob = new Blob([fileBuffer]);
      const formData = new FormData();
      formData.append("chat_id", String(chatId));
      formData.append("video", blob, filename);
      formData.append("supports_streaming", "true");
      if (caption) formData.append("caption", caption);

      const res = await fetch(`${this.apiBaseUrl}/sendVideo`, {
        method: "POST",
        body: formData,
      });

      if (res.status === 413) {
        throw new Error("Request Entity Too Large (file exceeds Telegram Bot API 50MB limit)");
      }

      let data: any;
      try {
        data = await res.json();
      } catch {
        throw new Error(`Telegram sendVideo HTTP ${res.status}: ${res.statusText}`);
      }

      if (data.ok) return data.result;
      throw new Error(data.description || "Failed to send video to Telegram");
    } catch (err: any) {
      if (err.message && err.message.includes("Request Entity Too Large")) {
        throw err;
      }
      throw err;
    }
  }

  async getUpdates(offset: number = 0, timeout: number = 10): Promise<TelegramUpdate[]> {
    try {
      const res = await fetch(
        `${this.apiBaseUrl}/getUpdates?offset=${offset}&timeout=${timeout}`,
        {
          method: "GET",
        }
      );
      const data = (await res.json()) as any;
      if (data.ok && Array.isArray(data.result)) {
        return data.result;
      }
      return [];
    } catch {
      return [];
    }
  }
}
