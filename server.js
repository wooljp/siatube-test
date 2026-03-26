const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 5000;
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingTimeout: 120000,
  pingInterval: 30000,
  transports: ['websocket', 'polling'],
  allowEIO3: true,
  maxHttpBufferSize: 1e6,
  connectTimeout: 45000
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'chat.html'));
});

app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    storage: db.isUsingDatabase() ? 'postgresql' : 'not connected'
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server is running on port ${PORT}`);
});

const MAX_HISTORY = db.MAX_HISTORY;

function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

let users = {};
let messages = [];
const onlineUsers = new Map();
const userSockets = new Map();
const adminUsers = new Set();
const mutedUsers = new Map();
const bannedUsers = new Set();
const userStatusMap = new Map();
const userIpMap = new Map();
const userMessageHistory = new Map(); // Anti-spam history
const userLastMessageTime = new Map(); // Rate limiting: 1 message per second

function getClientIp(socket) {
  let ip;
  const forwardedFor = socket.handshake.headers['x-forwarded-for'];
  if (forwardedFor) {
    ip = forwardedFor.split(',')[0].trim();
  } else {
    ip = socket.handshake.address;
  }
  return db.normalizeIpAddress(ip);
}

function addUserSocket(displayName, socketId) {
  if (!userSockets.has(displayName)) {
    userSockets.set(displayName, new Set());
  }
  userSockets.get(displayName).add(socketId);
}

function removeUserSocket(displayName, socketId) {
  if (userSockets.has(displayName)) {
    userSockets.get(displayName).delete(socketId);
    if (userSockets.get(displayName).size === 0) {
      userSockets.delete(displayName);
      return true;
    }
  }
  return false;
}

function getUniqueOnlineUsers() {
  return Array.from(userSockets.keys());
}

const fortunes = [
  { result: '大吉', weight: 80 },
  { result: '中吉', weight: 120 },
  { result: '小吉', weight: 200 },
  { result: '吉', weight: 150 },
  { result: '凶', weight: 195 },
  { result: '大凶', weight: 150 },
  { result: 'あれれ...結果が表示されませんでした。', weight: 30 },
  { result: '極大吉', weight: 30 },
  { result: 'ミラクル,1%をあてた。', weight: 10 },
  { result: '???', weight: 30 },
  { result: 'おめでとう!!!🍫プレゼント!!!!!', weight: 5 },
];

function drawFortune() {
  const totalWeight = fortunes.reduce((sum, f) => sum + f.weight, 0);
  let random = Math.random() * totalWeight;
  for (const fortune of fortunes) {
    random -= fortune.weight;
    if (random <= 0) {
      return fortune;
    }
  }
  return fortunes[0];
}

function checkMuted(username) {
  if (mutedUsers.has(username)) {
    const muteInfo = mutedUsers.get(username);
    if (Date.now() < muteInfo.until) {
      const remaining = Math.ceil((muteInfo.until - Date.now()) / 1000);
      return { muted: true, remaining };
    } else {
      mutedUsers.delete(username);
    }
  }
  return { muted: false };
}

async function processCommand(command, username, socket, isAdmin) {
  const parts = command.trim().split(' ');
  const cmd = parts[0].toLowerCase();
  const args = parts.slice(1);

  switch (cmd) {
    case '/delete':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      messages = [];
      await db.deleteAllMessages();
      io.emit('allMessagesDeleted');
      return { type: 'system', message: '管理者がすべてのメッセージを削除しました' };

    case '/rule':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }

      const ruleMessageText = `私は ${username} って言います！ここでは管理者をしてます
まずはchoco-chatのルール説明を軽くさせてもらいます。
ルール①…他の人が嫌がることはしないでください！
Ex)暴言、過度な発言、ログイン・ログアウトを繰り返すことなどetc
ルール②…話し方や話す内容は自由！ただし、ルール①に違反する内容はダメです。

+α役職がある人とない人で上下関係は全くないので相手が良ければ是非タメ口で喋ってあげてください！
まとめ！①、②に違反する行為が確認された場合それなりの罰則があるのでわかっていてほしいです！
最後に自己紹介をお願いしたいです！
呼んでほしい名前と一言お願いします！是非楽しんでいってください！`;

      if (args.length >= 1) {
        // Targeted rule (Private to the user)
        const ruleTarget = args[0];
        if (!userSockets.has(ruleTarget)) {
          return { type: 'error', message: 'そのユーザーはオンラインではありません' };
        }

        const ruleTargetData = {
          id: generateId(),
          username: isAdmin ? username + ' 管理者' : username,
          message: `@${ruleTarget} さんへ (個別案内)\n\n${ruleMessageText}`,
          color: users[username]?.color || '#000000',
          timestamp: new Date().toISOString(),
          isCommandResult: true
        };

        const targetSocketSet = userSockets.get(ruleTarget);
        for (const sid of targetSocketSet) {
          const targetSocketObj = io.sockets.sockets.get(sid);
          if (targetSocketObj) {
            targetSocketObj.emit('message', ruleTargetData);
          }
        }

        socket.emit('systemMessage', `${ruleTarget} さんに個別ルール説明を送信しました`);
        return { type: 'private', message: `${ruleTarget} さんへの個別送信完了` };
      } else {
        // Broadcast rule (To everyone)
        const ruleBroadcastData = {
          id: generateId(),
          username: isAdmin ? username + ' 管理者' : username,
          message: `【全体案内】\n\n${ruleMessageText}`,
          color: users[username]?.color || '#000000',
          timestamp: new Date().toISOString(),
          isCommandResult: true
        };
        await addMessageToStorage(ruleBroadcastData);
        io.emit('message', ruleBroadcastData);
        return { type: 'private', message: `全員にルール説明を表示しました` };
      }

    case '/prmdelete':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      await db.deleteAllPrivateMessages();
      io.emit('allPrivateMessagesDeleted');
      return { type: 'system', message: '管理者がすべてのプライベートメッセージを削除しました' };

    case '/system':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      const nowTime = Date.now();
      const muteList = [];
      for (const [u, info] of mutedUsers.entries()) {
        if (info.until > nowTime) {
          const rem = Math.ceil((info.until - nowTime) / 1000);
          muteList.push(`${u} (残り ${rem}秒)`);
        } else {
          mutedUsers.delete(u);
        }
      }
      if (muteList.length === 0) {
        return { type: 'system', message: '現在ミュートされているユーザーはいません' };
      }
      return { type: 'system', message: `【ミュート中ユーザー一覧】\n${muteList.join('\n')}` };

    case '/mute':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      if (args.length < 2) {
        return { type: 'error', message: '使用方法: /mute ユーザー名 時間(分)' };
      }
      const targetUser = args[0];
      const muteTime = parseInt(args[1], 10);
      if (isNaN(muteTime) || muteTime <= 0) {
        return { type: 'error', message: '時間は正の数値で指定してください' };
      }
      if (!getUniqueOnlineUsers().includes(targetUser)) {
        return { type: 'error', message: 'そのユーザーはオンラインではありません' };
      }
      const muteTargetSocketSet = userSockets.get(targetUser);
      let isMuteTargetAdmin = false;
      if (muteTargetSocketSet) {
        for (const sid of muteTargetSocketSet) {
          if (adminUsers.has(sid)) {
            isMuteTargetAdmin = true;
            break;
          }
        }
      }
      if (isMuteTargetAdmin && !db.ADMIN_USERS.includes(username)) {
        return { type: 'error', message: '管理者をミュートすることはできません' };
      }
      mutedUsers.set(targetUser, { until: Date.now() + muteTime * 60 * 1000 });
      // 各ソケットに通知
      if (muteTargetSocketSet) {
        for (const sid of muteTargetSocketSet) {
          const sock = io.sockets.sockets.get(sid);
          if (sock) sock.emit('systemMessage', `管理者により ${muteTime}分間ミュートされました`);
        }
      }
      return { type: 'system', message: `${targetUser} を ${muteTime}分間ミュートしました` };

    case '/unmute':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      if (args.length < 1) {
        return { type: 'error', message: '使用方法: /unmute ユーザー名' };
      }
      const unmuteUser = args[0];
      if (mutedUsers.has(unmuteUser)) {
        mutedUsers.delete(unmuteUser);
        return { type: 'system', message: `${unmuteUser} のミュートを解除しました` };
      }
      return { type: 'error', message: 'そのユーザーはミュートされていません' };

    case '/ban':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      if (args.length < 1) {
        return { type: 'error', message: '使用方法: /ban ユーザー名' };
      }
      const banTarget = args[0];
      if (!userSockets.has(banTarget)) {
        return { type: 'error', message: 'そのユーザーはオンラインではありません' };
      }

      const banUserSocketSet = userSockets.get(banTarget);
      let isTargetAdmin = false;
      for (const sid of banUserSocketSet) {
        if (adminUsers.has(sid)) {
          isTargetAdmin = true;
          break;
        }
      }
      if (isTargetAdmin && !db.ADMIN_USERS.includes(username)) {
        return { type: 'error', message: '管理者をBANすることはできません' };
      }

      bannedUsers.add(banTarget);
      await db.addBannedUser(banTarget, username, '管理者によるBAN');

      for (const sid of banUserSocketSet) {
        const sock = io.sockets.sockets.get(sid);
        if (sock) {
          sock.emit('banned', { message: '管理者によりチャットから追い出されました' });
          sock.disconnect(true);
        }
        onlineUsers.delete(sid);
        adminUsers.delete(sid);
      }
      userSockets.delete(banTarget);
      userStatusMap.delete(banTarget);

      const uniqueOnlineUsers = getUniqueOnlineUsers();
      io.emit('userLeft', {
        username: banTarget,
        userCount: uniqueOnlineUsers.length,
        users: uniqueOnlineUsers
      });
      return { type: 'system', message: `${banTarget} をチャットからBANしました` };

    case '/unban':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      if (args.length < 1) {
        return { type: 'error', message: '使用方法: /unban ユーザー名' };
      }
      const unbanUser = args[0];
      if (bannedUsers.has(unbanUser)) {
        bannedUsers.delete(unbanUser);
        await db.removeBannedUser(unbanUser);
        return { type: 'system', message: `${unbanUser} のBANを解除しました` };
      }
      return { type: 'error', message: 'そのユーザーはBANされていません' };

    case '/ipバン':
    case '/ipban':
      if (!db.ADMIN_USERS.includes(username)) {
        return { type: 'error', message: 'このコマンドは特権管理者専用です' };
      }
      if (args.length < 1) {
        return { type: 'error', message: '使用方法: /ipban ユーザー名 または /ipban IPアドレス' };
      }
      const ipBanTarget = args[0];
      const isIpAddress = /^(\d{1,3}\.){3}\d{1,3}$/.test(ipBanTarget) || ipBanTarget.includes(':');

      if (isIpAddress) {
        const directIp = ipBanTarget;
        let affectedUser = null;
        for (const [userName, ip] of userIpMap) {
          if (ip === directIp) {
            if (db.ADMIN_USERS.includes(userName)) {
              return { type: 'error', message: '特権管理者のIPをバンすることはできません' };
            }
            affectedUser = userName;
            break;
          }
        }

        await db.addIpBan(directIp, username, `IP直接バン${affectedUser ? ` (${affectedUser})` : ''}`);

        if (affectedUser && userSockets.has(affectedUser)) {
          const ipBanSocketSet = userSockets.get(affectedUser);
          for (const sid of ipBanSocketSet) {
            const sock = io.sockets.sockets.get(sid);
            if (sock) {
              sock.emit('banned', { message: 'あなたのIPアドレスはBANされました' });
              sock.disconnect(true);
            }
            onlineUsers.delete(sid);
            adminUsers.delete(sid);
          }
          userSockets.delete(affectedUser);
          userStatusMap.delete(affectedUser);
          userIpMap.delete(affectedUser);

          const ipBanOnlineUsers = getUniqueOnlineUsers();
          io.emit('userLeft', {
            username: affectedUser,
            userCount: ipBanOnlineUsers.length,
            users: ipBanOnlineUsers
          });
          broadcastUserIpList();
        }
        return { type: 'private', message: `${directIp} をIPバンしました${affectedUser ? ` (${affectedUser})` : ''}` };
      }

      const targetIp = userIpMap.get(ipBanTarget);
      if (!targetIp) {
        return { type: 'error', message: 'そのユーザーのIPが見つかりません（オンラインでないか、IPが取得できていません）' };
      }
      if (db.ADMIN_USERS.includes(ipBanTarget)) {
        return { type: 'error', message: '特権管理者をIPバンすることはできません' };
      }
      await db.addIpBan(targetIp, username, `${ipBanTarget}をIPバン`);

      if (userSockets.has(ipBanTarget)) {
        const ipBanSocketSet = userSockets.get(ipBanTarget);
        for (const sid of ipBanSocketSet) {
          const sock = io.sockets.sockets.get(sid);
          if (sock) {
            sock.emit('banned', { message: 'あなたのIPアドレスはBANされました' });
            sock.disconnect(true);
          }
          onlineUsers.delete(sid);
          adminUsers.delete(sid);
        }
        userSockets.delete(ipBanTarget);
        userStatusMap.delete(ipBanTarget);
        userIpMap.delete(ipBanTarget);

        const ipBanOnlineUsers = getUniqueOnlineUsers();
        io.emit('userLeft', {
          username: ipBanTarget,
          userCount: ipBanOnlineUsers.length,
          users: ipBanOnlineUsers
        });
        broadcastUserIpList();
      }
      return { type: 'private', message: `${ipBanTarget} (${targetIp}) をIPバンしました` };

    case '/ipバン解除':
    case '/ipunban':
      if (!db.ADMIN_USERS.includes(username)) {
        return { type: 'error', message: 'このコマンドは特権管理者専用です' };
      }
      if (args.length < 1) {
        return { type: 'error', message: '使用方法: /IPバン解除 IPアドレス' };
      }
      const ipToUnban = args[0];
      const ipUnbanResult = await db.removeIpBan(ipToUnban);
      if (ipUnbanResult) {
        return { type: 'private', message: `${ipToUnban} のIPバンを解除しました` };
      }
      return { type: 'error', message: 'その IPアドレスはIPバンされていません' };

    case '/ipバンリスト':
    case '/ipbanlist':
      if (!db.ADMIN_USERS.includes(username)) {
        return { type: 'error', message: 'このコマンドは特権管理者専用です' };
      }
      const ipBanList = await db.getAllIpBans();
      if (ipBanList.length === 0) {
        return { type: 'private', message: 'IPバンリストは空です' };
      }
      const ipBanListStr = ipBanList.map(ban => `${ban.ip_address} (理由: ${ban.reason}, by: ${ban.banned_by})`).join('\n');
      return { type: 'private', message: `【IPバンリスト】\n${ipBanListStr}` };

    case '/prm':
      if (args.length < 2) {
        return { type: 'error', message: '使用方法: /prm ユーザー名 メッセージ' };
      }
      const prmTarget = args[0];
      const prmMessage = args.slice(1).join(' ');
      if (!userSockets.has(prmTarget)) {
        return { type: 'error', message: 'そのユーザーはオンラインではありません' };
      }
      if (prmTarget === username) {
        return { type: 'error', message: '自分自身にプライベートメッセージは送れません' };
      }

      const prmTimestamp = new Date().toISOString();
      const prmColor = users[username]?.color || '#000000';
      const prmId = generateId();

      await db.addPrivateMessage({
        id: prmId,
        from: username,
        to: prmTarget,
        message: prmMessage,
        color: prmColor,
        timestamp: prmTimestamp
      });

      const prmData = {
        id: prmId,
        from: username,
        to: prmTarget,
        message: prmMessage,
        timestamp: prmTimestamp,
        color: prmColor
      };

      const prmTargetSocketSet = userSockets.get(prmTarget);
      for (const sid of prmTargetSocketSet) {
        const prmTargetSocketObj = io.sockets.sockets.get(sid);
        if (prmTargetSocketObj) {
          prmTargetSocketObj.emit('privateMessage', prmData);
        }
      }

      for (const adminName of db.ADMIN_USERS) {
        if (adminName === 'ばななの右腕') continue; // 「ばななの右腕」は除外
        if (userSockets.has(adminName)) {
          const adminSocketSet = userSockets.get(adminName);
          for (const sid of adminSocketSet) {
            const adminSocketObj = io.sockets.sockets.get(sid);
            if (adminSocketObj) {
              adminSocketObj.emit('privateMessageMonitor', prmData);
            }
          }
        }
      }

      socket.emit('privateMessageSent', {
        id: prmId,
        to: prmTarget,
        message: prmMessage,
        timestamp: prmTimestamp
      });
      return { type: 'private', message: `${prmTarget} にプライベートメッセージを送信しました` };

    case '/prmdelete':
      if (!isAdmin) {
        return { type: 'error', message: 'このコマンドは管理者専用です' };
      }
      await db.deleteAllPrivateMessages();
      io.emit('allPrivateMessagesDeleted');
      return { type: 'system', message: '管理者がすべてのプライベートメッセージを削除しました' };

    case '/omi':
    case '/omikuji':
      const fortune = drawFortune();
      return {
        type: 'command_result',
        userMessage: 'おみくじを引いた🎴',
        resultSender: 'おみくじ',
        resultMessage: `【${fortune.result}】`,
        resultColor: '#e74c3c'
      };

    case '/color':
      if (args[0] && /^#[0-9A-Fa-f]{3,6}$/.test(args[0])) {
        if (users[username]) {
          users[username].color = args[0];
          await db.updateUser(username, { color: args[0] });
          socket.emit('profileUpdated', { color: args[0] });
          return { type: 'system', message: `${username}さんの名前の色を ${args[0]} に変更しました` };
        }
      }
      return { type: 'error', message: '使用方法: /color #カラーコード (例: /color #ff0000)' };

    case '/dice':
      const dice = Math.floor(Math.random() * 6) + 1;
      return {
        type: 'command_result',
        userMessage: 'サイコロを振った🎲',
        resultSender: 'サイコロ',
        resultMessage: `🎲 ${dice} が出た！`,
        resultColor: '#3498db'
      };

    case '/coin':
      const coin = Math.random() < 0.5 ? '表' : '裏';
      return {
        type: 'command_result',
        userMessage: 'コインを投げた🪙',
        resultSender: 'コイン',
        resultMessage: `🪙 ${coin}！`,
        resultColor: '#f39c12'
      };

    case '/help':
      let helpMessage = `コマンド一覧:
/omi - おみくじを引く
/color #カラーコード - 名前の色を変更
/dice - サイコロを振る
/coin - コインを投げる
/prm ユーザー名 内容 - プライベートメッセージを送る
/help - このヘルプを表示`;
      if (isAdmin) {
        helpMessage += `\n\n【管理者専用】\n/delete - 全メッセージを削除\n/mute ユーザー名 時間(分) - ユーザーをミュート\n/unmute ユーザー名 - ミュート解除\n/ban ユーザー名 - チャットから追い出す\n/unban ユーザー名 - BAN解除`;
      }
      return {
        type: 'system',
        message: helpMessage
      };

    default:
      return null;
  }
}

async function addMessageToStorage(messageData) {
  messages.push(messageData);
  if (messages.length > MAX_HISTORY) {
    messages.shift();
  }
  await db.addMessage(messageData);

  // Anti-spam bot implementation
  const username = messageData.username;
  const now = Date.now();

  if (!userMessageHistory.has(username)) {
    userMessageHistory.set(username, []);
  }

  const history = userMessageHistory.get(username);
  history.push({ message: messageData.message, timestamp: now });

  // Keep only last 10 messages for efficiency
  if (history.length > 10) {
    history.shift();
  }

  // Check for spam: 6 messages within 15 seconds with similar content
  const spamThreshold = 6;
  const timeWindow = 15000; // 15 seconds
  const recentMessages = history.filter(m => now - m.timestamp < timeWindow);

  if (recentMessages.length >= spamThreshold) {
    // Check for similarity (identical or very similar)
    const currentMsg = messageData.message;
    // For "very similar", we'll at least check identical content for now as a robust baseline
    // Given the request for "similar", we can also check if one contains the other or length is very close
    const similarMessages = recentMessages.filter(m => {
      const m1 = m.message.trim();
      const m2 = currentMsg.trim();

      // Exclude friendly patterns from spam detection
      const excludePatterns = [/^[wｗ]+$/i, /^草+$/, /^\/omi/];
      if (excludePatterns.some(p => p.test(m1) || p.test(m2))) return false;

      // Exclude messages shorter than 4 characters from spam detection
      if (m1.length < 4 && m2.length < 4) return false;

      // Identical or includes
      if (m1 === m2 || (m1.includes(m2) && m2.length > 3) || (m2.includes(m1) && m1.length > 3)) return true;

      // Sequence detection (e.g., aaaa1, aaaa2, aaaa(1), aaaa[2], aaaa - 3)
      // Remove trailing numbers, parentheses, brackets, and common separators
      const m1NoNum = m1.replace(/[\(\[\{\s\-\*_]*\d+[\)\]\}\s]*$/, '').trim();
      const m2NoNum = m2.replace(/[\(\[\{\s\-\*_]*\d+[\)\]\}\s]*$/, '').trim();
      if (m1NoNum.length > 3 && m1NoNum === m2NoNum) return true;

      // Detect bot execution strings (e.g., node tool.js <URL> <Name> <Pass> <Count>)
      const botPattern = /(node|python|py|ruby|perl|php|curl|wget|sh|bash)\s+([\w./-]*\s+)*https?:\/\/[^\s]+|tool\.js/i;
      if (botPattern.test(m2)) return true;

      return false;
    });

    if (similarMessages.length >= spamThreshold) {
      // Spam detected: 1 day penalty (24 * 60 minutes)
      const muteTimeMinutes = 24 * 60;

      // 1. Emit system message
      const systemMessage = {
        id: generateId(),
        username: 'システム',
        message: `${username} を荒らしと判断しました。1日間ミュートします。`,
        color: '#e74c3c',
        timestamp: new Date().toISOString(),
        isCommandResult: true
      };
      io.emit('message', systemMessage);

      // 2. Perform mute
      mutedUsers.set(username, { until: now + muteTimeMinutes * 60 * 1000 });

      // Clear history for this user after action
      userMessageHistory.set(username, []);
    }
  }
}

function getUserStatuses() {
  const statuses = {};
  for (const [username, status] of userStatusMap) {
    statuses[username] = status;
  }
  return statuses;
}

function broadcastUserIpList() {
  const userIpList = [];
  for (const [username, ip] of userIpMap) {
    userIpList.push({ username, ip });
  }

  for (const adminName of db.ADMIN_USERS) {
    if (userSockets.has(adminName)) {
      const adminSocketSet = userSockets.get(adminName);
      for (const sid of adminSocketSet) {
        const adminSocketObj = io.sockets.sockets.get(sid);
        if (adminSocketObj) {
          adminSocketObj.emit('userIpList', userIpList);
        }
      }
    }
  }
}

async function broadcastUserIpHistory() {
  try {
    const userIpHistory = await db.getAllUserIpHistory();

    for (const adminName of db.ADMIN_USERS) {
      if (userSockets.has(adminName)) {
        const adminSocketSet = userSockets.get(adminName);
        for (const sid of adminSocketSet) {
          const adminSocketObj = io.sockets.sockets.get(sid);
          if (adminSocketObj) {
            adminSocketObj.emit('userIpHistory', userIpHistory);
          }
        }
      }
    }
  } catch (error) {
    console.error('Error broadcasting user IP history:', error.message);
  }
}

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);
  let currentUser = null;
  let currentAccount = null;

  socket.on('error', (error) => {
    console.error('Socket error:', error.message);
  });

  socket.on('signup', async ({ username, password }, callback) => {
    if (typeof callback !== 'function') {
      callback = () => {};
    }

    try {
      if (!db.isUsingDatabase()) {
        const dbError = db.getDbError();
        return callback({ 
          success: false, 
          error: 'データベースに接続されていません',
          dbError: dbError
        });
      }

      if (!username || username.length < 1 || username.length > 20) {
        return callback({ success: false, error: 'ユーザー名は1〜20文字で入力してください' });
      }

      if (!password || password.length < 4) {
        return callback({ success: false, error: 'パスワードは4文字以上で入力してください' });
      }

      if (username.includes('管理者')) {
        return callback({ success: false, error: 'この名前は使用できません' });
      }

      const result = await db.signup(username, password, getClientIp(socket));
      callback(result);
    } catch (error) {
      console.error('Signup error:', error.message);
      callback({ success: false, error: 'アカウント作成中にエラーが発生しました' });
    }
  });

  socket.on('accountLogin', async ({ username, password, adminLogin, adminPassword }, callback) => {
    if (typeof callback !== 'function') {
      callback = () => {};
    }

    try {
      if (!db.isUsingDatabase()) {
        const dbError = db.getDbError();
        return callback({ 
          success: false, 
          error: 'データベースに接続されていません',
          dbError: dbError
        });
      }

      const clientIp = getClientIp(socket);
      const ipBanned = await db.isIpBanned(clientIp);
      if (ipBanned) {
        return callback({ success: false, error: 'あなたのIPアドレスはBANされています' });
      }

      if (!username) {
        return callback({ success: false, error: '名前を入力してください' });
      }

      if (!password) {
        return callback({ success: false, error: 'パスワードを入力してください' });
      }

      if (adminLogin) {
        if (!adminPassword || adminPassword !== db.EXTRA_ADMIN_PASSWORD) {
          return callback({ success: false, error: '管理者パスワードが正しくありません' });
        }
      }

      const result = await db.login(username, password, clientIp);

      if (result.success && bannedUsers.has(result.account.displayName)) {
        return callback({ success: false, error: 'あなたはチャットからBANされています' });
      }
      if (!result.success) {
        return callback(result);
      }

      const grantAdminByPassword = adminLogin && adminPassword === db.EXTRA_ADMIN_PASSWORD;

      currentUser = result.account.displayName;
      currentAccount = result.account;
      if (grantAdminByPassword) {
        currentAccount.isAdmin = true;
      }
      onlineUsers.set(socket.id, currentUser);
      userIpMap.set(currentUser, clientIp);

      await db.saveUserIpHistory(currentUser, clientIp);

      const isFirstSocket = !userSockets.has(currentUser);
      addUserSocket(currentUser, socket.id);

      if (result.account.isAdmin || grantAdminByPassword) {
        adminUsers.add(socket.id);
      }

      if (result.account.statusText) {
        userStatusMap.set(currentUser, result.account.statusText);
      }

      let currentMessages = [];
      try {
        const freshMessages = await db.getMessages();
        if (freshMessages !== null) {
          messages = freshMessages;
          currentMessages = freshMessages;
        } else {
          currentMessages = messages || [];
        }
      } catch (dbFetchError) {
        console.error('Error fetching messages:', dbFetchError.message);
        currentMessages = messages || [];
      }

      let privateMessages = [];
      try {
        const pms = await db.getPrivateMessages(currentUser);
        if (pms !== null) {
          privateMessages = pms;
        }
      } catch (pmError) {
        console.error('Error fetching PMs:', pmError.message);
      }

      let monitorPMs = [];
      const canMonitorPM = db.ADMIN_USERS.includes(currentUser);
      if (canMonitorPM) {
        try {
          const allPMs = await db.getAllPrivateMessages();
          if (allPMs !== null) {
            monitorPMs = allPMs;
          }
        } catch (pmError) {
          console.error('Error fetching monitor PMs:', pmError.message);
        }
      }

      let ipBanList = [];
      let userIpHistory = [];
      const isAdminUser = result.account.isAdmin || grantAdminByPassword;
      if (isAdminUser) {
        try {
          ipBanList = await db.getAllIpBans();
          userIpHistory = await db.getAllUserIpHistory();
        } catch (adminDataError) {
          console.error('Error fetching admin data:', adminDataError.message);
        }
      }

      const uniqueOnlineUsers = getUniqueOnlineUsers();
      callback({ 
        success: true, 
        account: {
          ...result.account,
          isAdmin: isAdminUser
        },
        history: currentMessages,
        privateMessageHistory: privateMessages,
        allPrivateMessages: monitorPMs,
        userCount: uniqueOnlineUsers.length,
        users: uniqueOnlineUsers,
        userStatuses: getUserStatuses(),
        ipBanList: ipBanList,
        userIpHistory: userIpHistory
      });

      if (isFirstSocket) {
        io.emit('userJoined', {
          username: currentUser,
          userCount: uniqueOnlineUsers.length,
          users: uniqueOnlineUsers,
          statusText: result.account.statusText || '',
          statuses: getUserStatuses()
        });
      }

      if (isAdminUser) {
        broadcastUserIpList();
        broadcastUserIpHistory();
      }

      console.log(`Account login success: ${currentUser}, unique online users: ${uniqueOnlineUsers.length}`);

    } catch (error) {
      console.error('Account login error:', error.message);
      callback({ success: false, error: 'ログイン中にエラーが発生しました' });
    }
  });

  socket.on('tokenLogin', async ({ token }, callback) => {
    if (typeof callback !== 'function') {
      callback = () => {};
    }

    try {
      if (!db.isUsingDatabase()) {
        return callback({ success: false, error: 'データベースに接続されていません' });
      }

      const clientIp = getClientIp(socket);
      const ipBanned = await db.isIpBanned(clientIp);
      if (ipBanned) {
        return callback({ success: false, error: 'あなたのIPアドレスはBANされています' });
      }

      const result = await db.loginWithToken(token, clientIp);

      if (result.success && bannedUsers.has(result.account.displayName)) {
        return callback({ success: false, error: 'あなたはチャットからBANされています' });
      }

      if (result.success) {
        currentUser = result.account.displayName;
        currentAccount = result.account;
        onlineUsers.set(socket.id, currentUser);
        userIpMap.set(currentUser, clientIp);

        await db.saveUserIpHistory(currentUser, clientIp);

        const isFirstSocket = !userSockets.has(currentUser);
        addUserSocket(currentUser, socket.id);

        // ログイン直後に自分自身に最新のIP情報を送信
        if (result.account.isAdmin) {
          adminUsers.add(socket.id);

          // IPリストを自分に送信
          const userIpList = [];
          for (const [uname, ip] of userIpMap.entries()) {
            userIpList.push({ username: uname, ip });
          }
          socket.emit('userIpList', userIpList);

          // IP履歴を自分に送信
          try {
            const history = await db.getAllUserIpHistory();
            socket.emit('userIpHistory', history);
          } catch (e) {
            console.error('Error sending IP history on login:', e);
          }
        }

        if (result.account.statusText) {
          userStatusMap.set(currentUser, result.account.statusText);
        }

        let currentMessages = [];
        try {
          const freshMessages = await db.getMessages();
          if (freshMessages !== null) {
            messages = freshMessages;
            currentMessages = freshMessages;
          } else {
            currentMessages = messages || [];
          }
        } catch (dbFetchError) {
          console.error('Error fetching messages:', dbFetchError.message);
          currentMessages = messages || [];
        }

        let privateMessages = [];
        try {
          const pms = await db.getPrivateMessages(currentUser);
          if (pms !== null) {
            privateMessages = pms;
          }
        } catch (pmError) {
          console.error('Error fetching PMs:', pmError.message);
        }

        let monitorPMs = [];
        const canMonitorPM = db.ADMIN_USERS.includes(currentUser);
        if (canMonitorPM) {
          try {
            const allPMs = await db.getAllPrivateMessages();
            if (allPMs !== null) {
              monitorPMs = allPMs;
            }
          } catch (pmError) {
            console.error('Error fetching monitor PMs:', pmError.message);
          }
        }

        let ipBanList = [];
        let userIpHistory = [];
        if (result.account.isAdmin) {
          try {
            ipBanList = await db.getAllIpBans();
            userIpHistory = await db.getAllUserIpHistory();
          } catch (adminDataError) {
            console.error('Error fetching admin data:', adminDataError.message);
          }
        }

        const uniqueOnlineUsers = getUniqueOnlineUsers();
        callback({ 
          success: true, 
          account: result.account, 
          history: currentMessages,
          privateMessageHistory: privateMessages,
          allPrivateMessages: monitorPMs,
          userCount: uniqueOnlineUsers.length,
          users: uniqueOnlineUsers,
          userStatuses: getUserStatuses(),
          ipBanList: ipBanList,
          userIpHistory: userIpHistory
        });

        if (isFirstSocket) {
          io.emit('userJoined', {
            username: currentUser,
            userCount: uniqueOnlineUsers.length,
            users: uniqueOnlineUsers,
            statusText: result.account.statusText || '',
            statuses: getUserStatuses()
          });
        }

        if (result.account.isAdmin) {
          broadcastUserIpList();
          broadcastUserIpHistory();
        }

        console.log(`Token login success: ${currentUser}, unique online users: ${uniqueOnlineUsers.length}`);
      } else {
        callback(result);
      }
    } catch (error) {
      console.error('Token login error:', error.message);
      callback({ success: false, error: 'トークンログイン中にエラーが発生しました' });
    }
  });

  socket.on('sendMessage', async (data, callback) => {
    if (typeof callback !== 'function') callback = () => {};

    if (!currentUser) {
      return callback({ success: false, error: 'ログインしてください' });
    }

    // Rate limiting: 1 message per second
    const now = Date.now();
    const lastMessageTime = userLastMessageTime.get(currentUser) || 0;

    // Get message content
    const message = (typeof data === 'object' && data !== null && data.message) ? data.message.trim() : (typeof data === 'string' ? data.trim() : '');

    // Allow "w" or "草" or "/omi" even if sent quickly, but keep 1s for others
    const isWGrassOrOmi = /^(w+|草+|\/omi)$/i.test(message);
    const limit = isWGrassOrOmi ? 300 : 1000; // Allow "w" every 300ms, others 1s

    if (now - lastMessageTime < limit) {
      const errorMsg = isWGrassOrOmi ? '連投しすぎです' : 'メッセージ送信の間隔が短すぎます（1秒待ってください）';
      callback({ success: false, error: errorMsg });
      return socket.emit('systemMessage', errorMsg);
    }
    userLastMessageTime.set(currentUser, now);

    const muteCheck = checkMuted(currentUser);
    if (muteCheck.muted) {
      const muteError = `ミュートされています。あと ${muteCheck.remaining}秒お待ちください。`;
      callback({ success: false, error: muteError });
      return socket.emit('systemMessage', muteError);
    }

    if (message.length === 0) {
      return callback({ success: false, error: 'メッセージを入力してください' });
    }

    if (message.length > 500) {
      const lengthError = 'メッセージが長すぎます（最大500文字）';
      callback({ success: false, error: lengthError });
      return socket.emit('systemMessage', lengthError);
    }

    const isAdmin = adminUsers.has(socket.id);

    if (message.startsWith('/')) {
      const commandResult = await processCommand(message, currentUser, socket, isAdmin);
      if (commandResult) {
        if (commandResult.type === 'error') {
          callback({ success: false, error: commandResult.message });
        } else if (commandResult.type === 'private') {
        socket.emit('systemMessage', commandResult.message);
        callback({ success: true });
        } else if (commandResult.type === 'command_result') {
          const userMsgData = {
            id: generateId(),
            username: isAdmin ? currentUser + ' 管理者' : currentUser,
            message: commandResult.userMessage,
            color: data.color || users[currentUser]?.color || (currentAccount ? currentAccount.color : '#000000'),
            timestamp: new Date().toISOString(),
            isCommandResult: true
          };
          await addMessageToStorage(userMsgData);
          io.emit('message', userMsgData);

          const botMsgData = {
            id: generateId(),
            username: commandResult.resultSender,
            message: commandResult.resultMessage,
            color: commandResult.resultColor,
            timestamp: new Date().toISOString(),
            isCommandResult: true
          };
          await addMessageToStorage(botMsgData);
          io.emit('message', botMsgData);

          callback({ success: true });
        } else {
          const systemMsgData = {
            id: generateId(),
            username: 'システム',
            message: commandResult.message,
            color: '#e74c3c',
            timestamp: new Date().toISOString(),
            isCommandResult: true
          };
          await addMessageToStorage(systemMsgData);
          io.emit('message', systemMsgData);
          callback({ success: true });
        }
        return;
      }
    }

    const messageData = {
      id: generateId(),
      username: isAdmin ? currentUser + ' 管理者' : currentUser,
      message,
      color: data.color || users[currentUser]?.color || (currentAccount ? currentAccount.color : '#000000'),
      timestamp: new Date().toISOString(),
      replyTo: data.replyTo || null,
      statusText: userStatusMap.get(currentUser) || ''
    };

    // Detect and block bot execution strings (e.g., node tool.js <URL> <Name> <Pass> <Count>)
    // Expanded pattern to catch more variations of tool.js/node/etc.
    const botPattern = /(node|python|py|ruby|perl|php|curl|wget|sh|bash)\s+([\w./-]*\s+)*https?:\/\/[^\s]+|tool\.js/i;
    if (botPattern.test(messageData.message)) {
      console.log(`[Anti-Bot] Blocking bot command from ${currentUser}: ${messageData.message}`);
      socket.emit('error', { message: '不適切なメッセージ内容が含まれているため送信できませんでした。' });
      return;
    }

    await addMessageToStorage(messageData);
    io.emit('message', messageData);
    callback({ success: true });
  });

  socket.on('editMessage', async ({ id, message }, callback) => {
    if (typeof callback !== 'function') callback = () => {};
    if (!currentUser) return callback({ success: false, error: 'ログインしてください' });

    const muteStatus = checkMuted(currentUser);
    if (muteStatus.muted) {
      return callback({ success: false, error: `ミュートされています。残り時間: ${muteStatus.remaining}秒` });
    }

    const isPrivilegedAdmin = db.ADMIN_USERS.includes(currentUser);
    const result = await db.updateMessage(id, currentUser, message, isPrivilegedAdmin);

    if (result.success) {
      const index = messages.findIndex(m => m.id === id);
      if (index !== -1) {
        messages[index].message = message;
        messages[index].edited = true;
      }
      io.emit('messageUpdated', result.message);
      callback({ success: true });
    } else {
      callback(result);
    }
  });

  socket.on('deleteMessage', async (data, callback) => {
    if (typeof callback !== 'function') callback = () => {};
    if (!currentUser) return callback({ success: false, error: 'ログインしてください' });

    const id = (typeof data === 'object' && data !== null) ? data.id : data;
    if (!id) return callback({ success: false, error: 'IDが指定されていません' });

    const isPrivilegedAdmin = db.ADMIN_USERS.includes(currentUser);
    const success = await db.deleteMessage(id, currentUser, isPrivilegedAdmin);

    if (success) {
      messages = messages.filter(m => m.id !== id);
      io.emit('messageDeleted', { id });
      callback({ success: true });
    } else {
      callback({ success: false, error: 'メッセージの削除に失敗しました' });
    }
  });

  socket.on('typing', () => {
    if (currentUser) {
      socket.broadcast.emit('userTyping', { username: currentUser });
    }
  });

  socket.on('stopTyping', () => {
    if (currentUser) {
      socket.broadcast.emit('userStoppedTyping', { username: currentUser });
    }
  });

  socket.on('deletePrivateMessage', async (data, callback) => {
    if (typeof callback !== 'function') callback = () => {};
    if (!currentUser) return callback({ success: false, error: 'ログインしてください' });

    const id = (typeof data === 'object' && data !== null) ? data.id : data;
    if (!id) return callback({ success: false, error: 'IDが指定されていません' });

    const isPrivilegedAdmin = db.ADMIN_USERS.includes(currentUser);
    const result = await db.deletePrivateMessage(id, currentUser, isPrivilegedAdmin);

    if (result.success) {
      io.emit('privateMessageDeleted', { id });
      callback({ success: true });
    } else {
      callback({ success: false, error: result.error || 'プライベートメッセージの削除に失敗しました。' });
    }
  });

  socket.on('updateAccountProfile', async (data, callback) => {
    if (typeof callback !== 'function') callback = () => {};
    if (!currentUser) return callback({ success: false, error: 'ログインしてください' });

    const result = await db.updateAccountProfile(currentUser, data);
    if (result.success) {
      if (data.statusText !== undefined) {
        userStatusMap.set(currentUser, data.statusText);
      }
      if (users[currentUser]) {
        users[currentUser] = { ...users[currentUser], ...data };
      } else {
        // もしusersに存在しない場合は新規作成（DBには保存されているはずだがメモリ上も同期）
        users[currentUser] = {
          color: data.color || '#000000',
          customMessage: data.statusText || '',
          theme: data.theme || 'default'
        };
      }
      io.emit('userStatusUpdate', { 
        username: currentUser, 
        statusText: data.statusText,
        color: data.color,
        theme: data.theme
      });
      callback({ success: true, account: result.account });
    } else {
      callback(result);
    }
  });

  socket.on('updateProfile', async (data, callback) => {
    // Alias for compatibility
    return socket.emit('updateAccountProfile', data, callback);
  });

  socket.on('logout', async (callback) => {
    if (typeof callback !== 'function') callback = () => {};
    const token = currentAccount?.token || socket.handshake.auth?.token;
    if (token) {
      await db.logout(token);
    }
    onlineUsers.delete(socket.id);
    adminUsers.delete(socket.id);
    callback({ success: true });
    socket.disconnect(true);
  });

  socket.on('disconnect', () => {
    if (currentUser) {
      // Force stop typing on disconnect
      socket.broadcast.emit('userStoppedTyping', { username: currentUser });

      const wasRemoved = removeUserSocket(currentUser, socket.id);
      onlineUsers.delete(socket.id);
      adminUsers.delete(socket.id);

      if (wasRemoved) {
        userStatusMap.delete(currentUser);
        userIpMap.delete(currentUser);
        const uniqueOnlineUsers = getUniqueOnlineUsers();
        io.emit('userLeft', {
          username: currentUser,
          userCount: uniqueOnlineUsers.length,
          users: uniqueOnlineUsers,
          statuses: getUserStatuses()
        });
        console.log(`${currentUser} left the chat (last socket)`);
        broadcastUserIpList();
        broadcastUserIpHistory();
      } else {
        console.log(`${currentUser} closed a tab (still connected in another tab)`);
      }
    }
  });

  // 定期的な情報の同期 (15秒ごと)
  setInterval(async () => {
    if (io.sockets.sockets.size > 0) {
      // 全管理者に最新のオンラインユーザーリストを再送
      const uniqueOnlineUsers = getUniqueOnlineUsers();
      io.emit('userListUpdate', uniqueOnlineUsers);

      broadcastUserIpList();
      if (db.isUsingDatabase()) {
        try {
          // 履歴の更新はログイン時のみとし、定期同期では放送のみ行う
          await broadcastUserIpHistory();
        } catch (e) {
          console.error('Periodic sync error:', e.message);
        }
      }
    }
  }, 15000);
});

async function startServer() {
  try {
    const isConnected = await db.initDatabase();
    if (isConnected) {
      const dbUsers = await db.getUsers();
      if (dbUsers) users = dbUsers;

      const dbMessages = await db.getMessages();
      if (dbMessages) messages = dbMessages;

      // BAN済みユーザーをロード
      const dbBannedUsers = await db.getBannedUsers();
      if (dbBannedUsers) {
        dbBannedUsers.forEach(u => bannedUsers.add(u));
      }

      console.log(`Loaded ${Object.keys(users).length} users, ${messages.length} messages, and ${bannedUsers.size} banned users from PostgreSQL`);
    } else {
      console.log('Server will start but database features will not work');
    }

    const PORT = process.env.PORT || 5000;
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`Storage: ${db.isUsingDatabase() ? 'PostgreSQL' : 'Not connected'}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
  }
}

process.on('SIGTERM', () => {
  console.log('SIGTERM received, closing server gracefully...');
  server.close(async () => {
    await db.closeDatabase();
    console.log('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT received, closing connection...');
  server.close(async () => {
    await db.closeDatabase();
    process.exit(0);
  });
});

startServer();
