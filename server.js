const express = require('express');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const app = express();

// ============ CONFIGURAÇÕES ============
const EVOLUTION_BASE_URL = process.env.EVOLUTION_BASE_URL || 'https://evo.flowzap.fun';
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY || '';
const PIX_TIMEOUT = 7 * 60 * 1000;
const PORT = process.env.PORT || 3000;
const MESSAGE_BLOCK_TIME = 60000;
const JWT_SECRET = process.env.JWT_SECRET || 'orion-secret-2025';
const ADMIN_LOGIN = process.env.ADMIN_LOGIN || 'Danilo';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '$Senha123';
const CLEANUP_DAYS = parseInt(process.env.CLEANUP_DAYS || '7');

// ============ DATABASE ============
const db = require('./database');
db.initDatabase();

// ============ ESTADO EM MEMÓRIA ============
let conversations = new Map();       // phoneKey -> conv
let phoneIndex = new Map();          // variação -> phoneKey
let phoneVariations = new Map();
let lidMapping = new Map();
let phoneToLid = new Map();
let stickyInstances = new Map();     // phoneKey -> instanceName (FIXO até cair)
let pixTimeouts = new Map();
let webhookLocks = new Map();
let logs = [];
let sentMessagesHash = new Map();
let messageBlockTimers = new Map();
let lastSuccessfulInstanceIndex = -1;
let activeInstancesCache = [];       // cache em memória sincronizado com DB
let sseClients = [];

// ============ SSE ============
function sendSSE(event, data) {
    const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients = sseClients.filter(res => {
        try { res.write(msg); return true; } catch { return false; }
    });
}

// ============ INSTÂNCIAS ============
function refreshInstanceCache() {
    const dbInstances = db.getInstances();
    activeInstancesCache = dbInstances
        .filter(i => !i.paused && i.connected)
        .map(i => i.name);
}

function getActiveInstances() {
    return activeInstancesCache;
}

// Configura instâncias iniciais
const CONFIGURED_INSTANCES = (process.env.INSTANCES || 'F01').split(',').map(s => s.trim());
for (const inst of CONFIGURED_INSTANCES) {
    db.ensureInstance(inst);
}
refreshInstanceCache();

// ============ PRODUTO HELPER ============
function identifyProduct(offerId) {
    const product = db.getProductByOfferId(offerId);
    return product ? product : null;
}

// ============ VARIÁVEIS DINÂMICAS ============
function replaceVariables(text, conversation) {
    if (!text || !conversation) return text;
    let result = text;
    if (conversation.pixCode) {
        result = result.replace(/\{PIX_LINK\}/g, conversation.pixCode);
        result = result.replace(/\{PIX_GERADO\}/g, conversation.pixCode);
        result = result.replace(/\{PIX_CODE\}/g, conversation.pixCode);
    }
    if (conversation.customerName) {
        result = result.replace(/\{NOME_CLIENTE\}/g, conversation.customerName);
        result = result.replace(/\{NOME\}/g, conversation.customerName);
    }
    if (conversation.amount) result = result.replace(/\{VALOR\}/g, conversation.amount);
    if (conversation.productName) result = result.replace(/\{PRODUTO\}/g, conversation.productName);
    return result;
}

// ============ ANTI-DUPLICAÇÃO (igual ao sistema antigo) ============
function generateMessageHash(phoneKey, step, conversation) {
    const baseContent = step.text || step.mediaUrl || '';
    const data = `${phoneKey}|${step.type}|${baseContent}|${step.id}`;
    return crypto.createHash('md5').update(data).digest('hex');
}

function isMessageBlocked(phoneKey, step, conversation) {
    const hash = generateMessageHash(phoneKey, step, conversation);
    const lastSent = messageBlockTimers.get(hash);
    if (lastSent) {
        const timeSince = Date.now() - lastSent;
        if (timeSince < MESSAGE_BLOCK_TIME) {
            addLog('MESSAGE_BLOCKED', `🚫 Bloqueada há ${Math.round(timeSince/1000)}s`, { phoneKey, hash: hash.substring(0,8) });
            return true;
        }
    }
    return false;
}

function registerSentMessage(phoneKey, step, conversation) {
    const hash = generateMessageHash(phoneKey, step, conversation);
    messageBlockTimers.set(hash, Date.now());
    if (!sentMessagesHash.has(phoneKey)) sentMessagesHash.set(phoneKey, new Set());
    sentMessagesHash.get(phoneKey).add(hash);
}

setInterval(() => {
    const now = Date.now();
    let cleaned = 0;
    for (const [hash, ts] of messageBlockTimers.entries()) {
        if (now - ts > MESSAGE_BLOCK_TIME) { messageBlockTimers.delete(hash); cleaned++; }
    }
    if (cleaned > 0) console.log(`🧹 ${cleaned} bloqueios expirados removidos`);
}, 120000);

// ============ NORMALIZAÇÃO DE TELEFONE (mantida igual ao sistema antigo) ============
function normalizePhoneKey(phone) {
    if (!phone) return null;
    let cleaned = String(phone).split('@')[0].replace(/\D/g, '');
    if (cleaned.length < 8) { console.log('❌ Telefone muito curto:', phone); return null; }
    return cleaned.slice(-8);
}

function generateAllPhoneVariations(fullPhone) {
    const cleaned = String(fullPhone).split('@')[0].replace(/\D/g, '');
    if (cleaned.length < 8) return [];
    const variations = new Set();
    variations.add(cleaned);
    if (!cleaned.startsWith('55')) variations.add('55' + cleaned);
    if (cleaned.startsWith('55') && cleaned.length > 2) variations.add(cleaned.substring(2));
    for (let i = 8; i <= Math.min(13, cleaned.length); i++) {
        const lastN = cleaned.slice(-i);
        variations.add(lastN);
        if (!lastN.startsWith('55')) variations.add('55' + lastN);
    }
    if (cleaned.length >= 11) {
        const ddd = cleaned.slice(-11, -9);
        const numero = cleaned.slice(-9);
        if (numero.length === 9 && numero[0] === '9') {
            const semNove = ddd + numero.substring(1);
            variations.add(semNove); variations.add('55' + semNove);
            for (let i = 8; i <= semNove.length; i++) variations.add(semNove.slice(-i));
        }
        if (numero.length === 8 || (numero.length === 9 && numero[0] !== '9')) {
            const comNove = ddd + '9' + numero;
            variations.add(comNove); variations.add('55' + comNove);
            for (let i = 8; i <= comNove.length; i++) variations.add(comNove.slice(-i));
        }
    }
    if (cleaned.length === 12 && cleaned.startsWith('55')) {
        const ddd = cleaned.substring(2, 4);
        const numero = cleaned.substring(4);
        const comNove = '55' + ddd + '9' + numero;
        variations.add(comNove); variations.add(comNove.substring(2));
    }
    if (cleaned.length === 13 && cleaned.startsWith('55')) {
        const numeroSemNove = cleaned.substring(0, 4) + cleaned.substring(5);
        variations.add(numeroSemNove); variations.add(numeroSemNove.substring(2));
    }
    return Array.from(variations).filter(v => v && v.length >= 8);
}

function registerPhoneUniversal(fullPhone, phoneKey) {
    if (!phoneKey || phoneKey.length !== 8) return;
    const variations = generateAllPhoneVariations(fullPhone);
    const suffixes = ['@s.whatsapp.net', '@lid', '@g.us', ''];
    variations.forEach(v => {
        phoneIndex.set(v, phoneKey);
        phoneVariations.set(v, phoneKey);
        suffixes.forEach(s => {
            phoneIndex.set(v + s, phoneKey);
            phoneVariations.set(v + s, phoneKey);
        });
    });
}

function registerLidMapping(lidJid, phoneKey, realNumber) {
    if (!lidJid || !phoneKey) return;
    lidMapping.set(lidJid, phoneKey);
    phoneToLid.set(phoneKey, lidJid);
    const lidCleaned = lidJid.split('@')[0].replace(/\D/g, '');
    if (lidCleaned) { lidMapping.set(lidCleaned, phoneKey); lidMapping.set(lidCleaned + '@lid', phoneKey); }
    addLog('LID_MAPPING', `🆔 @lid mapeado`, { lid: lidJid, phoneKey });
}

function findConversationUniversal(phone) {
    const phoneKey = normalizePhoneKey(phone);
    if (!phoneKey) return null;
    console.log('🔍 Buscando:', phoneKey);

    // Nível 1: busca direta
    let conv = conversations.get(phoneKey);
    if (conv) { registerPhoneUniversal(phone, phoneKey); return conv; }

    // Nível 2: variações indexadas
    const variations = generateAllPhoneVariations(phone);
    for (const v of variations) {
        const k = phoneIndex.get(v) || phoneVariations.get(v);
        if (k) { conv = conversations.get(k); if (conv) { registerPhoneUniversal(phone, k); return conv; } }
    }

    // Nível 3: sufixos WhatsApp
    const suffixes = ['@s.whatsapp.net', '@lid', '@g.us', ''];
    for (const s of suffixes) {
        for (const v of variations) {
            const k = phoneIndex.get(v + s) || phoneVariations.get(v + s);
            if (k) { conv = conversations.get(k); if (conv) { registerPhoneUniversal(phone, k); return conv; } }
        }
    }

    // Nível 4: busca exaustiva
    for (const [key, c] of conversations.entries()) {
        if (key === phoneKey || key.slice(-7) === phoneKey.slice(-7)) { registerPhoneUniversal(phone, key); return c; }
        if (c.remoteJid) {
            const ck = normalizePhoneKey(c.remoteJid);
            if (ck === phoneKey || (ck && ck.slice(-7) === phoneKey.slice(-7))) { registerPhoneUniversal(phone, key); return c; }
        }
    }

    // Nível 5: mapeamento @lid
    if (String(phone).includes('@lid')) {
        const mk = lidMapping.get(phone) || lidMapping.get(String(phone).split('@')[0]);
        if (mk) { conv = conversations.get(mk); if (conv) return conv; }
    }

    addLog('CONV_NOT_FOUND', `❌ Não encontrado após 5 níveis`, { phoneKey, total: conversations.size });
    return null;
}

// ============ LOCK (igual ao sistema antigo) ============
async function acquireWebhookLock(phoneKey, timeout = 10000) {
    const start = Date.now();
    while (webhookLocks.get(phoneKey)) {
        if (Date.now() - start > timeout) { addLog('LOCK_TIMEOUT', `Timeout lock para ${phoneKey}`); return false; }
        await new Promise(r => setTimeout(r, 100));
    }
    webhookLocks.set(phoneKey, true);
    return true;
}
function releaseWebhookLock(phoneKey) { webhookLocks.delete(phoneKey); }

// ============ HELPERS ============
function phoneToRemoteJid(phone) {
    let cleaned = phone.replace(/\D/g, '');
    if (!cleaned.startsWith('55')) cleaned = '55' + cleaned;
    if (cleaned.length === 12) { const ddd = cleaned.substring(2, 4); cleaned = '55' + ddd + '9' + cleaned.substring(4); }
    return cleaned + '@s.whatsapp.net';
}

function extractMessageText(message) {
    if (!message) return '';
    if (message.conversation) return message.conversation;
    if (message.extendedTextMessage?.text) return message.extendedTextMessage.text;
    if (message.imageMessage) return message.imageMessage.caption || '[IMAGEM]';
    if (message.videoMessage) return message.videoMessage.caption || '[VÍDEO]';
    if (message.audioMessage) return '[ÁUDIO]';
    if (message.documentMessage) return '[DOCUMENTO]';
    if (message.stickerMessage) return '[FIGURINHA]';
    if (message.locationMessage) return '[LOCALIZAÇÃO]';
    if (message.contactMessage) return '[CONTATO]';
    if (message.viewOnceMessage) {
        if (message.viewOnceMessage.message?.imageMessage) return '[IMAGEM ÚNICA]';
        if (message.viewOnceMessage.message?.videoMessage) return '[VÍDEO ÚNICO]';
    }
    return '[MENSAGEM]';
}

function addLog(type, message, data = null) {
    const log = { id: Date.now() + Math.random(), timestamp: new Date(), type, message, data };
    logs.unshift(log);
    if (logs.length > 500) logs = logs.slice(0, 500);
    console.log(`[${log.timestamp.toISOString()}] ${type}: ${message}`);
    sendSSE('log', { type, message, timestamp: log.timestamp });
}

// ============ SINCRONIZAÇÃO MEMÓRIA → DB ============
function convToDb(phoneKey, conv) {
    db.saveConversation({
        phone_key: phoneKey,
        remote_jid: conv.remoteJid,
        funnel_id: conv.funnelId,
        step_index: conv.stepIndex,
        order_code: conv.orderCode,
        customer_name: conv.customerName,
        product_id: conv.productId,
        product_name: conv.productName,
        order_bumps: conv.orderBumps || [],
        amount: conv.amount,
        pix_code: conv.pixCode,
        payment_method: conv.paymentMethod || 'PIX',
        waiting_for_response: conv.waiting_for_response,
        pix_waiting: conv.pixWaiting,
        sticky_instance: stickyInstances.get(phoneKey),
        canceled: conv.canceled,
        completed: conv.completed,
        has_error: conv.hasError,
        transferred_from_pix: conv.transferredFromPix,
        paused: conv.paused,
        created_at: conv.createdAt ? conv.createdAt.toISOString() : new Date().toISOString(),
        last_message_at: conv.lastSystemMessage ? conv.lastSystemMessage.toISOString() : null,
        last_reply_at: conv.lastReply ? conv.lastReply.toISOString() : null,
        completed_at: conv.completedAt ? conv.completedAt.toISOString() : null,
        canceled_at: conv.canceledAt ? conv.canceledAt.toISOString() : null,
    });
}

// Salva no DB a cada 15s
setInterval(() => {
    for (const [phoneKey, conv] of conversations.entries()) convToDb(phoneKey, conv);
}, 15000);

// Limpeza automática
setInterval(() => {
    const deleted = db.deleteOldConversations(CLEANUP_DAYS);
    if (deleted > 0) {
        addLog('CLEANUP', `🧹 ${deleted} conversas removidas (>${CLEANUP_DAYS}d)`);
        for (const [phoneKey, conv] of conversations.entries()) {
            if ((conv.completed || conv.canceled) && conv.createdAt) {
                const ageDays = (Date.now() - conv.createdAt.getTime()) / 86400000;
                if (ageDays > CLEANUP_DAYS) {
                    conversations.delete(phoneKey);
                    stickyInstances.delete(phoneKey);
                }
            }
        }
    }
}, 6 * 60 * 60 * 1000);

// ============ EVOLUTION API ============
async function sendToEvolution(instanceName, endpoint, payload) {
    const url = `${EVOLUTION_BASE_URL}${endpoint}/${instanceName}`;
    try {
        const response = await axios.post(url, payload, {
            headers: { 'Content-Type': 'application/json', 'apikey': EVOLUTION_API_KEY },
            timeout: 15000
        });
        addLog('EVO_OK', `✅ ${instanceName} respondeu`, { status: response.status });
        return { ok: true, data: response.data };
    } catch (error) {
        addLog('EVO_ERR', `❌ Erro em ${instanceName}`, {
            status: error.response?.status,
            error: error.response?.data || error.message,
            code: error.code
        });
        return { ok: false, error: error.response?.data || error.message, status: error.response?.status };
    }
}

async function checkInstanceConnected(instanceName) {
    try {
        const response = await axios.get(`${EVOLUTION_BASE_URL}/instance/connectionState/${instanceName}`, {
            headers: { 'apikey': EVOLUTION_API_KEY }, timeout: 5000
        });
        return response.data?.instance?.state === 'open';
    } catch { return false; }
}

// Ativa "digitando" na instância — máx 25s (limitação do WhatsApp)
async function sendPresence(remoteJid, instanceName, seconds) {
    if (!instanceName) return;
    try {
        const cappedMs = Math.min(seconds * 1000, 25000);
        await sendToEvolution(instanceName, '/chat/sendPresence', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            options: { presence: 'composing', delay: cappedMs }
        });
    } catch {}
}

async function sendText(remoteJid, text, instanceName) {
    return sendToEvolution(instanceName, '/message/sendText', {
        number: remoteJid.replace('@s.whatsapp.net', ''), text
    });
}

async function sendImage(remoteJid, imageUrl, caption, instanceName) {
    return sendToEvolution(instanceName, '/message/sendMedia', {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        mediatype: 'image', media: imageUrl, caption: caption || ''
    });
}

async function sendVideo(remoteJid, videoUrl, caption, instanceName) {
    return sendToEvolution(instanceName, '/message/sendMedia', {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        mediatype: 'video', media: videoUrl, caption: caption || ''
    });
}

async function sendAudio(remoteJid, audioUrl, instanceName) {
    try {
        addLog('AUDIO_DL', `⬇️ Baixando áudio`, { url: audioUrl.substring(0, 60) });
        const audioResponse = await axios.get(audioUrl, {
            responseType: 'arraybuffer', timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const base64Audio = `data:audio/mpeg;base64,${Buffer.from(audioResponse.data).toString('base64')}`;
        addLog('AUDIO_CONV', `✅ Áudio convertido (${Math.round(base64Audio.length / 1024)}KB)`);
        const result = await sendToEvolution(instanceName, '/message/sendWhatsAppAudio', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            audio: base64Audio, delay: 1200, encoding: true
        });
        if (result.ok) return result;
        addLog('AUDIO_RETRY', `⚠️ Tentando formato alternativo`);
        return sendToEvolution(instanceName, '/message/sendMedia', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            mediatype: 'audio', media: base64Audio, mimetype: 'audio/mpeg'
        });
    } catch (error) {
        addLog('AUDIO_ERR', `❌ Erro no áudio: ${error.message}`);
        return sendToEvolution(instanceName, '/message/sendWhatsAppAudio', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            audio: audioUrl, delay: 1200
        });
    }
}

async function sendViewOnce(remoteJid, mediaUrl, mediaType, instanceName) {
    const number = remoteJid.replace('@s.whatsapp.net', '');
    
    // Método 1: endpoint específico sendWhatsAppAudio style com viewOnce
    addLog('VIEWONCE_TRY1', `📤 Tentando view once método 1`);
    const result1 = await sendToEvolution(instanceName, '/message/sendMedia', {
        number,
        mediatype: mediaType,
        media: mediaUrl,
        fileName: mediaType === 'image' ? 'image.jpg' : 'video.mp4',
        options: { delay: 1000, presence: 'composing' },
        viewOnce: true,
        isViewOnce: true
    });
    if (result1.ok) { addLog('VIEWONCE_OK', `✅ View once enviado (método 1)`); return result1; }

    // Método 2: estrutura de mensagem WhatsApp nativa
    addLog('VIEWONCE_TRY2', `⚠️ Tentando view once método 2`);
    try {
        const mediaResponse = await axios.get(mediaUrl, {
            responseType: 'arraybuffer', timeout: 30000,
            headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': '*/*' }
        });
        const mimetype = mediaType === 'image' ? 'image/jpeg' : 'video/mp4';
        const base64Data = Buffer.from(mediaResponse.data).toString('base64');
        
        // Tenta via sendMessage com estrutura nativa do WhatsApp
        const result2 = await sendToEvolution(instanceName, '/message/sendMessage', {
            number,
            options: { delay: 1000, presence: 'composing' },
            mediaMessage: {
                mediatype: mediaType,
                media: `data:${mimetype};base64,${base64Data}`,
                fileName: mediaType === 'image' ? 'photo.jpg' : 'video.mp4',
                gifPlayback: false
            },
            viewOnceMessage: {
                key: { fromMe: true, remoteJid: remoteJid },
                message: {
                    viewOnceMessage: {
                        message: mediaType === 'image' 
                            ? { imageMessage: { url: mediaUrl, mimetype, viewOnce: true } }
                            : { videoMessage: { url: mediaUrl, mimetype, viewOnce: true } }
                    }
                }
            }
        });
        if (result2.ok) { addLog('VIEWONCE_OK2', `✅ View once enviado (método 2)`); return result2; }

        // Método 3: base64 direto com flag viewOnce
        addLog('VIEWONCE_TRY3', `⚠️ Tentando view once método 3 (base64)`);
        const result3 = await sendToEvolution(instanceName, '/message/sendMedia', {
            number,
            mediatype: mediaType,
            media: `data:${mimetype};base64,${base64Data}`,
            mimetype,
            viewOnce: true,
            isViewOnce: true,
            options: { delay: 1000 }
        });
        if (result3.ok) { addLog('VIEWONCE_OK3', `✅ View once enviado (método 3)`); return result3; }
        
        addLog('VIEWONCE_ERR', `❌ Todos métodos view once falharam, enviando mídia normal`);
        // Fallback final: envia como mídia normal
        return sendToEvolution(instanceName, '/message/sendMedia', {
            number, mediatype: mediaType, media: mediaUrl
        });
    } catch (error) {
        addLog('VIEWONCE_ERR', `❌ Erro view once: ${error.message}`);
        // Fallback: envia como mídia normal
        return sendToEvolution(instanceName, '/message/sendMedia', {
            number, mediatype: mediaType, media: mediaUrl
        });
    }
}

// ============ DISTRIBUIÇÃO INTELIGENTE DE INSTÂNCIAS ============
// Lógica melhorada: round-robin ponderado por volume de mensagens recentes
function selectNextInstance(isFirstMessage) {
    const active = getActiveInstances();
    if (active.length === 0) return null;
    if (active.length === 1) return active[0];

    if (isFirstMessage) {
        // Pega estatísticas de hoje para distribuição justa
        const today = new Date().toISOString().split('T')[0];
        const stats = db.getInstanceStats(1);
        const todayStats = {};
        for (const inst of active) todayStats[inst] = 0;
        for (const s of stats) {
            if (s.date === today && todayStats[s.instance] !== undefined) {
                todayStats[s.instance] = s.messages_sent;
            }
        }
        // Escolhe a instância com MENOS mensagens hoje
        const sorted = active.slice().sort((a, b) => todayStats[a] - todayStats[b]);
        return sorted[0];
    }
    return null; // se não é primeira mensagem, usa sticky
}

// ============ ENVIO COM FALLBACK MELHORADO ============
async function sendWithFallback(phoneKey, remoteJid, step, conversation, isFirstMessage = false) {
    // Anti-duplicação (igual ao sistema antigo)
    if (isMessageBlocked(phoneKey, step, conversation)) {
        addLog('SEND_BLOCKED', `🚫 Mensagem duplicada bloqueada`, { phoneKey, stepId: step.id });
        return { success: false, error: 'MESSAGE_ALREADY_SENT', blocked: true };
    }

    const finalText = replaceVariables(step.text, conversation);
    const finalMediaUrl = replaceVariables(step.mediaUrl, conversation);

    const activeInstances = getActiveInstances();
    if (activeInstances.length === 0) {
        addLog('NO_INSTANCES', '⚠️ Nenhuma instância ativa!');
        return { success: false, error: 'NO_ACTIVE_INSTANCES' };
    }

    // STICKY: se já tem instância fixada E ela está ativa, usa ela primeiro
    let instancesToTry;
    const stickyInstance = stickyInstances.get(phoneKey);

    if (!isFirstMessage && stickyInstance && activeInstances.includes(stickyInstance)) {
        // Lead existente com instância ativa → sticky primeiro
        instancesToTry = [stickyInstance, ...activeInstances.filter(i => i !== stickyInstance)];
        addLog('STICKY_USE', `📌 Usando instância fixa ${stickyInstance}`, { phoneKey });
    } else if (isFirstMessage) {
        // Primeiro contato → distribuição inteligente por carga
        const preferred = selectNextInstance(true);
        if (preferred) {
            instancesToTry = [preferred, ...activeInstances.filter(i => i !== preferred)];
        } else {
            instancesToTry = [...activeInstances];
        }
        addLog('INSTANCE_SELECT', `🎯 Selecionada ${instancesToTry[0]} para novo lead`, { phoneKey });
    } else {
        // Sticky caiu ou não existe → tenta qualquer ativa
        instancesToTry = [...activeInstances];
        if (stickyInstance) addLog('STICKY_FALLBACK', `⚠️ Sticky ${stickyInstance} indisponível, usando fallback`, { phoneKey });
    }

    let lastError = null;

    for (const instanceName of instancesToTry) {
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                let result;
                if (step.type === 'text') result = await sendText(remoteJid, finalText, instanceName);
                else if (step.type === 'image') result = await sendImage(remoteJid, finalMediaUrl, '', instanceName);
                else if (step.type === 'image+text') result = await sendImage(remoteJid, finalMediaUrl, finalText, instanceName);
                else if (step.type === 'video') result = await sendVideo(remoteJid, finalMediaUrl, '', instanceName);
                else if (step.type === 'video+text') result = await sendVideo(remoteJid, finalMediaUrl, finalText, instanceName);
                else if (step.type === 'audio') result = await sendAudio(remoteJid, finalMediaUrl, instanceName);
                else if (step.type === 'viewonce_image') result = await sendViewOnce(remoteJid, finalMediaUrl, 'image', instanceName);
                else if (step.type === 'viewonce_video') result = await sendViewOnce(remoteJid, finalMediaUrl, 'video', instanceName);
                else result = { ok: true };

                if (result && result.ok) {
                    registerSentMessage(phoneKey, step, conversation);
                    // Fixa a instância para este lead
                    const oldSticky = stickyInstances.get(phoneKey);
                    if (oldSticky !== instanceName) {
                        stickyInstances.set(phoneKey, instanceName);
                        if (oldSticky) addLog('STICKY_CHANGED', `🔄 Instância trocada ${oldSticky}→${instanceName} (fallback)`, { phoneKey });
                        else addLog('STICKY_SET', `📌 Instância fixada: ${instanceName}`, { phoneKey });
                    }
                    db.updateInstanceStats(instanceName, 1);
                    db.logMessage(phoneKey, 'out', finalText || finalMediaUrl, instanceName, step.id);
                    addLog('SEND_OK', `✅ Enviado via ${instanceName} (tentativa ${attempt})`, { phoneKey, type: step.type });
                    sendSSE('message_sent', { phoneKey, instance: instanceName, stepType: step.type });
                    return { success: true, instanceName };
                }
                lastError = result?.error;
                if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
            } catch (e) {
                lastError = e.message;
                if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
            }
        }
    }

    addLog('SEND_FAILED', `❌ Falha total para ${phoneKey}`, { lastError });
    const conv = conversations.get(phoneKey);
    if (conv) { conv.hasError = true; conversations.set(phoneKey, conv); }
    return { success: false, error: lastError };
}

// ============ ORQUESTRAÇÃO ============
async function createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, pixCode, orderBumps, paymentMethod) {
    console.log('🔴 createPixWaiting:', phoneKey);
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) {
        addLog('PIX_BLOCKED', `🚫 Conversa já existe`, { phoneKey, orderCode });
        return;
    }

    const conv = {
        phoneKey, remoteJid,
        funnelId: productId + '_PIX',
        stepIndex: -1, orderCode, customerName,
        productId, productName, orderBumps: orderBumps || [],
        amount, pixCode, paymentMethod: paymentMethod || 'PIX',
        waiting_for_response: false, pixWaiting: true,
        createdAt: new Date(), lastSystemMessage: null, lastReply: null,
        canceled: false, completed: false, paused: false
    };
    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);

    db.recordEvent('PIX_GENERATED', {
        phone_key: phoneKey, product_id: productId, product_name: productName,
        amount: parseFloat((amount || '0').replace(/[^0-9,.]/g, '').replace(',', '.')),
        payment_method: 'PIX', order_code: orderCode, order_bumps: orderBumps
    });

    sendSSE('pix_generated', { phoneKey, customerName, productName, amount, orderCode });
    addLog('PIX_WAITING', `⏳ PIX aguardando (7min) para ${phoneKey}`, { orderCode, productId });

    const timeout = setTimeout(async () => {
        const c = conversations.get(phoneKey);
        if (c && c.orderCode === orderCode && !c.canceled && c.pixWaiting) {
            addLog('PIX_TIMEOUT', `⏰ 7 minutos — iniciando funil para ${phoneKey}`, { orderCode });
            c.pixWaiting = false; c.stepIndex = 0;
            conversations.set(phoneKey, c);
            await sendStep(phoneKey);
        }
        pixTimeouts.delete(phoneKey);
    }, PIX_TIMEOUT);

    pixTimeouts.set(phoneKey, { timeout, orderCode, createdAt: new Date() });
}

async function transferPixToApproved(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, orderBumps, paymentMethod) {
    console.log('🟢 transferPixToApproved:', phoneKey);
    const pixConv = conversations.get(phoneKey);
    const pixCode = pixConv ? pixConv.pixCode : null;
    // Mantém a instância sticky do funil PIX no funil APROVADA
    const existingSticky = stickyInstances.get(phoneKey);

    if (pixConv) {
        pixConv.canceled = true; pixConv.canceledAt = new Date();
        pixConv.cancelReason = 'PAYMENT_APPROVED';
        conversations.set(phoneKey, pixConv);
    }

    const pixTimeout = pixTimeouts.get(phoneKey);
    if (pixTimeout) { clearTimeout(pixTimeout.timeout); pixTimeouts.delete(phoneKey); }

    const amountNum = parseFloat((amount || '0').replace(/[^0-9,.]/g, '').replace(',', '.'));
    db.recordEvent(paymentMethod === 'CREDIT_CARD' ? 'CARD_PAID' : 'PIX_PAID', {
        phone_key: phoneKey, product_id: productId, product_name: productName,
        amount: amountNum, payment_method: paymentMethod || 'PIX',
        order_code: orderCode, order_bumps: orderBumps
    });
    sendSSE('payment_approved', { phoneKey, customerName, productName, amount, paymentMethod: paymentMethod || 'PIX' });

    const conv = {
        phoneKey, remoteJid,
        funnelId: productId + '_APROVADA',
        stepIndex: 0, orderCode, customerName,
        productId, productName, orderBumps: orderBumps || [],
        amount, pixCode, paymentMethod: paymentMethod || 'PIX',
        waiting_for_response: false,
        createdAt: new Date(), lastSystemMessage: new Date(),
        lastReply: null, canceled: false, completed: false, paused: false,
        transferredFromPix: true
    };
    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);

    // Preserva sticky da instância que já estava atendendo este lead
    if (existingSticky) {
        stickyInstances.set(phoneKey, existingSticky);
        addLog('STICKY_PRESERVED', `📌 Instância preservada ${existingSticky} após pagamento`, { phoneKey });
    }

    addLog('TRANSFER_PIX_APPROVED', `💚 Transferido para APROVADA`, { phoneKey, productId });
    await sendStep(phoneKey);
}

async function startFunnel(phoneKey, remoteJid, funnelId, orderCode, customerName, productId, productName, amount, pixCode, orderBumps, paymentMethod) {
    console.log('🔵 startFunnel:', phoneKey, funnelId);
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) {
        addLog('FUNNEL_BLOCKED', `🚫 Conversa já existe`, { phoneKey, funnelId });
        return;
    }

    if (funnelId.endsWith('_APROVADA')) {
        const amountNum = parseFloat((amount || '0').replace(/[^0-9,.]/g, '').replace(',', '.'));
        db.recordEvent(paymentMethod === 'CREDIT_CARD' ? 'CARD_PAID' : 'PIX_PAID', {
            phone_key: phoneKey, product_id: productId, product_name: productName,
            amount: amountNum, payment_method: paymentMethod || 'PIX',
            order_code: orderCode, order_bumps: orderBumps
        });
        sendSSE('payment_approved', { phoneKey, customerName, productName, amount, paymentMethod: paymentMethod || 'PIX' });
    }

    const conv = {
        phoneKey, remoteJid, funnelId, stepIndex: 0,
        orderCode, customerName, productId, productName,
        orderBumps: orderBumps || [], amount, pixCode,
        paymentMethod: paymentMethod || 'PIX',
        waiting_for_response: false,
        createdAt: new Date(), lastSystemMessage: null,
        lastReply: null, canceled: false, completed: false, paused: false
    };
    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);
    addLog('FUNNEL_START', `🚀 Iniciando ${funnelId} para ${phoneKey}`, { orderCode });
    await sendStep(phoneKey);
}

// ============ SEND STEP (lógica igual ao antigo, com melhorias) ============
async function sendStep(phoneKey) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.pixWaiting || conversation.paused) return;

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel || !funnel.steps || funnel.steps.length === 0) {
        addLog('FUNNEL_EMPTY', `⚠️ Funil ${conversation.funnelId} não encontrado ou vazio`, { phoneKey });
        return;
    }

    const step = funnel.steps[conversation.stepIndex];
    if (!step) return;

    const isFirstMessage = conversation.stepIndex === 0 && !conversation.lastSystemMessage;

    addLog('STEP_START', `📤 Passo ${conversation.stepIndex + 1}/${funnel.steps.length} [${step.type}]`, {
        phoneKey, funnelId: conversation.funnelId, stepId: step.id, waitForReply: step.waitForReply || false
    });

    // ===== DELAY ANTES + DIGITANDO (MELHORADO) =====
    // Se tem delayBefore, mostra "digitando" pelo tempo do delay (igual ao que o usuário pediu)
    if (step.delayBefore && parseInt(step.delayBefore) > 0) {
        const delaySecs = parseInt(step.delayBefore);
        addLog('STEP_DELAY_BEFORE', `⏱️ delayBefore: ${delaySecs}s`, { phoneKey, stepId: step.id });
        // Para texto e imagem+texto, mostra digitando durante o delay
        if (step.type !== 'delay' && step.type !== 'audio' && step.type !== 'viewonce_image' && step.type !== 'viewonce_video') {
            const stickyInst = stickyInstances.get(phoneKey) || getActiveInstances()[0];
            if (stickyInst) {
                await sendPresence(conversation.remoteJid, stickyInst, delaySecs);
            }
        }
        await new Promise(r => setTimeout(r, delaySecs * 1000));
    } else if (step.showTyping && step.type !== 'delay') {
        // showTyping sem delayBefore: usa typingSeconds ou 3s padrão
        const typingSecs = parseInt(step.typingSeconds || 3);
        addLog('STEP_TYPING', `💬 Digitando: ${typingSecs}s`, { phoneKey, stepId: step.id });
        const stickyInst = stickyInstances.get(phoneKey) || getActiveInstances()[0];
        if (stickyInst) await sendPresence(conversation.remoteJid, stickyInst, typingSecs);
        await new Promise(r => setTimeout(r, typingSecs * 1000));
    }

    let result = { success: true };

    if (step.type === 'delay') {
        const delaySecs = parseInt(step.delaySeconds || 10);
        addLog('STEP_DELAY', `⏱️ Delay: ${delaySecs}s`, { phoneKey, stepId: step.id });
        await new Promise(r => setTimeout(r, delaySecs * 1000));
    } else {
        // 🔥 CORREÇÃO CRÍTICA do sistema antigo: marca waiting ANTES de enviar
        if (step.waitForReply) {
            conversation.waiting_for_response = true;
            conversations.set(phoneKey, conversation);
            addLog('STEP_MARKED_WAITING', `✅ Marcado como aguardando ANTES de enviar`, { phoneKey, stepId: step.id });
        }

        result = await sendWithFallback(phoneKey, conversation.remoteJid, step, conversation, isFirstMessage);

        if (result.blocked) {
            addLog('STEP_BLOCKED', `🚫 Bloqueado por duplicação`, { phoneKey, stepId: step.id });
            if (step.waitForReply) { conversation.waiting_for_response = false; conversations.set(phoneKey, conversation); }
            return;
        }
    }

    if (result.success) {
        conversation.lastSystemMessage = new Date();
        conversations.set(phoneKey, conversation);

        if (step.waitForReply && step.type !== 'delay') {
            // Fluxo PARADO — aguarda resposta do lead
            addLog('STEP_WAITING', `⏸️ Aguardando resposta (passo ${conversation.stepIndex + 1})`, { phoneKey, stepId: step.id });
        } else {
            // Avança automaticamente
            addLog('STEP_ADVANCE_AUTO', `⏭️ Avançando automaticamente`, { phoneKey, stepId: step.id });
            await advanceConversation(phoneKey, null, 'auto');
        }
    }
}

// 🔥 CORREÇÃO CRÍTICA: só marca waiting=false se reason='reply'
async function advanceConversation(phoneKey, replyText, reason) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.paused) return;

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel) return;

    // Funil condicional: verifica palavras-chave na resposta
    if (reason === 'reply' && replyText) {
        const currentStep = funnel.steps[conversation.stepIndex];
        if (currentStep && currentStep.conditions && currentStep.conditions.length > 0) {
            const replyNorm = replyText.toLowerCase()
                .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                .replace(/[^a-z0-9\s]/g, ' ');

            for (const condition of currentStep.conditions) {
                const keywords = (condition.keywords || '').toLowerCase()
                    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                    .split(',').map(k => k.trim()).filter(Boolean);
                const matched = keywords.some(kw => replyNorm.includes(kw));
                if (matched && condition.targetFunnelId) {
                    addLog('CONDITION_MATCH', `🎯 Condição "${condition.label}" ativada → ${condition.targetFunnelId}`, { phoneKey });
                    conversation.funnelId = condition.targetFunnelId;
                    conversation.stepIndex = 0;
                    conversation.waiting_for_response = false;
                    conversation.lastReply = new Date();
                    conversations.set(phoneKey, conversation);
                    await sendStep(phoneKey);
                    return;
                }
            }
        }
    }

    const nextStepIndex = conversation.stepIndex + 1;

    if (nextStepIndex >= funnel.steps.length) {
        conversation.waiting_for_response = false;
        conversation.completed = true;
        conversation.completedAt = new Date();
        conversations.set(phoneKey, conversation);
        convToDb(phoneKey, conversation);
        addLog('FUNNEL_DONE', `✅ Funil concluído`, { phoneKey, funnelId: conversation.funnelId });
        sendSSE('funnel_completed', { phoneKey, customerName: conversation.customerName });
        return;
    }

    conversation.stepIndex = nextStepIndex;

    // 🔥 CORREÇÃO: só desmarca waiting se for resposta do lead
    if (reason === 'reply') {
        conversation.lastReply = new Date();
        conversation.waiting_for_response = false;
    }
    // Se reason === 'auto', NÃO mexe em waiting_for_response

    conversations.set(phoneKey, conversation);
    addLog('STEP_NEXT', `➡️ Passo ${nextStepIndex + 1}/${funnel.steps.length}`, { phoneKey, reason });
    await sendStep(phoneKey);
}

// ============ VERIFICAÇÃO DE SAÚDE DAS INSTÂNCIAS ============
async function checkInstancesHealth() {
    const instances = db.getInstances();
    let changed = false;
    for (const inst of instances) {
        if (inst.paused) continue; // não verifica pausadas
        const connected = await checkInstanceConnected(inst.name);
        const wasConnected = !!inst.connected;
        if (connected !== wasConnected) {
            db.setInstanceConnected(inst.name, connected);
            changed = true;
            if (!connected) {
                addLog('INSTANCE_DOWN', `🔴 ${inst.name} caiu!`);
                sendSSE('instance_down', { name: inst.name });
            } else {
                addLog('INSTANCE_UP', `🟢 ${inst.name} voltou!`);
                sendSSE('instance_up', { name: inst.name });
            }
        }
    }
    if (changed) refreshInstanceCache(); // atualiza cache quando muda
}
setInterval(checkInstancesHealth, 60000);

// ============ MIDDLEWARES ============
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ============ AUTH ============
function authMiddleware(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ success: false, message: 'Não autorizado' });
    try { jwt.verify(token, JWT_SECRET); next(); }
    catch { res.status(401).json({ success: false, message: 'Token inválido' }); }
}

app.post('/auth/login', (req, res) => {
    const { login, password } = req.body;
    if (login === ADMIN_LOGIN && password === ADMIN_PASSWORD) {
        const token = jwt.sign({ login }, JWT_SECRET, { expiresIn: '7d' });
        res.json({ success: true, token });
    } else {
        res.status(401).json({ success: false, message: 'Credenciais inválidas' });
    }
});

// ============ SSE (token via query string pois EventSource não suporta headers) ============
app.get('/api/events-public', (req, res) => {
    const token = req.query.t;
    if (!token) return res.status(401).end();
    try { jwt.verify(token, JWT_SECRET); } catch { return res.status(401).end(); }
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.flushHeaders();
    sseClients.push(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch { clearInterval(ping); } }, 25000);
    req.on('close', () => { clearInterval(ping); sseClients = sseClients.filter(c => c !== res); });
});

// ============ WEBHOOKS ============
app.post('/webhook/kirvano', async (req, res) => {
    try {
        const data = req.body;
        const event = String(data.event || '').toUpperCase();
        const status = String(data.status || data.payment_status || '').toUpperCase();
        const method = String(data.payment?.method || data.payment_method || '').toUpperCase();

        const orderCode = data.sale_id || data.checkout_id || 'ORDER_' + Date.now();
        const customerName = data.customer?.name || 'Cliente';
        const customerPhone = data.customer?.phone_number || '';
        const totalPrice = data.total_price || 'R$ 0,00';

        // Captura código PIX — agora pega qrcode também (fix implementado anteriormente)
        const pixCode = data.payment?.pix_url || data.payment?.checkout_url ||
            data.payment?.payment_url || data.payment?.qrcode || null;

        // Order bumps
        const orderBumps = (data.products || []).filter(p => p.is_order_bump).map(p => p.name);
        const mainProducts = (data.products || []).filter(p => !p.is_order_bump);
        const mainOfferId = mainProducts[0]?.offer_id || data.products?.[0]?.offer_id;

        // Identifica produto pelo offer_id
        const productDb = mainOfferId ? db.getProductByOfferId(mainOfferId) : null;
        const productId = productDb?.id || 'GRUPO_VIP';
        const productName = productDb?.name || 'GRUPO VIP';

        const phoneKey = normalizePhoneKey(customerPhone);
        if (!phoneKey || phoneKey.length !== 8) {
            return res.json({ success: false, message: 'Telefone inválido' });
        }

        const remoteJid = phoneToRemoteJid(customerPhone);
        registerPhoneUniversal(customerPhone, phoneKey);

        const isApproved = event.includes('APPROVED') || event.includes('PAID') || status === 'APPROVED';
        const isPix = method.includes('PIX') || event.includes('PIX');
        const isCard = method.includes('CREDIT') || method.includes('CARD') || event.includes('CREDIT');
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';

        addLog('KIRVANO', `${event} — ${customerName}`, { orderCode, phoneKey, method, productId, pixCode: pixCode ? '✅' : '❌' });

        if (isApproved) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && existingConv.funnelId === productId + '_PIX') {
                await transferPixToApproved(phoneKey, remoteJid, orderCode, customerName, productId, productName, totalPrice, orderBumps, paymentMethod);
            } else {
                const pt = pixTimeouts.get(phoneKey);
                if (pt) { clearTimeout(pt.timeout); pixTimeouts.delete(phoneKey); }
                await startFunnel(phoneKey, remoteJid, productId + '_APROVADA', orderCode, customerName, productId, productName, totalPrice, pixCode, orderBumps, paymentMethod);
            }
        } else if (isPix && event.includes('GENERATED')) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && !existingConv.canceled) return res.json({ success: true, message: 'Já existe' });
            await createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, totalPrice, pixCode, orderBumps, 'PIX');
        }

        res.json({ success: true, phoneKey });
    } catch (error) {
        addLog('KIRVANO_ERR', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/webhook/perfectpay', async (req, res) => {
    try {
        const data = req.body;
        const statusEnum = parseInt(data.sale_status_enum);
        const saleCode = data.code;
        const productCode = data.product?.code;
        const planCode = data.plan?.code;
        const customerName = data.customer?.full_name || 'Cliente';
        const customerPhone = (data.customer?.phone_area_code || '') + (data.customer?.phone_number || '');
        const saleAmount = data.sale_amount || 0;
        const totalPrice = 'R$ ' + (saleAmount / 100).toFixed(2).replace('.', ',');
        const paymentTypeEnum = parseInt(data.payment_type_enum || 0);
        const isCard = paymentTypeEnum === 2;
        const pixCode = data.billet_url || data.pix_url || data.billet_number || null;
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';

        const productDb = planCode ? db.getProductByOfferId(planCode) : (productCode ? db.getProductByOfferId(productCode) : null);
        const productId = productDb?.id || 'GRUPO_VIP';
        const productName = productDb?.name || 'GRUPO VIP';

        const phoneKey = normalizePhoneKey(customerPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: false, message: 'Telefone inválido' });

        const remoteJid = phoneToRemoteJid(customerPhone);
        registerPhoneUniversal(customerPhone, phoneKey);
        addLog('PERFECTPAY', `Status ${statusEnum}`, { saleCode, phoneKey, productId });

        if (statusEnum === 2) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && existingConv.funnelId === productId + '_PIX') {
                await transferPixToApproved(phoneKey, remoteJid, saleCode, customerName, productId, productName, totalPrice, [], paymentMethod);
            } else {
                const pt = pixTimeouts.get(phoneKey);
                if (pt) { clearTimeout(pt.timeout); pixTimeouts.delete(phoneKey); }
                await startFunnel(phoneKey, remoteJid, productId + '_APROVADA', saleCode, customerName, productId, productName, totalPrice, pixCode, [], paymentMethod);
            }
            res.json({ success: true, phoneKey, action: 'approved' });
        } else if (statusEnum === 1 && !isCard) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && !existingConv.canceled) return res.json({ success: true, message: 'Já existe' });
            await createPixWaitingConversation(phoneKey, remoteJid, saleCode, customerName, productId, productName, totalPrice, pixCode, [], 'PIX');
            res.json({ success: true, phoneKey, action: 'pix_waiting' });
        } else {
            res.json({ success: true, action: 'status_' + statusEnum });
        }
    } catch (error) {
        addLog('PERFECTPAY_ERR', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// 🔥 CORREÇÃO CRÍTICA: webhook só aceita se waiting_for_response=true
app.post('/webhook/evolution', async (req, res) => {
    try {
        const data = req.body;
        const event = data.event;
        if (event && !event.includes('message')) return res.json({ success: true });

        const messageData = data.data;
        if (!messageData?.key) return res.json({ success: true });

        const remoteJid = messageData.key.remoteJid;
        const fromMe = messageData.key.fromMe;
        if (fromMe) return res.json({ success: true });

        const messageText = extractMessageText(messageData.message);
        const isLid = remoteJid.includes('@lid');
        let phoneToSearch = remoteJid;

        if (isLid) {
            addLog('LID_DETECTED', `🔴 @lid detectado`, { lid: remoteJid });
            if (messageData.key.participant) {
                phoneToSearch = messageData.key.participant;
                addLog('LID_PARTICIPANT', `✅ Número real extraído`, { lid: remoteJid, participant: phoneToSearch });
            } else {
                const mk = lidMapping.get(remoteJid);
                if (mk) { const mc = conversations.get(mk); if (mc) phoneToSearch = mc.remoteJid; }
            }
        }

        const incomingPhone = phoneToSearch.split('@')[0];
        const phoneKey = normalizePhoneKey(incomingPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: true });

        addLog('EVO_MSG', `📩 Mensagem${isLid ? ' (@lid)' : ''}: "${messageText.substring(0, 40)}"`, { phoneKey });

        const hasLock = await acquireWebhookLock(phoneKey);
        if (!hasLock) return res.json({ success: false, message: 'Lock timeout' });

        try {
            const conversation = findConversationUniversal(phoneToSearch);

            if (conversation && isLid) registerLidMapping(remoteJid, conversation.phoneKey, phoneToSearch);

            addLog('EVO_SEARCH', `🔍 Conversa ${conversation ? 'encontrada' : 'não encontrada'}`, {
                phoneKey,
                waiting: conversation ? conversation.waiting_for_response : null,
                pixWaiting: conversation ? conversation.pixWaiting : null,
                paused: conversation ? conversation.paused : null
            });

            if (!conversation || conversation.canceled || conversation.pixWaiting || conversation.paused) {
                addLog('EVO_IGNORED', `Ignorado (inexistente/cancelado/pixWaiting/pausado)`, { phoneKey });
                return res.json({ success: true });
            }

            // 🔥 CORREÇÃO: só aceita se REALMENTE está esperando resposta
            if (!conversation.waiting_for_response) {
                addLog('EVO_NOT_WAITING', `⚠️ Não aguardando resposta — IGNORANDO`, { phoneKey, step: conversation.stepIndex + 1 });
                return res.json({ success: true });
            }

            // Registra mensagem e analisa palavras
            db.logMessage(phoneKey, 'in', messageText, null, null);
            db.processWordFrequency(messageText, conversation.productId);

            addLog('CLIENT_REPLY', `✅ Resposta válida — avançando funil`, { phoneKey, text: messageText.substring(0, 50) });
            sendSSE('client_reply', { phoneKey, text: messageText.substring(0, 100) });

            await advanceConversation(phoneKey, messageText, 'reply');
            res.json({ success: true });
        } finally {
            releaseWebhookLock(phoneKey);
        }
    } catch (error) {
        addLog('EVO_ERR', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============ API ============
app.get('/api/dashboard', authMiddleware, (req, res) => {
    const today = db.getTodayStats();
    const allConvs = [...conversations.values()];
    const active = allConvs.filter(c => !c.canceled && !c.completed && !c.pixWaiting);
    const waiting = active.filter(c => c.waiting_for_response);
    res.json({
        success: true,
        data: {
            active_conversations: active.length - waiting.length,
            waiting_responses: waiting.length,
            pending_pix: pixTimeouts.size,
            completed_today: (today.pix_paid || 0) + (today.card_paid || 0),
            revenue_today: today.revenue || 0,
            pix_generated_today: today.pix_generated || 0,
            active_instances: getActiveInstances().length,
            total_instances: db.getInstances().length,
        }
    });
});

app.get('/api/conversations', authMiddleware, (req, res) => {
    const list = [...conversations.entries()].map(([phoneKey, conv]) => ({
        id: phoneKey,
        phone: (conv.remoteJid || '').replace('@s.whatsapp.net', ''),
        phoneKey,
        customerName: conv.customerName,
        productId: conv.productId,
        productName: conv.productName,
        orderBumps: conv.orderBumps || [],
        funnelId: conv.funnelId,
        stepIndex: conv.stepIndex,
        amount: conv.amount,
        pixCode: conv.pixCode,
        paymentMethod: conv.paymentMethod,
        waiting_for_response: conv.waiting_for_response,
        pixWaiting: conv.pixWaiting || false,
        createdAt: conv.createdAt,
        lastMessageAt: conv.lastSystemMessage,
        lastReplyAt: conv.lastReply,
        orderCode: conv.orderCode,
        stickyInstance: stickyInstances.get(phoneKey),
        canceled: conv.canceled || false,
        completed: conv.completed || false,
        hasError: conv.hasError || false,
        paused: conv.paused || false,
        pixTimeoutRemaining: pixTimeouts.has(phoneKey)
            ? Math.max(0, Math.round((PIX_TIMEOUT - (Date.now() - pixTimeouts.get(phoneKey).createdAt.getTime())) / 1000))
            : null
    }));
    list.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ success: true, data: list });
});

app.post('/api/conversations/:phoneKey/pause', authMiddleware, (req, res) => {
    const { phoneKey } = req.params;
    const { paused } = req.body;
    const conv = conversations.get(phoneKey);
    if (!conv) return res.status(404).json({ success: false });
    conv.paused = paused;
    conversations.set(phoneKey, conv);
    addLog('CONV_PAUSE', `${paused ? '⏸️ Pausado' : '▶️ Retomado'}: ${phoneKey}`);
    res.json({ success: true });
});

app.get('/api/logs', authMiddleware, (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json({ success: true, data: logs.slice(0, limit) });
});

app.get('/api/funnels', authMiddleware, (req, res) => {
    res.json({ success: true, data: db.getFunnels() });
});

app.post('/api/funnels', authMiddleware, async (req, res) => {
    try {
        const funnel = req.body;
        if (!funnel.id || !funnel.name || !Array.isArray(funnel.steps))
            return res.status(400).json({ success: false, error: 'id, name, steps obrigatórios' });
        funnel.steps.forEach((s, i) => { if (!s.id) s.id = 'step_' + Date.now() + '_' + i; });
        db.saveFunnel(funnel);
        addLog('FUNNEL_SAVED', `Funil salvo: ${funnel.id}`, { steps: funnel.steps.length });
        res.json({ success: true, data: funnel });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/funnels/:funnelId/move-step', authMiddleware, (req, res) => {
    const { funnelId } = req.params;
    const { fromIndex, direction } = req.body;
    const funnel = db.getFunnelById(funnelId);
    if (!funnel) return res.status(404).json({ success: false, error: 'Funil não encontrado' });
    const from = parseInt(fromIndex);
    const to = direction === 'up' ? from - 1 : from + 1;
    if (to < 0 || to >= funnel.steps.length) return res.status(400).json({ success: false, error: 'Fora dos limites' });
    [funnel.steps[from], funnel.steps[to]] = [funnel.steps[to], funnel.steps[from]];
    db.saveFunnel(funnel);
    res.json({ success: true, data: funnel });
});

app.get('/api/funnels/export', authMiddleware, (req, res) => {
    const funnels = db.getFunnels();
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="orion-funnels-${new Date().toISOString().split('T')[0]}.json"`);
    res.send(JSON.stringify({ version: '1.0', exportDate: new Date().toISOString(), funnels }, null, 2));
    addLog('FUNNELS_EXPORT', `Export: ${funnels.length} funis`);
});

app.post('/api/funnels/import', authMiddleware, (req, res) => {
    try {
        const { funnels } = req.body;
        if (!Array.isArray(funnels)) return res.status(400).json({ success: false, error: 'Formato inválido' });
        let imported = 0;
        for (const f of funnels) {
            if (f.id && f.name && Array.isArray(f.steps)) { db.saveFunnel(f); imported++; }
        }
        addLog('FUNNELS_IMPORT', `Import: ${imported} funis`);
        res.json({ success: true, imported });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/products', authMiddleware, (req, res) => {
    res.json({ success: true, data: db.getProducts() });
});

app.post('/api/products', authMiddleware, (req, res) => {
    const product = req.body;
    if (!product.id || !product.name) return res.status(400).json({ success: false, error: 'id e name obrigatórios' });
    db.saveProduct(product);
    refreshInstanceCache();
    addLog('PRODUCT_SAVED', `Produto: ${product.name}`);
    res.json({ success: true });
});

app.post('/api/products/:productId/toggle', authMiddleware, (req, res) => {
    db.toggleProduct(req.params.productId, req.body.active);
    res.json({ success: true });
});

app.get('/api/instances', authMiddleware, (req, res) => {
    const instances = db.getInstances();
    const stats = db.getInstanceStats(7);
    res.json({ success: true, data: instances, stats });
});

app.post('/api/instances/:name/pause', authMiddleware, (req, res) => {
    const { name } = req.params;
    const { paused } = req.body;
    db.ensureInstance(name);
    db.setInstancePaused(name, paused);
    refreshInstanceCache();
    addLog('INSTANCE_PAUSE', `${paused ? '⏸️' : '▶️'} ${name} ${paused ? 'pausada' : 'reativada'}`);
    sendSSE(paused ? 'instance_paused' : 'instance_resumed', { name });
    res.json({ success: true });
});

app.post('/api/instances/:name/add', authMiddleware, (req, res) => {
    db.ensureInstance(req.params.name);
    refreshInstanceCache();
    addLog('INSTANCE_ADD', `➕ ${req.params.name} adicionada`);
    res.json({ success: true });
});

app.get('/api/analytics', authMiddleware, (req, res) => {
    const days = parseInt(req.query.days) || 7;
    const productId = req.query.product || null;
    res.json({
        success: true,
        data: {
            eventStats: db.getEventStats(days),
            topWords: db.getTopWords(productId, 30),
            dropoff: db.getFunnelDropoff(),
            instanceStats: db.getInstanceStats(days)
        }
    });
});

app.post('/api/test/trigger', (req, res) => {
    const { type, phoneKey, productId, amount, customerName } = req.body;
    addLog('TEST', `🧪 Teste: ${type}`, { phoneKey });
    if (type === 'pix_generated') sendSSE('pix_generated', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90' });
    else if (type === 'payment_approved') sendSSE('payment_approved', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90', paymentMethod: 'PIX' });
    res.json({ success: true });
});

// ============ INICIALIZAÇÃO ============
app.listen(PORT, async () => {
    console.log('='.repeat(60));
    console.log('🌌 ORION v1.1 — Sistema de Automação WhatsApp');
    console.log('='.repeat(60));
    console.log(`✅ Porta: ${PORT}`);
    console.log(`✅ Evolution: ${EVOLUTION_BASE_URL}`);
    console.log(`✅ Instâncias: ${CONFIGURED_INSTANCES.join(', ')}`);
    console.log(`✅ Limpeza automática: ${CLEANUP_DAYS} dias`);
    console.log('');
    console.log('🔧 Melhorias v1.1:');
    console.log('  ✅ Distribuição inteligente por carga de instâncias');
    console.log('  ✅ Sticky preservado no pagamento aprovado');
    console.log('  ✅ Digitando sincronizado com delayBefore');
    console.log('  ✅ waitForReply 100% respeitado');
    console.log('  ✅ Anti-duplicação com hash MD5');
    console.log('  ✅ SSE com token na query string');
    console.log('  ✅ Busca de conversa 5 níveis');
    console.log('='.repeat(60));
    await checkInstancesHealth();
});
