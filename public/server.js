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
const JWT_SECRET = process.env.JWT_SECRET || 'orion-secret-2025-change-this';
const ADMIN_LOGIN = process.env.ADMIN_LOGIN || 'Danilo';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '$Senha123';
const CLEANUP_DAYS = parseInt(process.env.CLEANUP_DAYS || '7');

// ============ DATABASE ============
const db = require('./database');
db.initDatabase();

// ============ ESTADO EM MEMÓRIA (rápido) ============
let conversations = new Map();
let phoneIndex = new Map();
let phoneVariations = new Map();
let lidMapping = new Map();
let phoneToLid = new Map();
let stickyInstances = new Map();
let pixTimeouts = new Map();
let webhookLocks = new Map();
let logs = [];
let sentMessagesHash = new Map();
let messageBlockTimers = new Map();
let lastSuccessfulInstanceIndex = -1;

// SSE clients para notificações em tempo real
let sseClients = [];

function sendSSE(event, data) {
    const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients = sseClients.filter(res => {
        try { res.write(msg); return true; } catch { return false; }
    });
}

// ============ INSTÂNCIAS ============
function getActiveInstances() {
    const dbInstances = db.getInstances();
    return dbInstances
        .filter(i => !i.paused && i.connected)
        .map(i => i.name);
}

function getAllInstances() {
    return db.getInstances().map(i => i.name);
}

// Registra instâncias configuradas
const CONFIGURED_INSTANCES = (process.env.INSTANCES || 'F01').split(',').map(s => s.trim());
for (const inst of CONFIGURED_INSTANCES) {
    db.ensureInstance(inst);
}

// ============ PRODUTO HELPER ============
function identifyProduct(offerId) {
    const product = db.getProductByOfferId(offerId);
    return product ? product.id : null;
}

// ============ VARIÁVEIS DINÂMICAS ============
function replaceVariables(text, conversation) {
    if (!text || !conversation) return text;
    let result = text;
    if (conversation.pix_code) {
        result = result.replace(/\{PIX_LINK\}/g, conversation.pix_code);
        result = result.replace(/\{PIX_GERADO\}/g, conversation.pix_code);
        result = result.replace(/\{PIX_CODE\}/g, conversation.pix_code);
    }
    if (conversation.customer_name) {
        result = result.replace(/\{NOME_CLIENTE\}/g, conversation.customer_name);
        result = result.replace(/\{NOME\}/g, conversation.customer_name);
    }
    if (conversation.amount) result = result.replace(/\{VALOR\}/g, conversation.amount);
    if (conversation.product_name) result = result.replace(/\{PRODUTO\}/g, conversation.product_name);
    return result;
}

// ============ ANTI-DUPLICAÇÃO ============
function generateMessageHash(phoneKey, step) {
    const base = `${phoneKey}|${step.type}|${step.text || step.mediaUrl || ''}|${step.id}`;
    return crypto.createHash('md5').update(base).digest('hex');
}

function isMessageBlocked(phoneKey, step) {
    const hash = generateMessageHash(phoneKey, step);
    const lastSent = messageBlockTimers.get(hash);
    if (lastSent && (Date.now() - lastSent) < MESSAGE_BLOCK_TIME) return true;
    return false;
}

function registerSentMessage(phoneKey, step) {
    const hash = generateMessageHash(phoneKey, step);
    messageBlockTimers.set(hash, Date.now());
}

setInterval(() => {
    const now = Date.now();
    for (const [hash, ts] of messageBlockTimers.entries()) {
        if (now - ts > MESSAGE_BLOCK_TIME) messageBlockTimers.delete(hash);
    }
}, 120000);

// ============ NORMALIZAÇÃO DE TELEFONE ============
function normalizePhoneKey(phone) {
    if (!phone) return null;
    const cleaned = String(phone).split('@')[0].replace(/\D/g, '');
    if (cleaned.length < 8) return null;
    return cleaned.slice(-8);
}

function generateAllPhoneVariations(fullPhone) {
    const cleaned = String(fullPhone).split('@')[0].replace(/\D/g, '');
    if (cleaned.length < 8) return [];
    const variations = new Set();
    variations.add(cleaned);
    if (!cleaned.startsWith('55')) variations.add('55' + cleaned);
    if (cleaned.startsWith('55')) variations.add(cleaned.substring(2));
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
        }
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

function registerLidMapping(lidJid, phoneKey) {
    if (!lidJid || !phoneKey) return;
    lidMapping.set(lidJid, phoneKey);
    phoneToLid.set(phoneKey, lidJid);
    const cleaned = lidJid.split('@')[0].replace(/\D/g, '');
    if (cleaned) { lidMapping.set(cleaned, phoneKey); lidMapping.set(cleaned + '@lid', phoneKey); }
}

function findConversationUniversal(phone) {
    const phoneKey = normalizePhoneKey(phone);
    if (!phoneKey) return null;
    let conv = conversations.get(phoneKey);
    if (conv) { registerPhoneUniversal(phone, phoneKey); return conv; }
    const variations = generateAllPhoneVariations(phone);
    for (const v of variations) {
        const k = phoneIndex.get(v) || phoneVariations.get(v);
        if (k) { conv = conversations.get(k); if (conv) { registerPhoneUniversal(phone, k); return conv; } }
    }
    for (const [key, c] of conversations.entries()) {
        if (key === phoneKey || key.slice(-7) === phoneKey.slice(-7)) { registerPhoneUniversal(phone, key); return c; }
    }
    if (String(phone).includes('@lid')) {
        const mk = lidMapping.get(phone);
        if (mk) { conv = conversations.get(mk); if (conv) return conv; }
    }
    return null;
}

// ============ LOCK ============
async function acquireWebhookLock(phoneKey, timeout = 10000) {
    const start = Date.now();
    while (webhookLocks.get(phoneKey)) {
        if (Date.now() - start > timeout) return false;
        await new Promise(r => setTimeout(r, 100));
    }
    webhookLocks.set(phoneKey, true);
    return true;
}
function releaseWebhookLock(phoneKey) { webhookLocks.delete(phoneKey); }

// ============ SINCRONIZAÇÃO MEMÓRIA <-> DB ============
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

// Salva tudo no DB a cada 15s
setInterval(() => {
    for (const [phoneKey, conv] of conversations.entries()) convToDb(phoneKey, conv);
}, 15000);

// Limpeza automática de conversas antigas
setInterval(() => {
    const deleted = db.deleteOldConversations(CLEANUP_DAYS);
    if (deleted > 0) {
        addLog('CLEANUP', `🧹 ${deleted} conversas antigas removidas (>${CLEANUP_DAYS} dias)`);
        // Remove da memória também
        for (const [phoneKey, conv] of conversations.entries()) {
            if ((conv.completed || conv.canceled) && conv.createdAt) {
                const age = (Date.now() - conv.createdAt.getTime()) / (1000 * 60 * 60 * 24);
                if (age > CLEANUP_DAYS) conversations.delete(phoneKey);
            }
        }
    }
}, 6 * 60 * 60 * 1000); // A cada 6h

// ============ LOGS ============
function addLog(type, message, data = null) {
    const log = { id: Date.now() + Math.random(), timestamp: new Date(), type, message, data };
    logs.unshift(log);
    if (logs.length > 500) logs = logs.slice(0, 500);
    console.log(`[${log.timestamp.toISOString()}] ${type}: ${message}`);
    // Envia via SSE
    sendSSE('log', { type, message, timestamp: log.timestamp });
}

// ============ EVOLUTION API ============
async function sendToEvolution(instanceName, endpoint, payload) {
    const url = `${EVOLUTION_BASE_URL}${endpoint}/${instanceName}`;
    try {
        const response = await axios.post(url, payload, {
            headers: { 'Content-Type': 'application/json', 'apikey': EVOLUTION_API_KEY },
            timeout: 15000
        });
        return { ok: true, data: response.data };
    } catch (error) {
        return { ok: false, error: error.response?.data || error.message, status: error.response?.status };
    }
}

async function checkInstanceConnected(instanceName) {
    try {
        const response = await axios.get(`${EVOLUTION_BASE_URL}/instance/connectionState/${instanceName}`, {
            headers: { 'apikey': EVOLUTION_API_KEY },
            timeout: 5000
        });
        return response.data?.instance?.state === 'open';
    } catch { return false; }
}

async function sendText(remoteJid, text, instanceName) {
    return sendToEvolution(instanceName, '/message/sendText', {
        number: remoteJid.replace('@s.whatsapp.net', ''),
        text
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
        const audioResponse = await axios.get(audioUrl, { responseType: 'arraybuffer', timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const base64Audio = `data:audio/mpeg;base64,${Buffer.from(audioResponse.data).toString('base64')}`;
        const result = await sendToEvolution(instanceName, '/message/sendWhatsAppAudio', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            audio: base64Audio, delay: 1200, encoding: true
        });
        if (result.ok) return result;
        return sendToEvolution(instanceName, '/message/sendMedia', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            mediatype: 'audio', media: base64Audio, mimetype: 'audio/mpeg'
        });
    } catch {
        return sendToEvolution(instanceName, '/message/sendWhatsAppAudio', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            audio: audioUrl, delay: 1200
        });
    }
}

async function sendViewOnce(remoteJid, mediaUrl, mediaType, instanceName) {
    try {
        const mediaResponse = await axios.get(mediaUrl, { responseType: 'arraybuffer', timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const ext = mediaType === 'image' ? 'jpeg' : 'mp4';
        const mimetype = mediaType === 'image' ? 'image/jpeg' : 'video/mp4';
        const base64 = `data:${mimetype};base64,${Buffer.from(mediaResponse.data).toString('base64')}`;
        return sendToEvolution(instanceName, '/message/sendMedia', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            mediatype: mediaType, media: base64,
            mimetype, viewOnce: true
        });
    } catch (error) {
        addLog('VIEWONCE_ERROR', `Erro view once: ${error.message}`);
        return { ok: false, error: error.message };
    }
}

async function sendPresence(remoteJid, instanceName, seconds) {
    try {
        await sendToEvolution(instanceName, '/chat/sendPresence', {
            number: remoteJid.replace('@s.whatsapp.net', ''),
            options: { presence: 'composing', delay: Math.min(seconds * 1000, 25000) }
        });
    } catch {}
}

// ============ ENVIO COM FALLBACK ============
async function sendWithFallback(phoneKey, remoteJid, step, conversation, isFirstMessage = false) {
    if (isMessageBlocked(phoneKey, step)) {
        addLog('SEND_BLOCKED', `🚫 Mensagem duplicada bloqueada`, { phoneKey, stepId: step.id });
        return { success: false, blocked: true };
    }

    const finalText = replaceVariables(step.text, conversation);
    const finalMediaUrl = replaceVariables(step.mediaUrl, conversation);

    const activeInstances = getActiveInstances();
    if (activeInstances.length === 0) {
        addLog('NO_INSTANCES', '⚠️ Nenhuma instância ativa disponível');
        return { success: false, error: 'NO_ACTIVE_INSTANCES' };
    }

    let instancesToTry = [...activeInstances];
    const stickyInstance = stickyInstances.get(phoneKey);
    if (stickyInstance && !isFirstMessage && activeInstances.includes(stickyInstance)) {
        instancesToTry = [stickyInstance, ...activeInstances.filter(i => i !== stickyInstance)];
    } else if (isFirstMessage) {
        lastSuccessfulInstanceIndex = (lastSuccessfulInstanceIndex + 1) % activeInstances.length;
        const idx = lastSuccessfulInstanceIndex;
        instancesToTry = [...activeInstances.slice(idx), ...activeInstances.slice(0, idx)];
    }

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
                    registerSentMessage(phoneKey, step);
                    stickyInstances.set(phoneKey, instanceName);
                    if (isFirstMessage) lastSuccessfulInstanceIndex = activeInstances.indexOf(instanceName);
                    db.updateInstanceStats(instanceName, 1);
                    db.logMessage(phoneKey, 'out', finalText || finalMediaUrl, instanceName, step.id);
                    addLog('SEND_SUCCESS', `✅ Enviado via ${instanceName}`, { phoneKey, stepId: step.id, type: step.type });
                    sendSSE('message_sent', { phoneKey, instance: instanceName, stepType: step.type });
                    return { success: true, instanceName };
                }
                if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
            } catch (e) {
                if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
            }
        }
    }

    addLog('SEND_FAILED', `❌ Falha total para ${phoneKey}`);
    const conv = conversations.get(phoneKey);
    if (conv) { conv.hasError = true; conversations.set(phoneKey, conv); }
    return { success: false };
}

// ============ ORQUESTRAÇÃO ============
async function createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, pixCode, orderBumps, paymentMethod) {
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) {
        addLog('PIX_BLOCKED', `Conversa já existe para ${phoneKey}`);
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
    addLog('PIX_WAITING', `⏳ PIX aguardando para ${phoneKey}`, { orderCode, productId });

    const timeout = setTimeout(async () => {
        const c = conversations.get(phoneKey);
        if (c && c.orderCode === orderCode && !c.canceled && c.pixWaiting) {
            c.pixWaiting = false; c.stepIndex = 0;
            conversations.set(phoneKey, c);
            await sendStep(phoneKey);
        }
        pixTimeouts.delete(phoneKey);
    }, PIX_TIMEOUT);

    pixTimeouts.set(phoneKey, { timeout, orderCode, createdAt: new Date() });
}

async function transferPixToApproved(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, orderBumps, paymentMethod) {
    const pixConv = conversations.get(phoneKey);
    const pixCode = pixConv ? pixConv.pixCode : null;

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
        canceled: false, completed: false, paused: false,
        transferredFromPix: true
    };

    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);
    await sendStep(phoneKey);
}

async function startFunnel(phoneKey, remoteJid, funnelId, orderCode, customerName, productId, productName, amount, pixCode, orderBumps, paymentMethod) {
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) {
        addLog('FUNNEL_BLOCKED', `Conversa já existe para ${phoneKey}`);
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

async function sendStep(phoneKey) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.pixWaiting || conversation.paused) return;

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel || !funnel.steps || funnel.steps.length === 0) return;

    const step = funnel.steps[conversation.stepIndex];
    if (!step) return;

    const isFirstMessage = conversation.stepIndex === 0 && !conversation.lastSystemMessage;

    addLog('STEP_SEND', `📤 Passo ${conversation.stepIndex + 1}/${funnel.steps.length}`, {
        phoneKey, funnelId: conversation.funnelId, stepType: step.type
    });

    // Delay antes
    if (step.delayBefore && parseInt(step.delayBefore) > 0) {
        const secs = parseInt(step.delayBefore);
        // Mostra digitando durante o delay (até 25s)
        if (step.type !== 'delay' && step.type !== 'audio') {
            await sendPresence(conversation.remoteJid, stickyInstances.get(phoneKey) || getActiveInstances()[0], secs);
        }
        await new Promise(r => setTimeout(r, secs * 1000));
    } else if (step.showTyping && step.type !== 'delay') {
        const typingSecs = step.typingSeconds || 3;
        await sendPresence(conversation.remoteJid, stickyInstances.get(phoneKey) || getActiveInstances()[0], typingSecs);
        await new Promise(r => setTimeout(r, typingSecs * 1000));
    }

    let result = { success: true };

    if (step.type === 'delay') {
        const delaySecs = parseInt(step.delaySeconds || 10);
        addLog('STEP_DELAY', `⏱️ Delay: ${delaySecs}s`, { phoneKey });
        await new Promise(r => setTimeout(r, delaySecs * 1000));
    } else {
        if (step.waitForReply) {
            conversation.waiting_for_response = true;
            conversations.set(phoneKey, conversation);
        }
        result = await sendWithFallback(phoneKey, conversation.remoteJid, step, conversation, isFirstMessage);
        if (result.blocked) {
            if (step.waitForReply) { conversation.waiting_for_response = false; conversations.set(phoneKey, conversation); }
            return;
        }
    }

    if (result.success) {
        conversation.lastSystemMessage = new Date();
        conversations.set(phoneKey, conversation);

        // Funil condicional — verifica se o step tem condições definidas
        if (step.waitForReply) {
            addLog('STEP_WAITING', `⏸️ Aguardando resposta (passo ${conversation.stepIndex + 1})`, { phoneKey });
        } else {
            await advanceConversation(phoneKey, null, 'auto');
        }
    }
}

async function advanceConversation(phoneKey, replyText, reason) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.paused) return;

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel) return;

    // Funil condicional: verifica palavras-chave se houver resposta
    if (reason === 'reply' && replyText) {
        const currentStep = funnel.steps[conversation.stepIndex];
        if (currentStep && currentStep.conditions && currentStep.conditions.length > 0) {
            const replyNormalized = replyText.toLowerCase()
                .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                .replace(/[^a-z0-9\s]/g, ' ');

            for (const condition of currentStep.conditions) {
                const keywords = (condition.keywords || '').toLowerCase()
                    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
                    .split(',').map(k => k.trim()).filter(Boolean);

                const matched = keywords.some(kw => replyNormalized.includes(kw));
                if (matched && condition.targetFunnelId) {
                    addLog('CONDITION_MATCH', `🎯 Condição ativada: ${condition.label}`, { phoneKey, targetFunnel: condition.targetFunnelId });
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
        addLog('FUNNEL_DONE', `✅ Funil concluído para ${phoneKey}`, { funnelId: conversation.funnelId });
        sendSSE('funnel_completed', { phoneKey, customerName: conversation.customerName });
        return;
    }

    conversation.stepIndex = nextStepIndex;
    if (reason === 'reply') {
        conversation.lastReply = new Date();
        conversation.waiting_for_response = false;
    }
    conversations.set(phoneKey, conversation);
    await sendStep(phoneKey);
}

// ============ MIDDLEWARES ============
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ============ AUTH ============
function authMiddleware(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '') || req.cookies?.token;
    if (!token) return res.status(401).json({ success: false, message: 'Não autorizado' });
    try {
        jwt.verify(token, JWT_SECRET);
        next();
    } catch {
        res.status(401).json({ success: false, message: 'Token inválido' });
    }
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

// Serve login page and main app
app.use(express.static(path.join(__dirname, 'public')));

// ============ SSE ============
app.get('/api/events', authMiddleware, (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    sseClients.push(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch { clearInterval(ping); } }, 30000);
    req.on('close', () => { clearInterval(ping); sseClients = sseClients.filter(c => c !== res); });
});

// ============ WEBHOOKS ============
app.post('/webhook/kirvano', async (req, res) => {
    try {
        const data = req.body;
        const event = String(data.event || '').toUpperCase();
        const status = String(data.status || '').toUpperCase();
        const method = String(data.payment?.method || data.payment_method || '').toUpperCase();

        const orderCode = data.sale_id || data.checkout_id || 'ORDER_' + Date.now();
        const customerName = data.customer?.name || 'Cliente';
        const customerPhone = data.customer?.phone_number || '';
        const totalPrice = data.total_price || 'R$ 0,00';
        const pixCode = data.payment?.pix_url || data.payment?.checkout_url || data.payment?.payment_url || data.payment?.qrcode || null;
        const pixQrCode = data.payment?.qrcode || null;

        // Order bumps
        const orderBumps = (data.products || []).filter(p => p.is_order_bump).map(p => p.name);
        const mainProducts = (data.products || []).filter(p => !p.is_order_bump);

        // Identifica produto
        const mainOfferId = mainProducts[0]?.offer_id || data.products?.[0]?.offer_id;
        const productDb = mainOfferId ? db.getProductByOfferId(mainOfferId) : null;
        const productId = productDb?.id || 'GRUPO_VIP';
        const productName = productDb?.name || 'GRUPO VIP';

        const phoneKey = normalizePhoneKey(customerPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: false, message: 'Telefone inválido' });

        const remoteJid = phoneToRemoteJid(customerPhone);
        registerPhoneUniversal(customerPhone, phoneKey);

        addLog('KIRVANO', `${event} - ${customerName}`, { orderCode, phoneKey, method, productId });

        const isApproved = event.includes('APPROVED') || event.includes('PAID') || status === 'APPROVED';
        const isPix = method.includes('PIX') || event.includes('PIX');
        const isCard = method.includes('CREDIT') || method.includes('CARD') || event.includes('CREDIT');
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';

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
            await createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, totalPrice, pixCode || pixQrCode, orderBumps, 'PIX');
        }

        res.json({ success: true, phoneKey });
    } catch (error) {
        addLog('KIRVANO_ERROR', error.message);
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
        const phoneAreaCode = data.customer?.phone_area_code || '';
        const phoneNumber = data.customer?.phone_number || '';
        const customerPhone = phoneAreaCode + phoneNumber;
        const saleAmount = data.sale_amount || 0;
        const totalPrice = 'R$ ' + (saleAmount / 100).toFixed(2).replace('.', ',');
        const paymentTypeEnum = parseInt(data.payment_type_enum || 0);
        const isCard = paymentTypeEnum === 2;
        const pixCode = data.billet_url || data.pix_url || data.billet_number || null;
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';

        // Identifica produto pela oferta
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
        addLog('PERFECTPAY_ERROR', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

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
            if (messageData.key.participant) phoneToSearch = messageData.key.participant;
            else {
                const mk = lidMapping.get(remoteJid);
                if (mk) { const mc = conversations.get(mk); if (mc) phoneToSearch = mc.remoteJid; }
            }
        }

        const incomingPhone = phoneToSearch.split('@')[0];
        const phoneKey = normalizePhoneKey(incomingPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: true });

        const hasLock = await acquireWebhookLock(phoneKey);
        if (!hasLock) return res.json({ success: false, message: 'Lock timeout' });

        try {
            const conversation = findConversationUniversal(phoneToSearch);
            if (conversation && isLid) registerLidMapping(remoteJid, conversation.phoneKey);

            if (!conversation || conversation.canceled || conversation.pixWaiting || conversation.paused) {
                return res.json({ success: true });
            }

            if (!conversation.waiting_for_response) {
                addLog('NOT_WAITING', `⚠️ Não aguardando resposta, ignorando`, { phoneKey });
                return res.json({ success: true });
            }

            // Registra mensagem recebida
            db.logMessage(phoneKey, 'in', messageText, null, null);
            db.processWordFrequency(messageText, conversation.productId);

            addLog('CLIENT_REPLY', `✅ Resposta: "${messageText.substring(0, 50)}"`, { phoneKey });
            sendSSE('client_reply', { phoneKey, text: messageText.substring(0, 100) });

            await advanceConversation(phoneKey, messageText, 'reply');
            res.json({ success: true });
        } finally {
            releaseWebhookLock(phoneKey);
        }
    } catch (error) {
        addLog('EVOLUTION_ERROR', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

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
    return '[MENSAGEM]';
}

// ============ API ENDPOINTS ============

// Dashboard
app.get('/api/dashboard', authMiddleware, (req, res) => {
    const today = db.getTodayStats();
    const activeConvs = [...conversations.values()].filter(c => !c.canceled && !c.completed && !c.pixWaiting);
    const waitingConvs = activeConvs.filter(c => c.waiting_for_response);
    const activeInstances = getActiveInstances();

    res.json({
        success: true,
        data: {
            active_conversations: activeConvs.length - waitingConvs.length,
            waiting_responses: waitingConvs.length,
            pending_pix: pixTimeouts.size,
            completed_today: today.pix_paid + today.card_paid,
            revenue_today: today.revenue || 0,
            pix_generated_today: today.pix_generated || 0,
            active_instances: activeInstances.length,
            total_instances: getAllInstances().length,
        }
    });
});

// Conversas
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
        pixTimeoutRemaining: pixTimeouts.has(phoneKey) ? Math.max(0, Math.round((PIX_TIMEOUT - (Date.now() - pixTimeouts.get(phoneKey).createdAt.getTime())) / 1000)) : null
    }));
    list.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ success: true, data: list });
});

// Pausar/retomar conversa
app.post('/api/conversations/:phoneKey/pause', authMiddleware, (req, res) => {
    const { phoneKey } = req.params;
    const { paused } = req.body;
    const conv = conversations.get(phoneKey);
    if (!conv) return res.status(404).json({ success: false, message: 'Conversa não encontrada' });
    conv.paused = paused;
    conversations.set(phoneKey, conv);
    addLog('CONV_PAUSE', `${paused ? '⏸️ Pausado' : '▶️ Retomado'}: ${phoneKey}`);
    res.json({ success: true });
});

// Logs
app.get('/api/logs', authMiddleware, (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json({ success: true, data: logs.slice(0, limit) });
});

// Funis
app.get('/api/funnels', authMiddleware, (req, res) => {
    res.json({ success: true, data: db.getFunnels() });
});

app.post('/api/funnels', authMiddleware, async (req, res) => {
    try {
        const funnel = req.body;
        if (!funnel.id || !funnel.name || !Array.isArray(funnel.steps)) {
            return res.status(400).json({ success: false, error: 'Campos obrigatórios: id, name, steps' });
        }
        funnel.steps.forEach((s, i) => { if (!s.id) s.id = 'step_' + Date.now() + '_' + i; });
        db.saveFunnel(funnel);
        addLog('FUNNEL_SAVED', `Funil salvo: ${funnel.id}`);
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
});

app.post('/api/funnels/import', authMiddleware, (req, res) => {
    const { funnels } = req.body;
    if (!Array.isArray(funnels)) return res.status(400).json({ success: false, error: 'Formato inválido' });
    let imported = 0;
    for (const f of funnels) {
        if (f.id && f.name && Array.isArray(f.steps)) { db.saveFunnel(f); imported++; }
    }
    res.json({ success: true, imported });
});

// Produtos
app.get('/api/products', authMiddleware, (req, res) => {
    res.json({ success: true, data: db.getProducts() });
});

app.post('/api/products', authMiddleware, (req, res) => {
    const product = req.body;
    if (!product.id || !product.name) return res.status(400).json({ success: false, error: 'id e name obrigatórios' });
    db.saveProduct(product);
    addLog('PRODUCT_SAVED', `Produto salvo: ${product.name}`);
    res.json({ success: true });
});

app.post('/api/products/:productId/toggle', authMiddleware, (req, res) => {
    const { productId } = req.params;
    const { active } = req.body;
    db.toggleProduct(productId, active);
    res.json({ success: true });
});

// Instâncias
app.get('/api/instances', authMiddleware, (req, res) => {
    const instances = db.getInstances();
    const stats = db.getInstanceStats(7);
    res.json({ success: true, data: instances, stats });
});

app.post('/api/instances/:name/pause', authMiddleware, (req, res) => {
    const { name } = req.params;
    const { paused } = req.body;
    db.setInstancePaused(name, paused);
    db.ensureInstance(name);
    addLog('INSTANCE_PAUSE', `${paused ? '⏸️' : '▶️'} Instância ${name} ${paused ? 'pausada' : 'retomada'}`);
    res.json({ success: true });
});

app.post('/api/instances/:name/add', authMiddleware, (req, res) => {
    const { name } = req.params;
    db.ensureInstance(name);
    addLog('INSTANCE_ADDED', `➕ Instância ${name} adicionada`);
    res.json({ success: true });
});

// Métricas
app.get('/api/analytics', authMiddleware, (req, res) => {
    const days = parseInt(req.query.days) || 7;
    const productId = req.query.product || null;
    const eventStats = db.getEventStats(days);
    const topWords = db.getTopWords(productId, 30);
    const dropoff = db.getFunnelDropoff();
    const instanceStats = db.getInstanceStats(days);
    res.json({ success: true, data: { eventStats, topWords, dropoff, instanceStats } });
});

// Verificação de instâncias (polling periódico)
async function checkInstancesHealth() {
    const instances = db.getInstances();
    for (const inst of instances) {
        const connected = await checkInstanceConnected(inst.name);
        if (connected !== !!inst.connected) {
            db.setInstanceConnected(inst.name, connected);
            if (!connected) {
                addLog('INSTANCE_DOWN', `🔴 Instância ${inst.name} caiu!`);
                sendSSE('instance_down', { name: inst.name });
            } else {
                addLog('INSTANCE_UP', `🟢 Instância ${inst.name} voltou!`);
                sendSSE('instance_up', { name: inst.name });
            }
        }
    }
}

setInterval(checkInstancesHealth, 60000); // Verifica a cada 1 minuto

// Rota de teste (sem auth para facilitar testes externos)
app.post('/api/test/trigger', (req, res) => {
    const { type, phoneKey, productId, amount, customerName } = req.body;
    addLog('TEST_TRIGGER', `🧪 Teste: ${type}`, { phoneKey, productId });
    if (type === 'pix_generated') {
        sendSSE('pix_generated', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90' });
    } else if (type === 'payment_approved') {
        sendSSE('payment_approved', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90', paymentMethod: 'PIX' });
    }
    res.json({ success: true });
});

// ============ INICIALIZAÇÃO ============
app.listen(PORT, () => {
    console.log('='.repeat(60));
    console.log('🌌 ORION v1.0 - Sistema de Automação WhatsApp');
    console.log('='.repeat(60));
    console.log(`✅ Porta: ${PORT}`);
    console.log(`✅ Evolution: ${EVOLUTION_BASE_URL}`);
    console.log(`✅ Instâncias configuradas: ${CONFIGURED_INSTANCES.join(', ')}`);
    console.log(`✅ Limpeza automática: ${CLEANUP_DAYS} dias`);
    console.log('='.repeat(60));
    checkInstancesHealth();
});
