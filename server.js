const express = require('express');
const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const app = express();

// ============ WEB PUSH (notificações no celular) ============
let webpush = null;
try {
    webpush = require('web-push');
    const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY;
    const VAPID_PRIVATE = process.env.VAPID_PRIVATE_KEY;
    if (!VAPID_PUBLIC || !VAPID_PRIVATE) throw new Error('VAPID keys não configuradas no ambiente');
    webpush.setVapidDetails('mailto:admin@orion.app', VAPID_PUBLIC, VAPID_PRIVATE);
    console.log('✅ Web Push configurado');
} catch(e) {
    console.log('⚠️ web-push não instalado — notificações push desativadas');
}

// Assinaturas push em memória + banco
const pushSubscriptions = new Map();

// ============ CONFIGURAÇÕES ============
const EVOLUTION_BASE_URL = process.env.EVOLUTION_BASE_URL;
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY;
const PIX_TIMEOUT = parseInt(process.env.PIX_TIMEOUT_MS || (7 * 60 * 1000));
const PORT = process.env.PORT || 3000;
const MESSAGE_BLOCK_TIME = 60000;
const JWT_SECRET = process.env.JWT_SECRET;
const ADMIN_LOGIN = process.env.ADMIN_LOGIN;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
const CLEANUP_DAYS = parseInt(process.env.CLEANUP_DAYS || '7');
const NOTIFICATION_NUMBER = process.env.NOTIFICATION_NUMBER;
const NOTIFICATION_INSTANCE = process.env.NOTIFICATION_INSTANCE;
if (!JWT_SECRET || !ADMIN_LOGIN || !ADMIN_PASSWORD) {
  throw new Error("Variáveis de ambiente obrigatórias não definidas!");
}

// ============ DATABASE ============
const db = require('./database');
db.initDatabase();

// ============ RESTAURAR STICKY DO BANCO (sobrevive reinicializações) ============
function restoreStickyFromDB() {
    try {
        const rows = db.getDb().prepare("SELECT phone_key, sticky_instance FROM conversations WHERE sticky_instance IS NOT NULL AND canceled=0 AND completed=0").all();
        let restored = 0;
        for (const row of rows) {
            if (row.sticky_instance && row.phone_key) {
                stickyInstances.set(row.phone_key, row.sticky_instance);
                restored++;
            }
        }
        // Também restaurar para conversas concluídas recentes (últimos 7 dias) para reativação
        const recent = db.getDb().prepare("SELECT phone_key, sticky_instance FROM conversations WHERE sticky_instance IS NOT NULL AND datetime(created_at) > datetime('now','-7 days')").all();
        for (const row of recent) {
            if (row.sticky_instance && row.phone_key && !stickyInstances.has(row.phone_key)) {
                stickyInstances.set(row.phone_key, row.sticky_instance);
            }
        }
        if (restored > 0) console.log(`✅ Sticky restaurado: ${restored} clientes vinculados às suas instâncias`);
    } catch(e) { console.log('Sticky restore erro:', e.message); }
}

// ROLLBACK SEGURO: restaura conversas ativas (PIX pendente + funil em andamento) do banco para memória
function restorePendingConversations() {
    try {
        const rows = db.getDb().prepare(`
            SELECT * FROM conversations
            WHERE canceled=0 AND completed=0
              AND datetime(created_at) > datetime('now','-3 days')
        `).all();
        let restored = 0;
        for (const row of rows) {
            const conv = {
                phoneKey: row.phone_key,
                remoteJid: row.remote_jid,
                funnelId: row.funnel_id,
                stepIndex: row.step_index,
                orderCode: row.order_code,
                customerName: row.customer_name,
                productId: row.product_id,
                productName: row.product_name,
                orderBumps: (() => { try { return JSON.parse(row.order_bumps || '[]'); } catch(e) { return []; } })(),
                amount: row.amount || 0,
                amountDisplay: row.amount_display,
                netValue: row.net_value || 0,
                pixCode: row.pix_code,
                paymentMethod: row.payment_method || 'PIX',
                ddd: row.ddd, city: row.city, state: row.state,
                waiting_for_response: !!row.waiting_for_response,
                pixWaiting: !!row.pix_waiting,
                canceled: false, completed: false,
                hasError: !!row.has_error,
                invalidNumber: !!row.invalid_number,
                transferredFromPix: !!row.transferred_from_pix,
                paused: !!row.paused,
                reactivation: !!row.reactivation,
                abFunnelVariant: row.ab_funnel_variant,
                createdAt: row.created_at ? new Date(row.created_at) : new Date(),
                lastMessageAt: row.last_message_at ? new Date(row.last_message_at) : null,
                lastReplyAt: row.last_reply_at ? new Date(row.last_reply_at) : null
            };
            conversations.set(row.phone_key, conv);
            if (row.sticky_instance) stickyInstances.set(row.phone_key, row.sticky_instance);
            restored++;
        }
        if (restored > 0) console.log(`💾 Conversas restauradas: ${restored} em andamento recuperadas do banco`);
    } catch(e) { console.log('Restore conversations erro:', e.message); }
}

// ROLLBACK SEGURO: restaura timers PIX pendentes após restart do servidor
function restorePendingPixTimeouts() {
    try {
        db.cleanExpiredPixTimeouts();
        const rows = db.getAllPendingPixTimeouts();
        let restored = 0, fired = 0;
        const now = Date.now();
        for (const row of rows) {
            const fireAt = new Date(row.fire_at).getTime();
            const remaining = fireAt - now;
            const phoneKey = row.phone_key;
            const orderCode = row.order_code;

            // Recupera conversa do banco (conversations Map é reconstruído via outros meios, mas o timer em si precisa voltar)
            const conv = conversations.get(phoneKey);

            if (remaining <= 0) {
                // Timer já deveria ter disparado — dispara agora
                (async () => {
                    try {
                        const c = conversations.get(phoneKey);
                        if (c && c.orderCode === orderCode && !c.canceled && c.pixWaiting) {
                            c.pixWaiting = false; c.stepIndex = 0;
                            const selectedFunnel = selectABFunnel(c.productId, 'PIX');
                            c.funnelId = selectedFunnel; c.abFunnelVariant = selectedFunnel;
                            conversations.set(phoneKey, c);
                            db.recordABResult(selectedFunnel, false);
                            db.recordFunnelReceipt(phoneKey, c.productId, 'PIX', selectedFunnel);
                            await sendStep(phoneKey);
                        }
                        pixTimeouts.delete(phoneKey);
                        db.deletePixTimeout(phoneKey);
                    } catch(e) { console.error('Erro ao disparar timer restaurado:', e.message); }
                })();
                fired++;
            } else {
                // Reagenda com tempo restante
                const timeout = setTimeout(async () => {
                    try {
                        const c = conversations.get(phoneKey);
                        if (c && c.orderCode === orderCode && !c.canceled && c.pixWaiting) {
                            c.pixWaiting = false; c.stepIndex = 0;
                            const selectedFunnel = selectABFunnel(c.productId, 'PIX');
                            c.funnelId = selectedFunnel; c.abFunnelVariant = selectedFunnel;
                            conversations.set(phoneKey, c);
                            db.recordABResult(selectedFunnel, false);
                            db.recordFunnelReceipt(phoneKey, c.productId, 'PIX', selectedFunnel);
                            await sendStep(phoneKey);
                        }
                        pixTimeouts.delete(phoneKey);
                        db.deletePixTimeout(phoneKey);
                    } catch(e) { console.error('Erro ao disparar timer reagendado:', e.message); }
                }, remaining);
                pixTimeouts.set(phoneKey, { timeout, orderCode, createdAt: new Date() });
                restored++;
            }
        }
        if (restored > 0 || fired > 0) {
            console.log(`⏱️  Timers PIX restaurados: ${restored} reagendados, ${fired} disparados imediatamente`);
        }
    } catch(e) { console.log('Restore PIX timers erro:', e.message); }
}

// ============ ESTADO EM MEMÓRIA ============
let conversations = new Map();
let phoneIndex = new Map();
let phoneVariations = new Map();
let lidMapping = new Map();
let phoneToLid = new Map();
let stickyInstances = new Map();
let pixTimeouts = new Map();
let webhookLocks = new Map();
let logs = [];
let messageBlockTimers = new Map();
let sentMessagesHash = new Map();
let lastSuccessfulInstanceIndex = -1;
let activeInstancesCache = [];
let sseClients = [];

// A/B: índice atual por produto
let abIndexMap = new Map();

// ============ SSE ============
function sendSSE(event, data) {
    const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients = sseClients.filter(res => { try { res.write(msg); return true; } catch { return false; } });
}

// ============ INSTÂNCIAS ============
let abandonoInstancesCache = [];
function refreshInstanceCache() {
    const all = db.getInstances();
    const NOTIF_NAMES = ['NOTIFICACAO','NOTIFICACOES','NOTIFICAÇAO','NOTIFICAÇÕES'];
    if (NOTIFICATION_INSTANCE) NOTIF_NAMES.push(NOTIFICATION_INSTANCE.toUpperCase());
    // Pool principal: exclui notificação E abandono
    activeInstancesCache = all.filter(i => !i.paused && i.connected && !i.is_notification && !i.is_abandono && !NOTIF_NAMES.includes(i.name.toUpperCase())).map(i => i.name);
    // Pool de abandono: apenas instâncias marcadas como abandono, conectadas e não pausadas
    abandonoInstancesCache = all.filter(i => !i.paused && i.connected && i.is_abandono && !i.is_notification).map(i => i.name);
}

function getActiveInstances() { return activeInstancesCache; }
function getAbandonoInstances() { return abandonoInstancesCache; }

// Retorna o pool correto de instâncias baseado no tipo de funil do lead.
// ABANDONO e CARTAO_RECUSADO usam pool isolado; demais usam o pool principal.
function getPoolForFunnelType(funnelType) {
    if (funnelType === 'ABANDONO' || funnelType === 'CARTAO_RECUSADO') {
        return abandonoInstancesCache;
    }
    return activeInstancesCache;
}
function getPoolForConversation(phoneKey) {
    const conv = conversations.get(phoneKey);
    return getPoolForFunnelType(conv?.funnelType);
}

const CONFIGURED_INSTANCES = (process.env.INSTANCES || 'F01').split(',').map(s => s.trim());
for (const inst of CONFIGURED_INSTANCES) db.ensureInstance(inst);
if (NOTIFICATION_INSTANCE) db.ensureInstance(NOTIFICATION_INSTANCE, true);
// Sempre marcar variantes de notificação como is_notification=true
// Garante que NUNCA entrem no pool de envio para clientes
db.ensureInstance('NOTIFICACAO', true);
db.ensureInstance('NOTIFICACOES', true);
// Forçar is_notification=1 para essas instâncias no banco (correção de dados existentes)
try {
    db.getDb().prepare("UPDATE instances SET is_notification=1 WHERE name IN ('NOTIFICACAO','NOTIFICACOES','NOTIFICAÇAO','NOTIFICAÇÕES')").run();
    // Limpar instâncias fantasma (name null ou vazio) que podem ter sido criadas por bugs anteriores
    db.getDb().prepare("DELETE FROM instances WHERE name IS NULL OR trim(name) = ''").run();
} catch(e) {}
refreshInstanceCache();

// ============ NOTIFICAÇÕES ============
// Envia push para o celular
async function sendPushNotification(title, body, type = 'info') {
    if (!webpush || pushSubscriptions.size === 0) return;
    const iconMap = {
        pix_generated: '💰',
        payment: '✅',
        card: '💳',
        instance_down: '🔴',
        instance_up: '🟢',
        info: 'ℹ️'
    };
    const payload = JSON.stringify({
        title,
        body,
        type,
        tag: type,
        url: '/mobile.html',
        timestamp: Date.now()
    });
    const toDelete = [];
    for (const [id, sub] of pushSubscriptions.entries()) {
        try {
            await webpush.sendNotification(sub, payload);
        } catch(e) {
            if (e.statusCode === 410 || e.statusCode === 404) {
                toDelete.push(id);
            }
        }
    }
    // Remove assinaturas expiradas
    for (const id of toDelete) pushSubscriptions.delete(id);
    // Persiste assinaturas no banco
    try {
        db.getDb().prepare("DELETE FROM push_subscriptions WHERE sub_id IN (" + toDelete.map(()=>'?').join(',') + ")").run(...toDelete);
    } catch(e) {}
}

async function sendNotification(message) {
    try {
        const notifInst = db.getNotificationInstance();
        if (!notifInst) return;
        await sendToEvolution(notifInst.name, '/message/sendText', {
            number: NOTIFICATION_NUMBER,
            text: message
        });
        db.getDb().prepare('INSERT INTO notification_log (type, message, sent) VALUES (?, ?, 1)').run('NOTIFICATION', message);
    } catch(e) { console.log('Erro notificação:', e.message); }
}

// Relatórios programados
function formatCurrency(val) { return 'R$ ' + (val || 0).toFixed(2).replace('.', ','); }

async function sendScheduledReport(period) {
    const today = db.getTodayStats();
    const now = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
    const convRate = today.pix_generated > 0 ? ((today.pix_paid + today.card_paid) / today.pix_generated * 100).toFixed(1) : '0.0';
    const active = [...conversations.values()].filter(c => !c.canceled && !c.completed && !c.pixWaiting).length;

    const msgs = {
        morning: `🌅 *BOM DIA — RELATÓRIO NOTURNO*\n\n⏰ ${now}\n\n💰 PIX Gerados: ${today.pix_generated}\n✅ PIX Pagos: ${today.pix_paid}\n💳 Cartão: ${today.card_paid}\n📊 Conversão: ${convRate}%\n💵 Faturamento: ${formatCurrency(today.revenue)}\n💬 Leads no funil: ${active}\n\n_Dados acumulados desde meia-noite_`,
        noon: `☀️ *ATUALIZAÇÃO 12H*\n\n💰 PIX Gerados hoje: ${today.pix_generated}\n✅ Pagamentos: ${today.pix_paid + today.card_paid}\n📊 Conversão: ${convRate}%\n💵 Faturamento: ${formatCurrency(today.revenue)}\n💬 Leads ativos: ${active}`,
        evening: `🌆 *ATUALIZAÇÃO 18H*\n\n💰 PIX Gerados hoje: ${today.pix_generated}\n✅ Pagamentos: ${today.pix_paid + today.card_paid}\n📊 Conversão: ${convRate}%\n💵 Faturamento: ${formatCurrency(today.revenue)}\n💬 Leads ativos: ${active}\n\n${convRate < 20 ? '⚠️ Conversão abaixo do esperado!' : '✅ Conversão no ritmo!'}`,
        night: `🌙 *FECHAMENTO DO DIA*\n\n💰 PIX Gerados: ${today.pix_generated}\n✅ PIX Pagos: ${today.pix_paid}\n💳 Cartão: ${today.card_paid}\n📊 Taxa de conversão: ${convRate}%\n💵 *Faturamento total: ${formatCurrency(today.revenue)}*\n\nBoa noite! 🌙`
    };

    await sendNotification(msgs[period] || msgs.noon);
}

// Agenda relatórios
function scheduleReports() {
    setInterval(async () => {
        const now = new Date();
        const h = now.getHours();
        const m = now.getMinutes();
        if (m === 0) {
            if (h === 8) await sendScheduledReport('morning');
            else if (h === 12) await sendScheduledReport('noon');
            else if (h === 18) await sendScheduledReport('evening');
        }
        if (h === 23 && m === 50) await sendScheduledReport('night');
    }, 60000);
}

// Relatório semanal - domingo 20h
function scheduleWeeklyReport() {
    setInterval(async () => {
        const now = new Date();
        if (now.getDay() === 0 && now.getHours() === 20 && now.getMinutes() === 0) {
            const stats = db.getEventStats(7);
            const totalPix = stats.reduce((a, s) => a + (s.pix_generated || 0), 0);
            const totalPaid = stats.reduce((a, s) => a + (s.paid || 0), 0);
            const totalRev = stats.reduce((a, s) => a + (s.revenue || 0), 0);
            const convRate = totalPix > 0 ? (totalPaid / totalPix * 100).toFixed(1) : '0';
            const topWords = db.getTopWords('ALL', 5).map(w => w.word).join(', ');
            await sendNotification(`📊 *RELATÓRIO SEMANAL*\n\n💰 PIX Gerados: ${totalPix}\n✅ Pagamentos: ${totalPaid}\n📊 Conversão: ${convRate}%\n💵 Faturamento: ${formatCurrency(totalRev)}\n\n🗣️ Top objeções: ${topWords || 'sem dados'}\n\nBoa semana! 💪`);
        }
    }, 60000);
}

// Contador de leads ao vivo - a cada 30min horário comercial
function scheduleLeadCounter() {
    setInterval(async () => {
        const now = new Date();
        const h = now.getHours();
        const m = now.getMinutes();
        if (h >= 7 && h <= 23 && m === 30) {
            const active = [...conversations.values()].filter(c => !c.canceled && !c.completed && !c.pixWaiting).length;
            const waiting = [...conversations.values()].filter(c => c.waiting_for_response).length;
            const pix = pixTimeouts.size;
            await sendNotification(`💬 *LEADS AO VIVO*\n\n🟢 Ativos no funil: ${active}\n⏸️ Aguardando resposta: ${waiting}\n⏳ PIX pendentes: ${pix}`);
        }
    }, 60000);
}

// Alerta de queda de conversão
let pixGeneratedLast2h = 0;
let pixPaidLast2h = 0;
function trackConversionAlert() {
    setInterval(async () => {
        if (pixGeneratedLast2h >= 5 && pixPaidLast2h === 0) {
            await sendNotification(`⚠️ *ALERTA DE CONVERSÃO*\n\n${pixGeneratedLast2h} PIX gerados nas últimas 2h sem nenhum pagamento!\nVerifique seus anúncios e o checkout.`);
        }
        pixGeneratedLast2h = 0;
        pixPaidLast2h = 0;
    }, 2 * 60 * 60 * 1000);
}

scheduleReports();
scheduleWeeklyReport();
scheduleLeadCounter();
trackConversionAlert();

// ============ VARIÁVEIS DINÂMICAS ============
function getSaudacao() {
    const h = new Date().getHours();
    if (h >= 5 && h < 12) return 'bom dia';
    if (h >= 12 && h < 18) return 'boa tarde';
    return 'boa noite';
}

function formatName(fullName) {
    if (!fullName) return '';
    const first = fullName.trim().split(/\s+/)[0];
    return first.charAt(0).toUpperCase() + first.slice(1).toLowerCase();
}

function replaceVariables(text, conversation) {
    if (!text || !conversation) return text;
    let r = text;
    if (conversation.pixCode) { r = r.replace(/\{PIX_LINK\}/g, conversation.pixCode); r = r.replace(/\{PIX_GERADO\}/g, conversation.pixCode); r = r.replace(/\{PIX_CODE\}/g, conversation.pixCode); }
    if (conversation.customerName) { r = r.replace(/\{NOME_CLIENTE\}/g, formatName(conversation.customerName)); r = r.replace(/\{NOME\}/g, formatName(conversation.customerName)); }
    if (conversation.amountDisplay) r = r.replace(/\{VALOR\}/g, conversation.amountDisplay);
    if (conversation.productName) r = r.replace(/\{PRODUTO\}/g, conversation.productName);
    if (conversation.city) r = r.replace(/\{CIDADE\}/g, conversation.city);
    if (conversation.state) r = r.replace(/\{ESTADO\}/g, conversation.state);
    r = r.replace(/\{SAUDACAO\}/g, getSaudacao());
    return r;
}

// ============ A/B TEST ============
function selectABFunnel(productId, funnelType) {
    const product = db.getProducts().find(p => p.id === productId);
    if (!product) return productId + '_' + funnelType;

    let abFunnelIds = [];
    try { abFunnelIds = JSON.parse(product.ab_funnel_ids || '[]'); } catch {}

    // Filtra só funis do tipo correto
    const relevantFunnels = abFunnelIds.filter(id => {
        const f = db.getFunnelById(id);
        return f && (f.type === funnelType || id.includes(funnelType));
    });

    const defaultFunnel = productId + '_' + funnelType;
    if (relevantFunnels.length === 0) return defaultFunnel;

    // Adiciona o funil padrão ao pool se não estiver
    const pool = [defaultFunnel, ...relevantFunnels.filter(id => id !== defaultFunnel)];

    const key = productId + '_' + funnelType;
    const currentIdx = abIndexMap.get(key) || 0;
    const selectedFunnel = pool[currentIdx % pool.length];
    abIndexMap.set(key, currentIdx + 1);

    addLog('AB_SELECT', `🔄 A/B: ${selectedFunnel} (variante ${(currentIdx % pool.length) + 1}/${pool.length})`, { productId, funnelType });
    return selectedFunnel;
}

// ============ GATILHOS ============
function normStr(str) { return String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9\s]/g, ' ').trim(); }

function similarityScore(a, b) {
    if (a === b) return 1;
    if (Math.abs(a.length - b.length) > 3) return 0;
    let matches = 0;
    const shorter = a.length < b.length ? a : b;
    const longer = a.length < b.length ? b : a;
    for (let i = 0; i < shorter.length; i++) {
        if (longer.includes(shorter[i])) matches++;
    }
    return matches / longer.length;
}

function checkTriggers(text, conversation) {
    const triggers = db.getTriggers();
    if (!triggers.length) return null;
    const normText = normStr(text);

    for (const trigger of triggers) {
        const keywords = trigger.keywords.split(';').map(k => normStr(k.trim())).filter(Boolean);

        for (const kw of keywords) {
            let matched = false;

            if (trigger.match_type === 'exact') {
                matched = normText === kw;
            } else if (trigger.match_type === 'contains') {
                matched = normText.includes(kw);
            } else if (trigger.match_type === 'similar') {
                // Contém com tolerância OU similaridade alta
                matched = normText.includes(kw) || keywords.some(k => normText.split(' ').some(word => similarityScore(word, k) >= 0.75));
            }

            if (matched) {
                addLog('TRIGGER_MATCH', `🎯 Gatilho "${trigger.name}" ativado (${trigger.match_type})`, { keyword: kw, text: text.substring(0, 50) });
                return trigger;
            }
        }
    }
    return null;
}

// ============ ANTI-DUPLICAÇÃO ============
function generateMessageHash(phoneKey, step, conversation) {
    return crypto.createHash('md5').update(`${phoneKey}|${step.type}|${step.text || step.mediaUrl || ''}|${step.id}`).digest('hex');
}
function isMessageBlocked(phoneKey, step, conversation) {
    const hash = generateMessageHash(phoneKey, step, conversation);
    const last = messageBlockTimers.get(hash);
    if (last && (Date.now() - last) < MESSAGE_BLOCK_TIME) return true;
    return false;
}
function registerSentMessage(phoneKey, step, conversation) {
    const hash = generateMessageHash(phoneKey, step, conversation);
    messageBlockTimers.set(hash, Date.now());
}
setInterval(() => {
    const now = Date.now();
    for (const [h, ts] of messageBlockTimers.entries()) if (now - ts > MESSAGE_BLOCK_TIME) messageBlockTimers.delete(h);
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
    const v = new Set([cleaned]);
    if (!cleaned.startsWith('55')) v.add('55' + cleaned);
    if (cleaned.startsWith('55')) v.add(cleaned.substring(2));
    for (let i = 8; i <= Math.min(13, cleaned.length); i++) {
        const ln = cleaned.slice(-i); v.add(ln);
        if (!ln.startsWith('55')) v.add('55' + ln);
    }
    if (cleaned.length >= 11) {
        const ddd = cleaned.slice(-11, -9), num = cleaned.slice(-9);
        if (num[0] === '9') { const s = ddd + num.substring(1); v.add(s); v.add('55' + s); }
        else { const c = ddd + '9' + num; v.add(c); v.add('55' + c); }
    }
    if (cleaned.length === 12 && cleaned.startsWith('55')) { const n = '55' + cleaned.substring(2, 4) + '9' + cleaned.substring(4); v.add(n); v.add(n.substring(2)); }
    if (cleaned.length === 13 && cleaned.startsWith('55')) { const n = cleaned.substring(0, 4) + cleaned.substring(5); v.add(n); v.add(n.substring(2)); }
    return Array.from(v).filter(x => x && x.length >= 8);
}

function registerPhoneUniversal(fullPhone, phoneKey) {
    if (!phoneKey || phoneKey.length !== 8) return;
    const variations = generateAllPhoneVariations(fullPhone);
    const suffixes = ['@s.whatsapp.net', '@lid', '@g.us', ''];
    variations.forEach(v => { phoneIndex.set(v, phoneKey); phoneVariations.set(v, phoneKey); suffixes.forEach(s => { phoneIndex.set(v + s, phoneKey); phoneVariations.set(v + s, phoneKey); }); });
}

function registerLidMapping(lidJid, phoneKey) {
    if (!lidJid || !phoneKey) return;
    lidMapping.set(lidJid, phoneKey); phoneToLid.set(phoneKey, lidJid);
    const lc = lidJid.split('@')[0].replace(/\D/g, '');
    if (lc) { lidMapping.set(lc, phoneKey); lidMapping.set(lc + '@lid', phoneKey); }
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
        const mk = lidMapping.get(phone) || lidMapping.get(String(phone).split('@')[0]);
        if (mk) { conv = conversations.get(mk); if (conv) return conv; }
    }
    return null;
}

// ============ LOCK ============
async function acquireWebhookLock(phoneKey, timeout = 10000) {
    const start = Date.now();
    while (webhookLocks.get(phoneKey)) { if (Date.now() - start > timeout) return false; await new Promise(r => setTimeout(r, 100)); }
    webhookLocks.set(phoneKey, true); return true;
}
function releaseWebhookLock(phoneKey) { webhookLocks.delete(phoneKey); }

// ============ HELPERS ============
function phoneToRemoteJid(phone) {
    let c = phone.replace(/\D/g, '');
    if (!c.startsWith('55')) c = '55' + c;
    if (c.length === 12) c = '55' + c.substring(2, 4) + '9' + c.substring(4);
    return c + '@s.whatsapp.net';
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
    if (message.reactionMessage) return '[REAÇÃO]';
    if (message.viewOnceMessage) return '[MÍDIA ÚNICA]';
    return '[MENSAGEM]';
}

function addLog(type, message, data = null) {
    const log = { id: Date.now() + Math.random(), timestamp: new Date(), type, message, data };
    logs.unshift(log);
    if (logs.length > 500) logs = logs.slice(0, 500);
    console.log(`[${log.timestamp.toISOString()}] ${type}: ${message}`);
    sendSSE('log', { type, message, timestamp: log.timestamp });
}

// ============ DELAY COM VARIAÇÃO ALEATÓRIA ============
function randomDelay(seconds) {
    if (!seconds || seconds <= 0) return 0;
    const sec = parseInt(seconds);
    const min = Math.max(1, Math.round(sec * 0.8));
    const max = Math.round(sec * 1.2);
    return Math.floor(Math.random() * (max - min + 1) + min);
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
        amount: conv.amount || 0,
        amount_display: conv.amountDisplay,
        net_value: conv.netValue || 0,
        pix_code: conv.pixCode,
        payment_method: conv.paymentMethod || 'PIX',
        ddd: conv.ddd,
        city: conv.city,
        state: conv.state,
        waiting_for_response: conv.waiting_for_response,
        pix_waiting: conv.pixWaiting,
        sticky_instance: stickyInstances.get(phoneKey),
        canceled: conv.canceled,
        completed: conv.completed,
        has_error: conv.hasError,
        invalid_number: conv.invalidNumber,
        transferred_from_pix: conv.transferredFromPix,
        paused: conv.paused,
        reactivation: conv.reactivation,
        ab_funnel_variant: conv.abFunnelVariant,
        created_at: conv.createdAt ? new Date(conv.createdAt).toISOString() : new Date().toISOString(),
        last_message_at: conv.lastSystemMessage ? new Date(conv.lastSystemMessage).toISOString() : null,
        last_reply_at: conv.lastReply ? new Date(conv.lastReply).toISOString() : null,
        completed_at: conv.completedAt ? new Date(conv.completedAt).toISOString() : null,
        canceled_at: conv.canceledAt ? new Date(conv.canceledAt).toISOString() : null,
    });
}

setInterval(() => { for (const [k, c] of conversations.entries()) convToDb(k, c); }, 15000);

setInterval(() => {
    const deleted = db.deleteOldConversations(CLEANUP_DAYS);
    if (deleted > 0) {
        for (const [k, c] of conversations.entries()) {
            if ((c.completed || c.canceled) && c.createdAt) {
                const age = (Date.now() - new Date(c.createdAt).getTime()) / 86400000;
                if (age > CLEANUP_DAYS) { conversations.delete(k); stickyInstances.delete(k); }
            }
        }
    }
}, 6 * 60 * 60 * 1000);

// ============ EVOLUTION API ============
async function sendToEvolution(instanceName, endpoint, payload) {
    const url = `${EVOLUTION_BASE_URL}${endpoint}/${instanceName}`;
    try {
        const response = await axios.post(url, payload, { headers: { 'Content-Type': 'application/json', 'apikey': EVOLUTION_API_KEY }, timeout: 15000 });
        return { ok: true, data: response.data };
    } catch (error) {
        const status = error.response?.status;
        const isInvalidNumber = status === 400 && JSON.stringify(error.response?.data || '').includes('not exist');
        return { ok: false, error: error.response?.data || error.message, status, invalidNumber: isInvalidNumber };
    }
}

// Gera todas as variações possíveis de um número para envio
function generateSendVariations(phone) {
    const cleaned = String(phone).replace(/\D/g, '');
    const variations = new Set();
    
    // Base: número limpo
    variations.add(cleaned);
    
    // Com 55
    if (!cleaned.startsWith('55')) variations.add('55' + cleaned);
    // Sem 55
    if (cleaned.startsWith('55')) variations.add(cleaned.slice(2));
    
    // Extrai DDD e número
    let core = cleaned.startsWith('55') ? cleaned.slice(2) : cleaned;
    
    if (core.length >= 10) {
        const ddd = core.slice(0, 2);
        const num = core.slice(2);
        
        // Com 9 dígito
        if (num.length === 8) {
            variations.add(ddd + '9' + num);
            variations.add('55' + ddd + '9' + num);
        }
        // Sem 9 dígito
        if (num.length === 9 && num[0] === '9') {
            variations.add(ddd + num.slice(1));
            variations.add('55' + ddd + num.slice(1));
        }
        // Ambas com e sem 9
        variations.add('55' + ddd + num);
        variations.add(ddd + num);
    }
    
    // Ordena por probabilidade: com 55 e 9 dígito primeiro (formato mais comum no Brasil)
    return Array.from(variations).sort((a, b) => {
        const score = (n) => {
            let s = 0;
            if (n.startsWith('55')) s += 3;
            if (n.length === 13) s += 2; // 55 + DDD + 9 + 8 dígitos
            if (n.length === 11) s += 1; // DDD + 9 + 8 dígitos
            return s;
        };
        return score(b) - score(a);
    });
}

// Envia com fallback de variações de número
async function sendToEvolutionWithPhoneFallback(instanceName, endpoint, payload, originalPhone) {
    // Verifica se já temos uma variação que funcionou antes
    const knownVariation = db.getWorkingVariation(originalPhone);
    if (knownVariation) {
        const testPayload = { ...payload, number: knownVariation };
        const result = await sendToEvolution(instanceName, endpoint, testPayload);
        if (result.ok) return { ...result, usedVariation: knownVariation };
    }
    
    const variations = generateSendVariations(originalPhone);
    const failed = [];
    
    for (const variation of variations) {
        const testPayload = { ...payload, number: variation };
        const result = await sendToEvolution(instanceName, endpoint, testPayload);
        
        if (result.ok) {
            // Salva a variação que funcionou
            db.logPhoneVariation(originalPhone, variation, failed, true);
            addLog('PHONE_VAR_OK', `✅ Número funcionou: ${variation} (original: ${originalPhone})`);
            return { ...result, usedVariation: variation };
        }
        
        if (result.invalidNumber || result.status === 400) {
            failed.push(variation);
            continue; // Tenta próxima variação
        }
        
        // Erro de rede ou servidor - não é problema de número, retorna erro
        return result;
    }
    
    // Todas as variações falharam
    db.logPhoneVariation(originalPhone, null, failed, false);
    addLog('PHONE_VAR_FAIL', `❌ Todas as variações falharam para ${originalPhone} (${variations.length} tentadas)`);
    return { ok: false, invalidNumber: true, triedVariations: variations.length };
}

async function checkInstanceConnected(instanceName) {
    try {
        const r = await axios.get(`${EVOLUTION_BASE_URL}/instance/connectionState/${instanceName}`, { headers: { 'apikey': EVOLUTION_API_KEY }, timeout: 5000 });
        return r.data?.instance?.state === 'open';
    } catch { return false; }
}

async function sendPresence(remoteJid, instanceName, seconds) {
    if (!instanceName) return;
    try { await sendToEvolution(instanceName, '/chat/sendPresence', { number: remoteJid.replace('@s.whatsapp.net', ''), options: { presence: 'composing', delay: Math.min(seconds * 1000, 25000) } }); } catch {}
}

async function blockContact(remoteJid, instanceName) {
    try { await sendToEvolution(instanceName, '/chat/updateBlockStatus', { number: remoteJid.replace('@s.whatsapp.net', ''), status: 'block' }); } catch {}
}

async function sendText(remoteJid, text, instanceName) {
    const phone = remoteJid.replace('@s.whatsapp.net', '');
    return sendToEvolutionWithPhoneFallback(instanceName, '/message/sendText', { text }, phone);
}
async function sendImage(remoteJid, url, caption, instanceName) {
    const phone = remoteJid.replace('@s.whatsapp.net', '');
    return sendToEvolutionWithPhoneFallback(instanceName, '/message/sendMedia', { mediatype: 'image', media: url, caption: caption || '' }, phone);
}
async function sendVideo(remoteJid, url, caption, instanceName) {
    const phone = remoteJid.replace('@s.whatsapp.net', '');
    return sendToEvolutionWithPhoneFallback(instanceName, '/message/sendMedia', { mediatype: 'video', media: url, caption: caption || '' }, phone);
}
async function sendSticker(remoteJid, url, instanceName) {
    const phone = remoteJid.replace('@s.whatsapp.net', '');
    return sendToEvolutionWithPhoneFallback(instanceName, '/message/sendSticker', { sticker: url }, phone);
}

async function sendAudio(remoteJid, audioUrl, instanceName) {
    try {
        const audioResponse = await axios.get(audioUrl, { responseType: 'arraybuffer', timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const base64 = `data:audio/mpeg;base64,${Buffer.from(audioResponse.data).toString('base64')}`;
        const r = await sendToEvolution(instanceName, '/message/sendWhatsAppAudio', { number: remoteJid.replace('@s.whatsapp.net', ''), audio: base64, delay: 1200, encoding: true });
        if (r.ok) return r;
        return sendToEvolution(instanceName, '/message/sendMedia', { number: remoteJid.replace('@s.whatsapp.net', ''), mediatype: 'audio', media: base64, mimetype: 'audio/mpeg' });
    } catch {
        return sendToEvolution(instanceName, '/message/sendWhatsAppAudio', { number: remoteJid.replace('@s.whatsapp.net', ''), audio: audioUrl, delay: 1200 });
    }
}

async function sendViewOnce(remoteJid, mediaUrl, mediaType, instanceName) {
    try {
        const resp = await axios.get(mediaUrl, { responseType: 'arraybuffer', timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const mimetype = mediaType === 'image' ? 'image/jpeg' : 'video/mp4';
        const b64 = Buffer.from(resp.data).toString('base64');
        const r = await sendToEvolution(instanceName, '/message/sendMedia', { number: remoteJid.replace('@s.whatsapp.net', ''), mediatype: mediaType, media: b64, mimetype, viewOnce: true });
        if (r.ok) return r;
        return sendToEvolution(instanceName, '/message/sendMedia', { number: remoteJid.replace('@s.whatsapp.net', ''), mediatype: mediaType, media: mediaUrl });
    } catch {
        return sendToEvolution(instanceName, '/message/sendMedia', { number: remoteJid.replace('@s.whatsapp.net', ''), mediatype: mediaType, media: mediaUrl });
    }
}

// ============ SELEÇÃO DE INSTÂNCIA (distribuição inteligente) ============
function selectNextInstance(isFirstMessage, phoneKey) {
    const active = getPoolForConversation(phoneKey);
    if (active.length === 0) return null;
    if (active.length === 1) return active[0];

    let stickyInstance = stickyInstances.get(phoneKey);
    // Se não está na memória, tenta restaurar do banco
    if (!stickyInstance) {
        try {
            const row = db.getDb().prepare('SELECT sticky_instance FROM conversations WHERE phone_key=? AND sticky_instance IS NOT NULL ORDER BY created_at DESC LIMIT 1').get(phoneKey);
            if (row?.sticky_instance) {
                stickyInstance = row.sticky_instance;
                stickyInstances.set(phoneKey, stickyInstance);
            }
        } catch(e) {}
    }
    if (!isFirstMessage && stickyInstance && active.includes(stickyInstance)) return stickyInstance;

    // Para primeiro contato: escolhe a instância com menos mensagens hoje
    if (isFirstMessage) {
        const today = new Date().toISOString().split('T')[0];
        const stats = db.getInstanceStats(1);
        const todayStats = {};
        for (const inst of active) todayStats[inst] = 0;
        for (const s of stats) { if (s.date === today && todayStats[s.instance] !== undefined) todayStats[s.instance] = s.messages_sent; }
        return active.slice().sort((a, b) => todayStats[a] - todayStats[b])[0];
    }

    return active[0];
}

// ============ ENVIO COM FALLBACK ============
async function sendWithFallback(phoneKey, remoteJid, step, conversation, isFirstMessage = false) {
    if (isMessageBlocked(phoneKey, step, conversation)) {
        addLog('SEND_BLOCKED', `🚫 Duplicada bloqueada`, { phoneKey, stepId: step.id });
        return { success: false, blocked: true };
    }

    const finalText = replaceVariables(step.text, conversation);
    const finalMediaUrl = replaceVariables(step.mediaUrl, conversation);

    // Personalização por horário no passo 1
    let actualMediaUrl = finalMediaUrl;
    let actualText = finalText;
    if (step.timeVariants && conversation.stepIndex === 0) {
        const hour = new Date().getHours();
        const variant = hour < 12 ? step.timeVariants.morning : hour < 18 ? step.timeVariants.afternoon : step.timeVariants.evening;
        if (variant) { actualMediaUrl = variant.mediaUrl || actualMediaUrl; actualText = variant.text || actualText; }
    }

    const active = getPoolForConversation(phoneKey);
    if (active.length === 0) { addLog('NO_INSTANCES', '⚠️ Sem instâncias ativas!'); return { success: false, error: 'NO_ACTIVE_INSTANCES' }; }

    const preferred = selectNextInstance(isFirstMessage, phoneKey);
    const stickyInstance = stickyInstances.get(phoneKey);
    let instancesToTry;
    if (!isFirstMessage && stickyInstance && active.includes(stickyInstance)) {
        instancesToTry = [stickyInstance, ...active.filter(i => i !== stickyInstance)];
    } else {
        instancesToTry = preferred ? [preferred, ...active.filter(i => i !== preferred)] : [...active];
    }

    for (const instanceName of instancesToTry) {
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                let result;
                if (step.type === 'text') result = await sendText(remoteJid, actualText, instanceName);
                else if (step.type === 'image') result = await sendImage(remoteJid, actualMediaUrl, '', instanceName);
                else if (step.type === 'image+text') result = await sendImage(remoteJid, actualMediaUrl, actualText, instanceName);
                else if (step.type === 'video') result = await sendVideo(remoteJid, actualMediaUrl, '', instanceName);
                else if (step.type === 'video+text') result = await sendVideo(remoteJid, actualMediaUrl, actualText, instanceName);
                else if (step.type === 'audio') result = await sendAudio(remoteJid, actualMediaUrl, instanceName);
                else if (step.type === 'sticker') result = await sendSticker(remoteJid, actualMediaUrl, instanceName);
                else if (step.type === 'viewonce_image') result = await sendViewOnce(remoteJid, actualMediaUrl, 'image', instanceName);
                else if (step.type === 'viewonce_video') result = await sendViewOnce(remoteJid, actualMediaUrl, 'video', instanceName);
                else result = { ok: true };

                if (result && result.ok) {
                    registerSentMessage(phoneKey, step, conversation);
                    const oldSticky = stickyInstances.get(phoneKey);
                    stickyInstances.set(phoneKey, instanceName);
                    // Persiste no banco para sobreviver reinicializações
                    try { db.getDb().prepare('UPDATE conversations SET sticky_instance=? WHERE phone_key=?').run(instanceName, phoneKey); } catch(e){}
                    if (!oldSticky) addLog('STICKY_SET', `📌 Instância fixada: ${instanceName}`, { phoneKey });
                    else if (oldSticky !== instanceName) addLog('STICKY_CHANGE', `🔄 Instância trocada: ${oldSticky}→${instanceName}`, { phoneKey });
                    db.updateInstanceStats(instanceName, 1);
                    db.updateInstanceHealth(instanceName, true);
                    db.logMessage(phoneKey, 'out', actualText || actualMediaUrl, instanceName, step.id);
                    addLog('SEND_OK', `✅ Enviado via ${instanceName}`, { phoneKey, type: step.type });
                    sendSSE('message_sent', { phoneKey, instance: instanceName, stepType: step.type });
                    return { success: true, instanceName };
                }

                // Número inválido
                if (result.invalidNumber) {
                    addLog('INVALID_NUMBER', `❌ Número inválido: ${phoneKey} (${result.triedVariations || 1} variações testadas)`);
                    db.updateInstanceHealth(instanceName, false, true);
                    const conv = conversations.get(phoneKey);
                    if (conv) { conv.invalidNumber = true; conv.canceled = true; conversations.set(phoneKey, conv); }
                    return { success: false, invalidNumber: true };
                }
                db.updateInstanceHealth(instanceName, false, false);

                if (attempt < 3) await new Promise(r => setTimeout(r, 2000));
            } catch (e) { if (attempt < 3) await new Promise(r => setTimeout(r, 2000)); }
        }
    }

    addLog('SEND_FAILED', `❌ Falha total para ${phoneKey}`);
    const conv = conversations.get(phoneKey);
    if (conv) { conv.hasError = true; conversations.set(phoneKey, conv); }
    return { success: false };
}

// ============ ORQUESTRAÇÃO ============
// ============ ANTI-DUPLICATA COM COOLDOWN CONFIGURÁVEL ============
function getCooldownDays() {
    const setting = db.getSetting('FUNNEL_COOLDOWN_DAYS');
    return parseInt(setting) || 7;
}
function shouldBlockFunnelByCooldown(phoneKey, productId, funnelType) {
    const days = getCooldownDays();
    if (days <= 0) return null;
    const recent = db.hasReceivedFunnelRecently(phoneKey, productId, funnelType, days);
    if (recent) {
        addLog('COOLDOWN_BLOCK', `⏸️ Cooldown ${days}d: ${phoneKey} já recebeu ${funnelType} de ${productId} em ${recent.received_at}`, { phoneKey });
        return recent;
    }
    return null;
}

// ============ PULAR PASSOS DE APRESENTAÇÃO (INTRO) QUANDO VEM DE PIX ============
// Se cliente foi transferido de PIX→Aprovado, pular passos marcados como is_intro=true
// pois cliente já recebeu a apresentação da modelo no funil de PIX.
function getFirstNonIntroStepIndex(funnelId) {
    const funnel = db.getFunnelById(funnelId);
    if (!funnel || !funnel.steps?.length) return 0;
    for (let i = 0; i < funnel.steps.length; i++) {
        if (!funnel.steps[i].is_intro) return i;
    }
    return 0; // todos são intro? começa do 0 mesmo (fallback seguro)
}

async function createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, paymentMethod, location) {
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) { addLog('PIX_BLOCKED', `Já existe para ${phoneKey}`); return; }

    // Anti-duplicata: se recebeu funil PIX para este produto recentemente, não dispara
    if (shouldBlockFunnelByCooldown(phoneKey, productId, 'PIX')) {
        db.recordEvent('PIX_GENERATED', { phone_key: phoneKey, product_id: productId, product_name: productName, amount, net_value: netValue, payment_method: 'PIX', order_code: orderCode, order_bumps: orderBumps });
        pixGeneratedLast2h++;
        sendSSE('pix_generated', { phoneKey, customerName, productName, amount: 'R$ ' + (amount || 0).toFixed(2).replace('.', ','), orderCode, skipped: true });
        addLog('PIX_SKIPPED', `⏸️ PIX registrado mas funil não disparado (cooldown) para ${phoneKey}`, { orderCode });
        return;
    }

    const conv = {
        phoneKey, remoteJid, funnelId: productId + '_PIX', stepIndex: -1, orderCode, customerName,
        productId, productName, orderBumps: orderBumps || [], amount, amountDisplay: 'R$ ' + (amount || 0).toFixed(2).replace('.', ','),
        netValue, pixCode, paymentMethod: paymentMethod || 'PIX',
        ddd: location?.ddd, city: location?.city, state: location?.state,
        waiting_for_response: false, pixWaiting: true,
        createdAt: new Date(), canceled: false, completed: false, paused: false
    };

    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);
    try { convToDb(phoneKey, conv); } catch(e) {} // persiste imediato pro rollback seguro

    db.recordEvent('PIX_GENERATED', { phone_key: phoneKey, product_id: productId, product_name: productName, amount, net_value: netValue, payment_method: 'PIX', order_code: orderCode, order_bumps: orderBumps });
    pixGeneratedLast2h++;

    sendSSE('pix_generated', { phoneKey, customerName, productName, amount: conv.amountDisplay, orderCode });
    await sendNotification(`💰 PIX Gerado - ${conv.amountDisplay} · ${formatName(customerName)}`);
    await sendPushNotification(`💰 PIX Gerado — ${conv.amountDisplay}`, formatName(customerName), 'pix_generated');
    addLog('PIX_WAITING', `⏳ PIX aguardando para ${phoneKey}`, { orderCode });

    const timeout = setTimeout(async () => {
        const c = conversations.get(phoneKey);
        if (c && c.orderCode === orderCode && !c.canceled && c.pixWaiting) {
            c.pixWaiting = false; c.stepIndex = 0;
            const selectedFunnel = selectABFunnel(productId, 'PIX');
            c.funnelId = selectedFunnel; c.abFunnelVariant = selectedFunnel;
            conversations.set(phoneKey, c);
            db.recordABResult(selectedFunnel, false);
            db.recordFunnelReceipt(phoneKey, productId, 'PIX', selectedFunnel);
            await sendStep(phoneKey);
        }
        pixTimeouts.delete(phoneKey);
        try { db.deletePixTimeout(phoneKey); } catch(e) {}
    }, PIX_TIMEOUT);

    pixTimeouts.set(phoneKey, { timeout, orderCode, createdAt: new Date() });
    // ROLLBACK SEGURO: persiste timer no banco para sobreviver a deploy
    try {
        const fireAt = new Date(Date.now() + PIX_TIMEOUT).toISOString();
        db.savePixTimeout(phoneKey, orderCode, fireAt);
    } catch(e) { console.error('Erro ao persistir timer PIX:', e.message); }
}

async function transferPixToApproved(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, netValue, orderBumps, paymentMethod, location) {
    const pixConv = conversations.get(phoneKey);
    const pixCode = pixConv?.pixCode;
    const existingSticky = stickyInstances.get(phoneKey);
    const abVariant = pixConv?.abFunnelVariant;

    if (pixConv) { pixConv.canceled = true; pixConv.canceledAt = new Date(); conversations.set(phoneKey, pixConv); }
    const pt = pixTimeouts.get(phoneKey);
    if (pt) { clearTimeout(pt.timeout); pixTimeouts.delete(phoneKey); }
    try { db.deletePixTimeout(phoneKey); } catch(e) {}

    db.recordEvent(paymentMethod === 'CREDIT_CARD' ? 'CARD_PAID' : 'PIX_PAID', { phone_key: phoneKey, product_id: productId, product_name: productName, amount, net_value: netValue, payment_method: paymentMethod || 'PIX', order_code: orderCode, order_bumps: orderBumps, funnel_id: abVariant });
    // Atualiza receita automática do dia para o módulo de investimentos
    try { db.updateDailyAutoRevenue(new Date().toISOString().split('T')[0], netValue || amount || 0); } catch(e) {}
    if (abVariant) db.recordABResult(abVariant, true);
    if (existingSticky) db.updateInstanceStats(existingSticky, 0, true);
    pixPaidLast2h++;

    const amountDisplay = 'R$ ' + (netValue || amount || 0).toFixed(2).replace('.', ',');
    sendSSE('payment_approved', { phoneKey, customerName, productName, amount: amountDisplay, paymentMethod: paymentMethod || 'PIX' });
    const notifEmoji = paymentMethod === 'CREDIT_CARD' ? '💳 Cartão Aprovado!' : '✅ PIX Pago!';
    await sendNotification(`${notifEmoji} - ${amountDisplay} · ${formatName(customerName)}`);
    const pushType = paymentMethod === 'CREDIT_CARD' ? 'card' : 'payment';
    await sendPushNotification(`${notifEmoji} ${amountDisplay}`, formatName(customerName), pushType);

    const selectedFunnel = selectABFunnel(productId, 'APROVADA');
    // Como foi transferido de PIX, pula passos marcados como "apresentação"
    const startStepIndex = getFirstNonIntroStepIndex(selectedFunnel);
    if (startStepIndex > 0) addLog('SKIP_INTRO', `⏭️ Pulando ${startStepIndex} passo(s) de apresentação (vem de PIX)`, { phoneKey, funnelId: selectedFunnel });
    const conv = {
        phoneKey, remoteJid, funnelId: selectedFunnel, stepIndex: startStepIndex, orderCode, customerName,
        productId, productName, orderBumps: orderBumps || [], amount, amountDisplay, netValue, pixCode,
        paymentMethod: paymentMethod || 'PIX', ddd: location?.ddd, city: location?.city, state: location?.state,
        waiting_for_response: false, createdAt: new Date(), lastSystemMessage: new Date(),
        canceled: false, completed: false, paused: false, transferredFromPix: true, abFunnelVariant: selectedFunnel
    };
    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);
    if (existingSticky) stickyInstances.set(phoneKey, existingSticky);
    db.recordABResult(selectedFunnel, false);
    db.recordFunnelReceipt(phoneKey, productId, 'APROVADA', selectedFunnel);
    await sendStep(phoneKey);
}

async function startFunnel(phoneKey, remoteJid, funnelType, orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, paymentMethod, location) {
    const existing = conversations.get(phoneKey);
    if (existing && !existing.canceled) { addLog('FUNNEL_BLOCKED', `Já existe para ${phoneKey}`); return; }

    // Anti-duplicata por cooldown (sempre registra o evento, mas não dispara mensagem se dentro do cooldown)
    if (shouldBlockFunnelByCooldown(phoneKey, productId, funnelType)) {
        if (funnelType === 'APROVADA') {
            db.recordEvent(paymentMethod === 'CREDIT_CARD' ? 'CARD_PAID' : 'PIX_PAID', { phone_key: phoneKey, product_id: productId, product_name: productName, amount, net_value: netValue, payment_method: paymentMethod || 'PIX', order_code: orderCode, order_bumps: orderBumps });
            pixPaidLast2h++;
        }
        addLog('FUNNEL_SKIPPED', `⏸️ ${funnelType} registrado mas funil não disparado (cooldown) para ${phoneKey}`, { orderCode });
        return;
    }

    if (funnelType === 'APROVADA') {
        db.recordEvent(paymentMethod === 'CREDIT_CARD' ? 'CARD_PAID' : 'PIX_PAID', { phone_key: phoneKey, product_id: productId, product_name: productName, amount, net_value: netValue, payment_method: paymentMethod || 'PIX', order_code: orderCode, order_bumps: orderBumps });
        pixPaidLast2h++;
        const amtDisplay = 'R$ ' + (netValue || amount || 0).toFixed(2).replace('.', ',');
        sendSSE('payment_approved', { phoneKey, customerName, productName, amount: amtDisplay, paymentMethod: paymentMethod || 'PIX' });
        const notifMsg2 = paymentMethod === 'CREDIT_CARD' ? '💳 Cartão Aprovado!' : '✅ PIX Pago!';
        await sendNotification(`${notifMsg2} - ${amtDisplay} · ${formatName(customerName)}`);
        await sendPushNotification(`${notifMsg2} ${amtDisplay}`, formatName(customerName), paymentMethod === 'CREDIT_CARD' ? 'card' : 'payment');
    }

    const selectedFunnel = selectABFunnel(productId, funnelType);
    const amountDisplay = 'R$ ' + (netValue || amount || 0).toFixed(2).replace('.', ',');
    const conv = {
        phoneKey, remoteJid, funnelId: selectedFunnel, stepIndex: 0, orderCode, customerName,
        productId, productName, orderBumps: orderBumps || [], amount, amountDisplay, netValue, pixCode,
        paymentMethod: paymentMethod || 'PIX', ddd: location?.ddd, city: location?.city, state: location?.state,
        waiting_for_response: false, createdAt: new Date(),
        canceled: false, completed: false, paused: false, abFunnelVariant: selectedFunnel,
        funnelType
    };
    conversations.set(phoneKey, conv);
    registerPhoneUniversal(remoteJid, phoneKey);
    db.recordABResult(selectedFunnel, false);
    db.recordFunnelReceipt(phoneKey, productId, funnelType, selectedFunnel);
    addLog('FUNNEL_START', `🚀 Iniciando ${selectedFunnel} para ${phoneKey}`, { orderCode });
    await sendStep(phoneKey);
}

// ============ SEND STEP ============
async function sendStep(phoneKey) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.pixWaiting || conversation.paused || conversation.invalidNumber) return;

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel || !funnel.steps?.length) { addLog('FUNNEL_EMPTY', `⚠️ ${conversation.funnelId} vazio`, { phoneKey }); return; }

    const step = funnel.steps[conversation.stepIndex];
    if (!step) return;

    const isFirstMessage = conversation.stepIndex === 0 && !conversation.lastSystemMessage;
    addLog('STEP_START', `📤 Passo ${conversation.stepIndex + 1}/${funnel.steps.length} [${step.type}]`, { phoneKey, funnelId: conversation.funnelId });

    // Delay com variação aleatória
    if (step.delayBefore && parseInt(step.delayBefore) > 0) {
        const originalSecs = parseInt(step.delayBefore);
        const actualSecs = randomDelay(originalSecs);
        addLog('STEP_DELAY', `⏱️ delayBefore: ${originalSecs}s → ${actualSecs}s (±20%)`, { phoneKey });
        if (step.type !== 'delay' && step.type !== 'audio') {
            const sticky = stickyInstances.get(phoneKey) || getPoolForConversation(phoneKey)[0];
            if (sticky) await sendPresence(conversation.remoteJid, sticky, actualSecs);
        }
        await new Promise(r => setTimeout(r, actualSecs * 1000));
    } else if (step.showTyping && step.type !== 'delay') {
        const typingSecs = randomDelay(parseInt(step.typingSeconds || 3));
        const sticky = stickyInstances.get(phoneKey) || getPoolForConversation(phoneKey)[0];
        if (sticky) await sendPresence(conversation.remoteJid, sticky, typingSecs);
        await new Promise(r => setTimeout(r, typingSecs * 1000));
    }

    let result = { success: true };

    if (step.type === 'delay') {
        const actualSecs = randomDelay(parseInt(step.delaySeconds || 10));
        addLog('STEP_DELAY_EX', `⏱️ Delay: ${actualSecs}s`, { phoneKey });
        await new Promise(r => setTimeout(r, actualSecs * 1000));
    } else {
        if (step.waitForReply) { conversation.waiting_for_response = true; conversations.set(phoneKey, conversation); }
        result = await sendWithFallback(phoneKey, conversation.remoteJid, step, conversation, isFirstMessage);
        if (result.blocked) {
            if (step.waitForReply) { conversation.waiting_for_response = false; conversations.set(phoneKey, conversation); }
            return;
        }
        if (result.invalidNumber) return;
    }

    if (result.success) {
        conversation.lastSystemMessage = new Date();
        conversations.set(phoneKey, conversation);
        if (step.waitForReply && step.type !== 'delay') {
            addLog('STEP_WAIT', `⏸️ Aguardando resposta (passo ${conversation.stepIndex + 1})`, { phoneKey });
        } else {
            await advanceConversation(phoneKey, null, 'auto');
        }
    }
}

async function advanceConversation(phoneKey, replyText, reason) {
    const conversation = conversations.get(phoneKey);
    if (!conversation || conversation.canceled || conversation.paused) return;

    // Verifica gatilhos globais na resposta
    if (reason === 'reply' && replyText) {
        const trigger = checkTriggers(replyText, conversation);
        if (trigger) {
            addLog('TRIGGER_ACTION', `🎯 Executando gatilho: ${trigger.name}`, { phoneKey, autoBlock: trigger.auto_block });

            if (trigger.auto_block) {
                const sticky = stickyInstances.get(phoneKey);
                if (sticky) await blockContact(conversation.remoteJid, sticky);
                db.addToBlacklist(phoneKey, conversation.remoteJid, `Gatilho: ${trigger.name}`);
                sendSSE('lead_blocked', { phoneKey, reason: trigger.name });
            }

            if (!trigger.target_funnel_id || trigger.target_funnel_id === 'ENCERRAR') {
                conversation.canceled = true; conversation.canceledAt = new Date();
                conversation.cancelReason = trigger.name;
                conversations.set(phoneKey, conversation);
                addLog('TRIGGER_STOP', `🛑 Fluxo encerrado por gatilho`, { phoneKey });
                return;
            }

            conversation.funnelId = trigger.target_funnel_id;
            conversation.stepIndex = 0;
            conversation.waiting_for_response = false;
            conversation.lastReply = new Date();
            conversations.set(phoneKey, conversation);
            await sendStep(phoneKey);
            return;
        }
    }

    const funnel = db.getFunnelById(conversation.funnelId);
    if (!funnel) return;

    const nextStepIndex = conversation.stepIndex + 1;

    if (nextStepIndex >= funnel.steps.length) {
        conversation.waiting_for_response = false;
        conversation.completed = true;
        conversation.completedAt = new Date();
        conversations.set(phoneKey, conversation);
        convToDb(phoneKey, conversation);
        if (conversation.abFunnelVariant) db.recordABResult(conversation.abFunnelVariant, false);
        addLog('FUNNEL_DONE', `✅ Funil concluído`, { phoneKey });
        sendSSE('funnel_completed', { phoneKey, customerName: conversation.customerName });
        return;
    }

    conversation.stepIndex = nextStepIndex;
    if (reason === 'reply') { conversation.lastReply = new Date(); conversation.waiting_for_response = false; }
    conversations.set(phoneKey, conversation);
    addLog('STEP_NEXT', `➡️ Passo ${nextStepIndex + 1}/${funnel.steps.length}`, { phoneKey, reason });
    await sendStep(phoneKey);
}

// ============ VERIFICAÇÃO DE INSTÂNCIAS ============
async function checkInstancesHealth() {
    const instances = db.getInstances();
    let changed = false;
    for (const inst of instances) {
        if (inst.paused) continue;
        if (!inst.name || !inst.name.trim()) continue; // ignora instâncias sem nome válido
        const connected = await checkInstanceConnected(inst.name);
        if (connected !== !!inst.connected) {
            db.setInstanceConnected(inst.name, connected);
            changed = true;
            if (!connected) {
                // Monta info de identificação (celular físico/chip/número)
                const idParts = [];
                if (inst.device_name) idParts.push(`📱 ${inst.device_name}`);
                if (inst.device_slot) idParts.push(`🔹 ${inst.device_slot}`);
                if (inst.phone_number) idParts.push(`📞 ${inst.phone_number}`);
                if (inst.account_type) idParts.push(`(${inst.account_type})`);
                const idText = idParts.length ? '\n' + idParts.join(' · ') : '';

                addLog('INSTANCE_DOWN', `🔴 ${inst.name} caiu!${idText ? ' ' + idParts.join(' · ') : ''}`);
                sendSSE('instance_down', { name: inst.name });
                if (!inst.is_notification) {
                    await sendNotification(`🔴 Instância ${inst.name} caiu!${idText}\n⚠️ Verifique o celular fisicamente`);
                    await sendPushNotification(`🔴 ${inst.name} Caiu`, idParts.join(' · ') || 'Verifique o celular', 'instance_down');
                }
            } else {
                addLog('INSTANCE_UP', `🟢 ${inst.name} voltou!`);
                sendSSE('instance_up', { name: inst.name });
                if (!inst.is_notification) {
                    await sendNotification(`🟢 Instância ${inst.name} voltou!`);
                    await sendPushNotification(`🟢 Instância Voltou`, `${inst.name} está online novamente`, 'instance_up');
                }
            }
        }
        // Alerta de sobrecarga
        if (connected && !inst.is_notification && activeInstancesCache.length > 1) {
            const today = new Date().toISOString().split('T')[0];
            const stats = db.getInstanceStats(1).filter(s => s.date === today);
            const instStats = stats.find(s => s.instance === inst.name);
            const avgMessages = stats.reduce((a, s) => a + s.messages_sent, 0) / (stats.length || 1);
            if (instStats && instStats.messages_sent > avgMessages * 2.5 && instStats.messages_sent > 20) {
                await sendNotification(`⚠️ *INSTÂNCIA SOBRECARREGADA*\n\n📱 ${inst.name}: ${instStats.messages_sent} msgs hoje\nMédia das outras: ${Math.round(avgMessages)} msgs`);
            }
        }
    }
    if (changed) refreshInstanceCache();
}
setInterval(checkInstancesHealth, 60000);

// ============ MIDDLEWARES ============
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ============ AUTH ============
function authMiddleware(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ success: false });
    try { jwt.verify(token, JWT_SECRET); next(); } catch { res.status(401).json({ success: false }); }
}
app.post('/auth/login', (req, res) => {
    const { login, password } = req.body;
    if (login === ADMIN_LOGIN && password === ADMIN_PASSWORD) {
        res.json({ success: true, token: jwt.sign({ login }, JWT_SECRET, { expiresIn: '7d' }) });
    } else res.status(401).json({ success: false, message: 'Credenciais inválidas' });
});

// ============ SSE ============
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
        const status = String(data.status || '').toUpperCase();
        const method = String(data.payment?.method || data.payment_method || '').toUpperCase();
        const orderCode = data.sale_id || data.checkout_id || 'ORDER_' + Date.now();
        const customerName = data.customer?.name || 'Cliente';
        const customerPhone = data.customer?.phone_number || '';
        const pixCode = data.payment?.pix_url || data.payment?.checkout_url || data.payment?.payment_url || data.payment?.qrcode || null;
        const orderBumps = (data.products || []).filter(p => p.is_order_bump).map(p => p.name);
        const mainOfferId = (data.products || []).find(p => !p.is_order_bump)?.offer_id;
        const productDb = mainOfferId ? db.getProductByOfferId(mainOfferId) : null;
        const productId = productDb?.id || 'GRUPO_VIP';
        const productName = productDb?.name || 'GRUPO VIP';
        const amount = parseFloat(String(data.total_price || '0').replace(/[^0-9,.]/g, '').replace(',', '.')) || 0;
        const netValue = data.fiscal?.net_value || amount;
        const isCard = method.includes('CREDIT') || method.includes('CARD');
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';
        const isApproved = event.includes('APPROVED') || event.includes('PAID') || status === 'APPROVED';
        const isPix = method.includes('PIX') || event.includes('PIX');

        const phoneKey = normalizePhoneKey(customerPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: false, message: 'Telefone inválido' });
        if (db.isBlacklisted(phoneKey)) { addLog('BLACKLIST_BLOCK', `🚫 Bloqueado: ${phoneKey}`); return res.json({ success: true, message: 'Blacklisted' }); }

        const remoteJid = phoneToRemoteJid(customerPhone);
        registerPhoneUniversal(customerPhone, phoneKey);
        const location = db.getLocationFromPhone(customerPhone);

        addLog('KIRVANO', `${event} — ${customerName}`, { orderCode, phoneKey, productId });

        const isAbandoned = event.includes('ABANDON') || status === 'ABANDONED' || event === 'CHECKOUT_ABANDONED';
        const isRefused = event.includes('REFUSED') || event.includes('DECLINED') || event.includes('FAILED') || status === 'REFUSED' || status === 'DECLINED' || status === 'FAILED';

        if (isApproved) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv?.funnelId?.includes('_PIX')) {
                await transferPixToApproved(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, netValue, orderBumps, paymentMethod, location);
            } else {
                const pt = pixTimeouts.get(phoneKey); if (pt) { clearTimeout(pt.timeout); pixTimeouts.delete(phoneKey); }
                await startFunnel(phoneKey, remoteJid, 'APROVADA', orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, paymentMethod, location);
            }
        } else if (isRefused && isCard) {
            // Cartão recusado — dispara funil CARTAO_RECUSADO via pool de abandono
            addLog('CARD_REFUSED', `💳❌ Cartão recusado: ${customerName}`, { orderCode, phoneKey });
            await startFunnel(phoneKey, remoteJid, 'CARTAO_RECUSADO', orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, 'CREDIT_CARD', location);
        } else if (isAbandoned) {
            // Carrinho abandonado — dispara funil ABANDONO via pool de abandono
            addLog('ABANDONED', `🛒 Carrinho abandonado: ${customerName}`, { orderCode, phoneKey });
            await startFunnel(phoneKey, remoteJid, 'ABANDONO', orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, paymentMethod, location);
        } else if (isPix && event.includes('GENERATED')) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && !existingConv.canceled) return res.json({ success: true, message: 'Já existe' });
            await createPixWaitingConversation(phoneKey, remoteJid, orderCode, customerName, productId, productName, amount, netValue, pixCode, orderBumps, 'PIX', location);
        }
        res.json({ success: true, phoneKey });
    } catch (error) { addLog('KIRVANO_ERR', error.message); res.status(500).json({ success: false, error: error.message }); }
});

app.post('/webhook/perfectpay', async (req, res) => {
    try {
        const data = req.body;
        const statusEnum = parseInt(data.sale_status_enum);
        const customerName = data.customer?.full_name || 'Cliente';
        const customerPhone = (data.customer?.phone_area_code || '') + (data.customer?.phone_number || '');
        const saleAmount = (data.sale_amount || 0) / 100;
        const isCard = parseInt(data.payment_type_enum || 0) === 2;
        const paymentMethod = isCard ? 'CREDIT_CARD' : 'PIX';
        const pixCode = data.billet_url || data.pix_url || data.billet_number || null;
        const productDb = data.plan?.code ? db.getProductByOfferId(data.plan.code) : null;
        const productId = productDb?.id || 'GRUPO_VIP';
        const productName = productDb?.name || 'GRUPO VIP';
        const phoneKey = normalizePhoneKey(customerPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: false });
        if (db.isBlacklisted(phoneKey)) return res.json({ success: true });
        const remoteJid = phoneToRemoteJid(customerPhone);
        registerPhoneUniversal(customerPhone, phoneKey);
        const location = db.getLocationFromPhone(customerPhone);
        if (statusEnum === 2) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv?.funnelId?.includes('_PIX')) {
                await transferPixToApproved(phoneKey, remoteJid, data.code, customerName, productId, productName, saleAmount, saleAmount, [], paymentMethod, location);
            } else {
                const pt = pixTimeouts.get(phoneKey); if (pt) { clearTimeout(pt.timeout); pixTimeouts.delete(phoneKey); }
                await startFunnel(phoneKey, remoteJid, 'APROVADA', data.code, customerName, productId, productName, saleAmount, saleAmount, pixCode, [], paymentMethod, location);
            }
            res.json({ success: true });
        } else if (statusEnum === 1 && !isCard) {
            const existingConv = findConversationUniversal(customerPhone);
            if (existingConv && !existingConv.canceled) return res.json({ success: true });
            await createPixWaitingConversation(phoneKey, remoteJid, data.code, customerName, productId, productName, saleAmount, saleAmount, pixCode, [], 'PIX', location);
            res.json({ success: true });
        } else res.json({ success: true });
    } catch (error) { addLog('PERFECTPAY_ERR', error.message); res.status(500).json({ success: false }); }
});

app.post('/webhook/evolution', async (req, res) => {
    try {
        const data = req.body;
        const event = data.event;
        if (event && !event.includes('message')) return res.json({ success: true });
        const messageData = data.data;
        if (!messageData?.key) return res.json({ success: true });
        const remoteJid = messageData.key.remoteJid;
        if (messageData.key.fromMe) return res.json({ success: true });
        const messageText = extractMessageText(messageData.message);
        const isLid = remoteJid.includes('@lid');
        let phoneToSearch = remoteJid;
        if (isLid) {
            if (messageData.key.participant) phoneToSearch = messageData.key.participant;
            else { const mk = lidMapping.get(remoteJid); if (mk) { const mc = conversations.get(mk); if (mc) phoneToSearch = mc.remoteJid; } }
        }
        const incomingPhone = phoneToSearch.split('@')[0];
        const phoneKey = normalizePhoneKey(incomingPhone);
        if (!phoneKey || phoneKey.length !== 8) return res.json({ success: true });
        if (db.isBlacklisted(phoneKey)) return res.json({ success: true });

        const hasLock = await acquireWebhookLock(phoneKey);
        if (!hasLock) return res.json({ success: false });

        try {
            const conversation = findConversationUniversal(phoneToSearch);
            if (conversation && isLid) registerLidMapping(remoteJid, conversation.phoneKey);

            // Verifica reativação de lead antigo
            if (!conversation || conversation.canceled || conversation.completed) {
                const history = db.getCompletedConversationsByPhone(phoneKey);
                if (history.length > 0) {
                    const lastConv = history[0];
                    const daysSince = (Date.now() - new Date(lastConv.created_at).getTime()) / 86400000;
                    const reactivationDays = parseInt(process.env.REACTIVATION_DAYS || '3');
                    if (daysSince >= reactivationDays) {
                        const reactivationFunnel = process.env.REACTIVATION_FUNNEL_ID || (lastConv.product_id + '_REATIVACAO');
                        const reactivFunnel = db.getFunnelById(reactivationFunnel);
                        if (reactivFunnel) {
                            addLog('REACTIVATION', `♻️ Reativando lead antigo: ${phoneKey}`, { daysSince: Math.round(daysSince) });
                            const reactivConv = {
                                phoneKey, remoteJid: phoneToSearch,
                                funnelId: reactivationFunnel, stepIndex: 0,
                                orderCode: 'REATIV_' + Date.now(),
                                customerName: lastConv.customer_name,
                                productId: lastConv.product_id, productName: lastConv.product_name,
                                orderBumps: [], amount: 0, amountDisplay: '', netValue: 0,
                                ddd: lastConv.ddd, city: lastConv.city, state: lastConv.state,
                                waiting_for_response: false, createdAt: new Date(),
                                canceled: false, completed: false, paused: false, reactivation: true
                            };
                            conversations.set(phoneKey, reactivConv);
                            registerPhoneUniversal(phoneToSearch, phoneKey);
                            await sendStep(phoneKey);
                            return res.json({ success: true });
                        }
                    }
                }
                addLog('EVO_IGNORED', `Sem conversa ativa para ${phoneKey}`);
                return res.json({ success: true });
            }

            if (conversation.pixWaiting || conversation.paused || conversation.invalidNumber) return res.json({ success: true });
            
            // Garante que estamos usando a conversa mais atualizada da memória
            const freshConv = conversations.get(conversation.phoneKey) || conversation;
            if (!freshConv.waiting_for_response) { 
                addLog('NOT_WAITING', `⚠️ Não aguardando — ignorando (${conversation.phoneKey})`, { phoneKey }); 
                return res.json({ success: true }); 
            }
            // Atualiza referência para a conversa fresca
            Object.assign(conversation, freshConv);

            db.logMessage(phoneKey, 'in', messageText, null, null);
            db.processWordFrequency(messageText, conversation.productId);
            addLog('CLIENT_REPLY', `✅ Resposta: "${messageText.substring(0, 50)}"`, { phoneKey });
            sendSSE('client_reply', { phoneKey, text: messageText.substring(0, 100) });
            await advanceConversation(phoneKey, messageText, 'reply');
            res.json({ success: true });
        } finally { releaseWebhookLock(phoneKey); }
    } catch (error) { addLog('EVO_ERR', error.message); res.status(500).json({ success: false }); }
});

// ============ API ============
app.get('/api/dashboard', authMiddleware, (req, res) => {
    const today = db.getTodayStats();
    const allConvs = [...conversations.values()];
    const active = allConvs.filter(c => !c.canceled && !c.completed && !c.pixWaiting);
    const convRate = today.pix_generated > 0 ? ((today.pix_paid + today.card_paid) / today.pix_generated * 100).toFixed(1) : '0';
    res.json({ success: true, data: {
        active_conversations: active.filter(c => !c.waiting_for_response).length,
        waiting_responses: active.filter(c => c.waiting_for_response).length,
        pending_pix: pixTimeouts.size,
        completed_today: today.pix_paid + today.card_paid,
        pix_paid_today: today.pix_paid,
        card_paid_today: today.card_paid,
        revenue_today: today.revenue || 0,
        pix_generated_today: today.pix_generated || 0,
        conversion_rate: convRate,
        active_instances: getActiveInstances().length,
        total_instances: db.getInstances().filter(i => !i.is_notification).length,
    }});
});

app.get('/api/conversations', authMiddleware, (req, res) => {
    const list = [...conversations.entries()].map(([phoneKey, conv]) => ({
        id: phoneKey, phone: (conv.remoteJid || '').replace('@s.whatsapp.net', ''), phoneKey,
        customerName: conv.customerName, productId: conv.productId, productName: conv.productName,
        orderBumps: conv.orderBumps || [], funnelId: conv.funnelId, stepIndex: conv.stepIndex,
        amount: conv.amount, amountDisplay: conv.amountDisplay, netValue: conv.netValue,
        pixCode: conv.pixCode, paymentMethod: conv.paymentMethod,
        city: conv.city, state: conv.state, ddd: conv.ddd,
        waiting_for_response: conv.waiting_for_response, pixWaiting: conv.pixWaiting || false,
        createdAt: conv.createdAt, lastMessageAt: conv.lastSystemMessage, lastReplyAt: conv.lastReply,
        orderCode: conv.orderCode, stickyInstance: stickyInstances.get(phoneKey),
        canceled: conv.canceled || false, completed: conv.completed || false,
        hasError: conv.hasError || false, paused: conv.paused || false,
        invalidNumber: conv.invalidNumber || false, reactivation: conv.reactivation || false,
        abFunnelVariant: conv.abFunnelVariant,
        pixTimeoutRemaining: pixTimeouts.has(phoneKey) ? Math.max(0, Math.round((PIX_TIMEOUT - (Date.now() - new Date(pixTimeouts.get(phoneKey).createdAt).getTime())) / 1000)) : null
    })).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ success: true, data: list });
});

app.post('/api/conversations/:phoneKey/pause', authMiddleware, (req, res) => {
    const conv = conversations.get(req.params.phoneKey);
    if (!conv) return res.status(404).json({ success: false });
    conv.paused = req.body.paused; conversations.set(req.params.phoneKey, conv);
    addLog('CONV_PAUSE', `${req.body.paused ? '⏸️' : '▶️'} ${req.params.phoneKey}`);
    res.json({ success: true });
});

app.get('/api/logs', authMiddleware, (req, res) => res.json({ success: true, data: logs.slice(0, parseInt(req.query.limit) || 100) }));
app.get('/api/funnels', authMiddleware, (req, res) => res.json({ success: true, data: db.getFunnels() }));
app.post('/api/funnels', authMiddleware, (req, res) => {
    const funnel = req.body;
    if (!funnel.id || !funnel.name || !Array.isArray(funnel.steps)) return res.status(400).json({ success: false, error: 'id, name, steps obrigatórios' });
    funnel.steps.forEach((s, i) => { if (!s.id) s.id = 'step_' + Date.now() + '_' + i; });
    db.saveFunnel(funnel);
    addLog('FUNNEL_SAVED', `Funil salvo: ${funnel.id}`);
    res.json({ success: true, data: funnel });
});
app.post('/api/funnels/:funnelId/move-step', authMiddleware, (req, res) => {
    const funnel = db.getFunnelById(req.params.funnelId);
    if (!funnel) return res.status(404).json({ success: false });
    const from = parseInt(req.body.fromIndex), to = req.body.direction === 'up' ? from - 1 : from + 1;
    if (to < 0 || to >= funnel.steps.length) return res.status(400).json({ success: false });
    [funnel.steps[from], funnel.steps[to]] = [funnel.steps[to], funnel.steps[from]];
    db.saveFunnel(funnel); res.json({ success: true, data: funnel });
});
app.get('/api/funnels/export', authMiddleware, (req, res) => {
    const funnels = db.getFunnels();
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="orion-funnels-${new Date().toISOString().split('T')[0]}.json"`);
    res.send(JSON.stringify({ version: '1.0', exportDate: new Date().toISOString(), funnels }, null, 2));
});
app.post('/api/funnels/import', authMiddleware, (req, res) => {
    const { funnels } = req.body;
    if (!Array.isArray(funnels)) return res.status(400).json({ success: false });
    let imported = 0;
    for (const f of funnels) { if (f.id && f.name && Array.isArray(f.steps)) { db.saveFunnel(f); imported++; } }
    addLog('FUNNELS_IMPORT', `Import: ${imported} funis`);
    res.json({ success: true, imported });
});

app.get('/api/products', authMiddleware, (req, res) => res.json({ success: true, data: db.getProducts() }));
app.post('/api/products', authMiddleware, (req, res) => {
    const p = req.body;
    if (!p.id || !p.name) return res.status(400).json({ success: false });
    db.saveProduct(p); refreshInstanceCache();
    addLog('PRODUCT_SAVED', `Produto: ${p.name}`); res.json({ success: true });
});
app.post('/api/products/:id/toggle', authMiddleware, (req, res) => { db.toggleProduct(req.params.id, req.body.active); res.json({ success: true }); });
app.post('/api/products/:id/ab-funnels', authMiddleware, (req, res) => { db.updateProductABFunnels(req.params.id, req.body.ab_funnel_ids || []); res.json({ success: true }); });

app.get('/api/triggers', authMiddleware, (req, res) => res.json({ success: true, data: db.getTriggers() }));
app.post('/api/triggers', authMiddleware, (req, res) => { db.saveTrigger(req.body); res.json({ success: true }); });
app.delete('/api/triggers/:id', authMiddleware, (req, res) => { db.deleteTrigger(req.params.id); res.json({ success: true }); });

app.get('/api/blacklist', authMiddleware, (req, res) => res.json({ success: true, data: db.getBlacklist() }));
app.post('/api/blacklist/:phoneKey/remove', authMiddleware, (req, res) => { db.removeFromBlacklist(req.params.phoneKey); res.json({ success: true }); });

app.get('/api/instances', authMiddleware, (req, res) => res.json({ success: true, data: db.getInstances(), stats: db.getInstanceStats(7) }));
app.post('/api/instances/:name/pause', authMiddleware, (req, res) => {
    db.ensureInstance(req.params.name); db.setInstancePaused(req.params.name, req.body.paused);
    refreshInstanceCache(); addLog('INST_PAUSE', `${req.body.paused ? '⏸️' : '▶️'} ${req.params.name}`);
    res.json({ success: true });
});
app.post('/api/instances/:name/abandono', authMiddleware, (req, res) => {
    const name = req.params.name;
    // Não permite marcar instância de notificação como abandono
    if (name === NOTIFICATION_INSTANCE || name === 'NOTIFICACAO' || name === 'NOTIFICACOES') {
        return res.status(400).json({ success: false, error: 'Instância de notificação não pode ser de abandono' });
    }
    db.setInstanceAbandono(name, !!req.body.is_abandono);
    refreshInstanceCache();
    addLog('INST_ABANDONO', `${req.body.is_abandono ? '🛒' : '📱'} ${name} — ${req.body.is_abandono ? 'agora é de abandono' : 'voltou ao pool principal'}`);
    res.json({ success: true });
});
// Identificação física do chip/celular (pra saber qual aparelho pegar quando instância cair)
app.post('/api/instances/:name/identity', authMiddleware, (req, res) => {
    try {
        const { phone_number, device_name, device_slot, account_type } = req.body || {};
        db.updateInstanceIdentity(req.params.name, { phone_number, device_name, device_slot, account_type });
        addLog('INST_IDENTITY', `📝 ${req.params.name} identificado: ${device_name || '?'} · ${phone_number || '?'} · ${account_type || '?'}`);
        res.json({ success: true });
    } catch(e) {
        res.status(500).json({ success: false, error: e.message });
    }
});
app.post('/api/instances/:name/add', authMiddleware, (req, res) => { db.ensureInstance(req.params.name); refreshInstanceCache(); res.json({ success: true }); });
app.delete('/api/instances/:name', authMiddleware, (req, res) => {
    const name = req.params.name;
    // Não permite deletar a instância de notificação
    if (name === NOTIFICATION_INSTANCE || name === 'NOTIFICACAO' || name === 'NOTIFICACOES') {
        return res.status(400).json({ success: false, error: 'Não é possível remover instância de notificação' });
    }
    try {
        db.getDb().prepare('DELETE FROM instances WHERE name = ?').run(name);
        db.getDb().prepare('DELETE FROM instance_daily_stats WHERE instance = ?').run(name);
        // Remove sticky dessa instância
        for (const [k, v] of stickyInstances.entries()) {
            if (v === name) stickyInstances.delete(k);
        }
        refreshInstanceCache();
        addLog('INST_DELETE', `🗑️ Instância removida: ${name}`);
        res.json({ success: true });
    } catch(e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

app.get('/api/analytics', authMiddleware, (req, res) => {
    const days = parseInt(req.query.days) || 7;
    const productId = req.query.product || null;
    const fromDate = req.query.from || null;
    const toDate = req.query.to || null;
    const funnels = db.getFunnels();
    const abStats = funnels.filter(f => f.ab_leads > 0).map(f => ({ id: f.id, name: f.name, leads: f.ab_leads, conversions: f.ab_conversions, rate: f.ab_leads > 0 ? (f.ab_conversions / f.ab_leads * 100).toFixed(1) : '0' }));
    let eventStats;
    if (fromDate && toDate) {
        // Custom date range - get day by day stats
        eventStats = db.getDb().prepare(`SELECT date(created_at) as day,
            SUM(CASE WHEN type='PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
            SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN 1 ELSE 0 END) as paid,
            SUM(CASE WHEN type='PIX_PAID' THEN 1 ELSE 0 END) as pix_paid,
            SUM(CASE WHEN type='CARD_PAID' THEN 1 ELSE 0 END) as card_paid,
            SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as revenue
            FROM events WHERE date(created_at) BETWEEN ? AND ?
            GROUP BY date(created_at) ORDER BY day ASC`).all(fromDate, toDate);
    } else {
        eventStats = db.getEventStats(days);
        eventStats = eventStats.slice().reverse(); // chronological order
    }
    res.json({ success: true, data: { eventStats, topWords: db.getTopWords(productId, 30), dropoff: db.getFunnelDropoff(), instanceStats: db.getInstanceStats(days), abStats } });
});

// ============ WEB PUSH API ============
// Cria tabela de assinaturas se não existir
try {
    db.getDb().exec("CREATE TABLE IF NOT EXISTS push_subscriptions (sub_id TEXT PRIMARY KEY, subscription TEXT NOT NULL, created_at TEXT DEFAULT (datetime('now')))");
    // Restaura assinaturas salvas
    const saved = db.getDb().prepare("SELECT sub_id, subscription FROM push_subscriptions").all();
    for (const row of saved) {
        try { pushSubscriptions.set(row.sub_id, JSON.parse(row.subscription)); } catch(e){}
    }
    if (saved.length > 0) console.log(`✅ ${saved.length} assinaturas push restauradas`);
} catch(e) { console.log('Push DB erro:', e.message); }

app.get('/api/push/vapid-key', (req, res) => {
    const VAPID_PUBLIC = process.env.VAPID_PUBLIC_KEY || '';
    res.json({ publicKey: VAPID_PUBLIC });
});

app.post('/api/push/subscribe', authMiddleware, (req, res) => {
    const { subscription } = req.body;
    if (!subscription?.endpoint) return res.status(400).json({ success: false });
    const id = require('crypto').createHash('md5').update(subscription.endpoint).digest('hex');
    pushSubscriptions.set(id, subscription);
    try {
        db.getDb().prepare("INSERT OR REPLACE INTO push_subscriptions (sub_id, subscription) VALUES (?, ?)").run(id, JSON.stringify(subscription));
    } catch(e) {}
    addLog('PUSH_SUB', `📱 Nova assinatura push registrada`);
    res.json({ success: true, id });
});

app.post('/api/push/unsubscribe', authMiddleware, (req, res) => {
    const { endpoint } = req.body;
    if (!endpoint) return res.status(400).json({ success: false });
    const id = require('crypto').createHash('md5').update(endpoint).digest('hex');
    pushSubscriptions.delete(id);
    try { db.getDb().prepare("DELETE FROM push_subscriptions WHERE sub_id=?").run(id); } catch(e){}
    res.json({ success: true });
});

// ===== SETTINGS API =====
app.get('/api/settings', authMiddleware, (req, res) => {
    const defaults = {
        PIX_TIMEOUT_MS: process.env.PIX_TIMEOUT_MS || '420000',
        REACTIVATION_DAYS: process.env.REACTIVATION_DAYS || '3',
        NOTIFICATION_NUMBER: NOTIFICATION_NUMBER,
        CLEANUP_DAYS: CLEANUP_DAYS.toString(),
        HIGH_TICKET_MIN: '50',
        TAX_RATE: '0.1215',
        MAX_FUNNELS_PER_LEAD_PER_DAY: '3',
        FUNNEL_COOLDOWN_DAYS: '7'
    };
    const saved = db.getAllSettings();
    res.json({ success: true, data: { ...defaults, ...saved } });
});
app.post('/api/settings', authMiddleware, (req, res) => {
    const allowed = ['HIGH_TICKET_MIN','TAX_RATE','MAX_FUNNELS_PER_LEAD_PER_DAY','REACTIVATION_DAYS','NOTIFICATION_NUMBER','FUNNEL_COOLDOWN_DAYS'];
    for (const [key, value] of Object.entries(req.body)) {
        if (allowed.includes(key)) db.setSetting(key, value);
    }
    res.json({ success: true });
});

// ===== DAILY INVESTMENT API =====
app.get('/api/investment', authMiddleware, (req, res) => {
    const { from, to } = req.query;
    const startDate = from || new Date(Date.now() - 30*86400000).toISOString().split('T')[0];
    const endDate = to || new Date().toISOString().split('T')[0];
    const data = db.getDailyInvestmentRange(startDate, endDate);
    // Preenche dias sem dados com zeros
    const result = [];
    const current = new Date(startDate);
    const end = new Date(endDate);
    while (current <= end) {
        const dateStr = current.toISOString().split('T')[0];
        const existing = data.find(d => d.date === dateStr);
        // Pega receita automática do dia nos eventos
        const todayEvents = db.getDb().prepare("SELECT SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as rev FROM events WHERE date(created_at) = ?").get(dateStr);
        const autoRev = todayEvents?.rev || existing?.auto_revenue || 0;
        result.push(existing ? { ...existing, auto_revenue: autoRev } : { date: dateStr, facebook_spend: 0, extra_revenue: 0, auto_revenue: autoRev, tax_rate: 0.1215, tax_amount: 0, total_cost: 0, total_revenue: autoRev, net_profit: autoRev, roi: 0, notes: '' });
        current.setDate(current.getDate() + 1);
    }
    res.json({ success: true, data: result });
});
app.post('/api/investment/:date', authMiddleware, (req, res) => {
    const { date } = req.params;
    const { facebook_spend, extra_revenue, notes, tax_rate } = req.body;
    // Pega receita automática do dia
    const todayEvents = db.getDb().prepare("SELECT SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as rev FROM events WHERE date(created_at) = ?").get(date);
    const autoRev = todayEvents?.rev || 0;
    const result = db.saveDailyInvestment({ date, facebook_spend: parseFloat(facebook_spend)||0, extra_revenue: parseFloat(extra_revenue)||0, auto_revenue: autoRev, tax_rate: parseFloat(tax_rate)||0.1215, notes });
    res.json({ success: true, data: result });
});

// ===== INSTANCE HEALTH API =====
app.get('/api/instances/health', authMiddleware, (req, res) => {
    res.json({ success: true, data: db.getInstanceHealth() });
});

// ===== PHONE VARIATIONS API =====
app.get('/api/phone-variations', authMiddleware, (req, res) => {
    const rows = db.getDb().prepare('SELECT * FROM phone_variation_log ORDER BY id DESC LIMIT 100').all();
    res.json({ success: true, data: rows });
});

// ===== FUNNEL METRICS API =====
app.get('/api/funnel-metrics', authMiddleware, (req, res) => {
    const days = parseInt(req.query.days) || 30;
    const d = db.getDb();
    const since = `datetime('now', '-${days} days')`;
    
    const total = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE datetime(created_at) > ${since}`).get().n || 0;
    const completed = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE completed=1 AND datetime(created_at) > ${since}`).get().n || 0;
    const invalidNumber = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE invalid_number=1 AND datetime(created_at) > ${since}`).get().n || 0;
    const pixReceived = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE funnel_id LIKE '%_PIX%' AND datetime(created_at) > ${since}`).get().n || 0;
    const pixPaid = d.prepare(`SELECT COUNT(*) as n FROM events WHERE type IN ('PIX_PAID','CARD_PAID') AND datetime(created_at) > ${since}`).get().n || 0;
    const stoppedMid = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE canceled=1 AND completed=0 AND invalid_number=0 AND step_index > 0 AND datetime(created_at) > ${since}`).get().n || 0;
    const neverReplied = d.prepare(`SELECT COUNT(*) as n FROM conversations WHERE canceled=1 AND completed=0 AND step_index <= 1 AND datetime(created_at) > ${since}`).get().n || 0;
    
    const pct = (n, t) => t > 0 ? ((n/t)*100).toFixed(1) : '0.0';
    
    res.json({ success: true, data: {
        total, completed, invalidNumber, pixReceived, pixPaid, stoppedMid, neverReplied,
        rates: {
            completed: pct(completed, total),
            pixPaid: pct(pixPaid, pixReceived),
            stoppedMid: pct(stoppedMid, total),
            invalidNumber: pct(invalidNumber, total),
            neverReplied: pct(neverReplied, total)
        }
    }});
});

app.post('/api/test/trigger', (req, res) => {
    const { type, phoneKey, amount, customerName } = req.body;
    addLog('TEST', `🧪 ${type}`);
    if (type === 'pix_generated') { sendSSE('pix_generated', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90' }); pixGeneratedLast2h++; }
    else if (type === 'payment_approved') { sendSSE('payment_approved', { phoneKey, customerName: customerName || 'Teste', productName: 'GRUPO VIP', amount: amount || 'R$ 29,90', paymentMethod: 'PIX' }); pixPaidLast2h++; }
    res.json({ success: true });
});

// ============ INICIALIZAÇÃO ============
app.listen(PORT, async () => {
    console.log('='.repeat(60));
    console.log('🌌 ORION v2.0 — Sistema de Automação WhatsApp');
    console.log('='.repeat(60));
    console.log(`✅ Porta: ${PORT} | Evolution: ${EVOLUTION_BASE_URL}`);
    console.log(`✅ Instâncias: ${CONFIGURED_INSTANCES.join(', ')}`);
    if (NOTIFICATION_NUMBER && NOTIFICATION_INSTANCE) {
        console.log(`✅ Notificações → ${NOTIFICATION_NUMBER} via ${NOTIFICATION_INSTANCE}`);
    } else {
        console.log(`⚠️  Notificações por WhatsApp desativadas (defina NOTIFICATION_NUMBER e NOTIFICATION_INSTANCE para ativar)`);
    }
    console.log('='.repeat(60));
    console.log('🔧 Funcionalidades v2.0:');
    console.log('  ✅ A/B Test com rotação por instância');
    console.log('  ✅ Gatilhos globais (contém/exato/similar)');
    console.log('  ✅ Blacklist global por gatilho');
    console.log('  ✅ Delay com variação aleatória ±20%');
    console.log('  ✅ Variáveis: {NOME} {SAUDACAO} {CIDADE} {ESTADO}');
    console.log('  ✅ Personalização por horário (manhã/tarde/noite)');
    console.log('  ✅ Reativação de lead antigo');
    console.log('  ✅ Notificações via WhatsApp');
    console.log('  ✅ Relatórios automáticos 8h/12h/18h/23h50');
    console.log('  ✅ Relatório semanal domingo 20h');
    console.log('  ✅ Bloqueio automático via gatilho');
    console.log('  ✅ Figurinha como bloco de funil');
    console.log('='.repeat(60));
    await checkInstancesHealth();
    restoreStickyFromDB();
    restorePendingConversations();
    restorePendingPixTimeouts();
});
