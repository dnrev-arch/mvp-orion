const path = require('path');
const fs = require('fs');

let db;

function getDb() {
    if (!db) throw new Error('Banco não inicializado');
    return db;
}

function initDatabase() {
    const Database = require('better-sqlite3');
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

    db = new Database(path.join(dataDir, 'orion.db'));
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');

    db.exec(`
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            ab_funnel_ids TEXT DEFAULT '[]',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS product_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            offer_id TEXT NOT NULL,
            platform TEXT DEFAULT 'kirvano',
            FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS funnels (
            id TEXT PRIMARY KEY,
            product_id TEXT NOT NULL,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            steps TEXT DEFAULT '[]',
            ab_enabled INTEGER DEFAULT 0,
            ab_conversions INTEGER DEFAULT 0,
            ab_leads INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            keywords TEXT NOT NULL,
            match_type TEXT DEFAULT 'contains',
            target_funnel_id TEXT,
            auto_block INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS blacklist (
            phone_key TEXT PRIMARY KEY,
            phone TEXT,
            reason TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS conversations (
            phone_key TEXT PRIMARY KEY,
            remote_jid TEXT,
            funnel_id TEXT,
            step_index INTEGER DEFAULT 0,
            order_code TEXT,
            customer_name TEXT,
            product_id TEXT,
            product_name TEXT,
            order_bumps TEXT DEFAULT '[]',
            amount REAL DEFAULT 0,
            amount_display TEXT,
            net_value REAL DEFAULT 0,
            pix_code TEXT,
            payment_method TEXT DEFAULT 'PIX',
            ddd TEXT,
            city TEXT,
            state TEXT,
            waiting_for_response INTEGER DEFAULT 0,
            pix_waiting INTEGER DEFAULT 0,
            sticky_instance TEXT,
            canceled INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            has_error INTEGER DEFAULT 0,
            invalid_number INTEGER DEFAULT 0,
            transferred_from_pix INTEGER DEFAULT 0,
            paused INTEGER DEFAULT 0,
            reactivation INTEGER DEFAULT 0,
            ab_funnel_variant TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            last_message_at TEXT,
            last_reply_at TEXT,
            completed_at TEXT,
            canceled_at TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            phone_key TEXT,
            product_id TEXT,
            product_name TEXT,
            amount REAL,
            net_value REAL,
            payment_method TEXT,
            order_code TEXT,
            order_bumps TEXT DEFAULT '[]',
            instance TEXT,
            funnel_id TEXT,
            extra TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS messages_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_key TEXT NOT NULL,
            direction TEXT NOT NULL,
            content TEXT,
            instance TEXT,
            step_id TEXT,
            delivered INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS instances (
            name TEXT PRIMARY KEY,
            paused INTEGER DEFAULT 0,
            connected INTEGER DEFAULT 1,
            is_notification INTEGER DEFAULT 0,
            messages_total INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0,
            last_seen TEXT DEFAULT (datetime('now')),
            last_disconnected TEXT,
            last_connected TEXT DEFAULT (datetime('now')),
            added_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS instance_daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance TEXT NOT NULL,
            date TEXT NOT NULL,
            messages_sent INTEGER DEFAULT 0,
            leads_attended INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0,
            UNIQUE(instance, date)
        );

        CREATE TABLE IF NOT EXISTS word_frequency (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL,
            product_id TEXT DEFAULT 'ALL',
            count INTEGER DEFAULT 1,
            last_seen TEXT DEFAULT (datetime('now')),
            UNIQUE(word, product_id)
        );

        CREATE TABLE IF NOT EXISTS notification_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            message TEXT,
            sent INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
        CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
        CREATE INDEX IF NOT EXISTS idx_messages_phone ON messages_log(phone_key);
        CREATE INDEX IF NOT EXISTS idx_conv_created ON conversations(created_at);
        CREATE INDEX IF NOT EXISTS idx_blacklist ON blacklist(phone_key);
    `);

    // Produto padrão
    const existing = db.prepare('SELECT id FROM products WHERE id = ?').get('GRUPO_VIP');
    if (!existing) {
        db.prepare('INSERT INTO products (id, name, active, ab_funnel_ids) VALUES (?, ?, 1, ?)').run('GRUPO_VIP', 'GRUPO VIP', '[]');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', 'e79419d3-5b71-4f90-954b-b05e94de8d98', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', '06539c76-40ee-4811-8351-ab3f5ccc4437', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', '564bb9bb-718a-4e8b-a843-a2da62f616f0', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run('GRUPO_VIP_PIX', 'GRUPO_VIP', 'PIX', 'GRUPO VIP - PIX Pendente', '[]');
        db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run('GRUPO_VIP_APROVADA', 'GRUPO_VIP', 'APROVADA', 'GRUPO VIP - Compra Aprovada', '[]');
    }

    // Instância de notificações
    db.prepare('INSERT OR IGNORE INTO instances (name, is_notification, paused) VALUES (?, 1, 0)').run('NOTIFICACOES');

    console.log('✅ Banco de dados Orion inicializado');
    return db;
}

// ===== DDD -> LOCALIZAÇÃO =====
const DDD_MAP = {
    '11':'São Paulo,SP','12':'São José dos Campos,SP','13':'Santos,SP','14':'Bauru,SP','15':'Sorocaba,SP',
    '16':'Ribeirão Preto,SP','17':'São José do Rio Preto,SP','18':'Presidente Prudente,SP','19':'Campinas,SP',
    '21':'Rio de Janeiro,RJ','22':'Campos dos Goytacazes,RJ','24':'Volta Redonda,RJ',
    '27':'Vitória,ES','28':'Cachoeiro de Itapemirim,ES',
    '31':'Belo Horizonte,MG','32':'Juiz de Fora,MG','33':'Governador Valadares,MG','34':'Uberlândia,MG',
    '35':'Poços de Caldas,MG','37':'Divinópolis,MG','38':'Montes Claros,MG',
    '41':'Curitiba,PR','42':'Ponta Grossa,PR','43':'Londrina,PR','44':'Maringá,PR','45':'Foz do Iguaçu,PR','46':'Francisco Beltrão,PR',
    '47':'Joinville,SC','48':'Florianópolis,SC','49':'Chapecó,SC',
    '51':'Porto Alegre,RS','53':'Pelotas,RS','54':'Caxias do Sul,RS','55':'Santa Maria,RS',
    '61':'Brasília,DF','62':'Goiânia,GO','63':'Palmas,TO','64':'Rio Verde,GO','65':'Cuiabá,MT','66':'Rondonópolis,MT','67':'Campo Grande,MS','68':'Rio Branco,AC','69':'Porto Velho,RO',
    '71':'Salvador,BA','73':'Ilhéus,BA','74':'Juazeiro,BA','75':'Feira de Santana,BA','77':'Vitória da Conquista,BA',
    '79':'Aracaju,SE',
    '81':'Recife,PE','82':'Maceió,AL','83':'João Pessoa,PB','84':'Natal,RN','85':'Fortaleza,CE','86':'Teresina,PI',
    '87':'Petrolina,PE','88':'Juazeiro do Norte,CE','89':'Picos,PI',
    '91':'Belém,PA','92':'Manaus,AM','93':'Santarém,PA','94':'Marabá,PA','95':'Boa Vista,RR','96':'Macapá,AP','97':'Coari,AM','98':'São Luís,MA','99':'Imperatriz,MA'
};

function getLocationFromPhone(phone) {
    const cleaned = String(phone).replace(/\D/g, '');
    let ddd = '';
    if (cleaned.startsWith('55') && cleaned.length >= 4) ddd = cleaned.substring(2, 4);
    else if (cleaned.length >= 2) ddd = cleaned.substring(0, 2);

    if (DDD_MAP[ddd]) {
        const [city, state] = DDD_MAP[ddd].split(',');
        return { ddd, city, state };
    }
    return { ddd, city: '', state: '' };
}

// ===== PRODUTOS =====
function getProducts() { return getDb().prepare('SELECT * FROM products ORDER BY name').all(); }
function getActiveProducts() { return getDb().prepare('SELECT * FROM products WHERE active = 1').all(); }
function getProductByOfferId(offerId) {
    return getDb().prepare('SELECT p.* FROM products p JOIN product_offers po ON po.product_id = p.id WHERE po.offer_id = ? AND p.active = 1').get(offerId) || null;
}
function saveProduct(product) {
    const d = getDb();
    d.prepare('INSERT OR REPLACE INTO products (id, name, active, ab_funnel_ids) VALUES (?, ?, ?, ?)').run(product.id, product.name, product.active ? 1 : 0, JSON.stringify(product.ab_funnel_ids || []));
    if (product.offers) {
        d.prepare('DELETE FROM product_offers WHERE product_id = ?').run(product.id);
        for (const o of product.offers) d.prepare('INSERT INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run(product.id, o.offer_id, o.platform || 'kirvano');
    }
    d.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run(product.id + '_PIX', product.id, 'PIX', product.name + ' - PIX Pendente', '[]');
    d.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run(product.id + '_APROVADA', product.id, 'APROVADA', product.name + ' - Compra Aprovada', '[]');
}
function toggleProduct(productId, active) { getDb().prepare('UPDATE products SET active = ? WHERE id = ?').run(active ? 1 : 0, productId); }
function updateProductABFunnels(productId, abFunnelIds) { getDb().prepare('UPDATE products SET ab_funnel_ids = ? WHERE id = ?').run(JSON.stringify(abFunnelIds), productId); }

// ===== FUNIS =====
function getFunnels() {
    return getDb().prepare('SELECT * FROM funnels ORDER BY product_id, type').all().map(f => ({ ...f, steps: JSON.parse(f.steps || '[]') }));
}
function getFunnelById(id) {
    const f = getDb().prepare('SELECT * FROM funnels WHERE id = ?').get(id);
    return f ? { ...f, steps: JSON.parse(f.steps || '[]') } : null;
}
function saveFunnel(funnel) {
    getDb().prepare("INSERT OR REPLACE INTO funnels (id, product_id, type, name, steps, ab_enabled, updated_at) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))").run(funnel.id, funnel.product_id || 'GRUPO_VIP', funnel.type || 'PIX', funnel.name, JSON.stringify(funnel.steps || []), funnel.ab_enabled ? 1 : 0);
}
function recordABResult(funnelId, converted) {
    const field = converted ? 'ab_conversions = ab_conversions + 1, ab_leads = ab_leads + 1' : 'ab_leads = ab_leads + 1';
    getDb().prepare(`UPDATE funnels SET ${field} WHERE id = ?`).run(funnelId);
}

// ===== GATILHOS =====
function getTriggers() { return getDb().prepare('SELECT * FROM triggers WHERE active = 1 ORDER BY id').all(); }
function saveTrigger(trigger) {
    if (trigger.id) {
        getDb().prepare('UPDATE triggers SET name=?, keywords=?, match_type=?, target_funnel_id=?, auto_block=?, active=? WHERE id=?').run(trigger.name, trigger.keywords, trigger.match_type || 'contains', trigger.target_funnel_id || null, trigger.auto_block ? 1 : 0, trigger.active ? 1 : 0, trigger.id);
    } else {
        getDb().prepare('INSERT INTO triggers (name, keywords, match_type, target_funnel_id, auto_block, active) VALUES (?, ?, ?, ?, ?, 1)').run(trigger.name, trigger.keywords, trigger.match_type || 'contains', trigger.target_funnel_id || null, trigger.auto_block ? 1 : 0);
    }
}
function deleteTrigger(id) { getDb().prepare('DELETE FROM triggers WHERE id = ?').run(id); }

// ===== BLACKLIST =====
function isBlacklisted(phoneKey) { return !!getDb().prepare('SELECT 1 FROM blacklist WHERE phone_key = ?').get(phoneKey); }
function addToBlacklist(phoneKey, phone, reason) { getDb().prepare('INSERT OR IGNORE INTO blacklist (phone_key, phone, reason) VALUES (?, ?, ?)').run(phoneKey, phone, reason); }
function getBlacklist() { return getDb().prepare('SELECT * FROM blacklist ORDER BY created_at DESC').all(); }
function removeFromBlacklist(phoneKey) { getDb().prepare('DELETE FROM blacklist WHERE phone_key = ?').run(phoneKey); }

// ===== CONVERSAS =====
function getConversation(phoneKey) {
    const c = getDb().prepare('SELECT * FROM conversations WHERE phone_key = ?').get(phoneKey);
    return c ? { ...c, order_bumps: JSON.parse(c.order_bumps || '[]') } : null;
}
function saveConversation(conv) {
    getDb().prepare(`INSERT OR REPLACE INTO conversations 
        (phone_key, remote_jid, funnel_id, step_index, order_code, customer_name, product_id, product_name,
         order_bumps, amount, amount_display, net_value, pix_code, payment_method, ddd, city, state,
         waiting_for_response, pix_waiting, sticky_instance, canceled, completed, has_error, invalid_number,
         transferred_from_pix, paused, reactivation, ab_funnel_variant, created_at, last_message_at, last_reply_at, completed_at, canceled_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(conv.phone_key, conv.remote_jid, conv.funnel_id, conv.step_index, conv.order_code, conv.customer_name,
        conv.product_id, conv.product_name, JSON.stringify(conv.order_bumps || []),
        conv.amount || 0, conv.amount_display, conv.net_value || 0, conv.pix_code, conv.payment_method || 'PIX',
        conv.ddd, conv.city, conv.state,
        conv.waiting_for_response ? 1 : 0, conv.pix_waiting ? 1 : 0, conv.sticky_instance,
        conv.canceled ? 1 : 0, conv.completed ? 1 : 0, conv.has_error ? 1 : 0, conv.invalid_number ? 1 : 0,
        conv.transferred_from_pix ? 1 : 0, conv.paused ? 1 : 0, conv.reactivation ? 1 : 0,
        conv.ab_funnel_variant, conv.created_at, conv.last_message_at, conv.last_reply_at, conv.completed_at, conv.canceled_at);
}
function getConversations(limit = 200) {
    return getDb().prepare('SELECT * FROM conversations ORDER BY created_at DESC LIMIT ?').all(limit).map(c => ({ ...c, order_bumps: JSON.parse(c.order_bumps || '[]') }));
}
function getCompletedConversationsByPhone(phoneKey) {
    return getDb().prepare("SELECT * FROM conversations WHERE phone_key = ? AND (completed = 1 OR canceled = 1) ORDER BY created_at DESC").all(phoneKey);
}
function deleteOldConversations(days = 7) {
    return getDb().prepare("DELETE FROM conversations WHERE (completed=1 OR canceled=1) AND datetime(created_at) < datetime('now', '-' || ? || ' days')").run(days).changes;
}

// ===== EVENTOS =====
function recordEvent(type, data) {
    getDb().prepare('INSERT INTO events (type, phone_key, product_id, product_name, amount, net_value, payment_method, order_code, order_bumps, instance, funnel_id, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').run(type, data.phone_key, data.product_id, data.product_name, data.amount || 0, data.net_value || 0, data.payment_method, data.order_code, JSON.stringify(data.order_bumps || []), data.instance, data.funnel_id, data.extra ? JSON.stringify(data.extra) : null);
}
function getEventStats(days = 7) {
    return getDb().prepare(`SELECT date(created_at) as day,
        SUM(CASE WHEN type='PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
        SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN 1 ELSE 0 END) as paid,
        SUM(CASE WHEN type='PIX_PAID' THEN 1 ELSE 0 END) as pix_paid,
        SUM(CASE WHEN type='CARD_PAID' THEN 1 ELSE 0 END) as card_paid,
        SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as revenue
        FROM events WHERE datetime(created_at) >= datetime('now', '-' || ? || ' days')
        GROUP BY date(created_at) ORDER BY day DESC`).all(days);
}
function getTodayStats() {
    return getDb().prepare(`SELECT
        SUM(CASE WHEN type='PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
        SUM(CASE WHEN type='PIX_PAID' THEN 1 ELSE 0 END) as pix_paid,
        SUM(CASE WHEN type='CARD_PAID' THEN 1 ELSE 0 END) as card_paid,
        SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as revenue
        FROM events WHERE date(created_at) = date('now')`).get() || { pix_generated: 0, pix_paid: 0, card_paid: 0, revenue: 0 };
}
function getPeriodStats(startDate, endDate) {
    return getDb().prepare(`SELECT
        SUM(CASE WHEN type='PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
        SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN 1 ELSE 0 END) as paid,
        SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(net_value,amount,0) ELSE 0 END) as revenue
        FROM events WHERE date(created_at) BETWEEN ? AND ?`).get(startDate, endDate);
}

// ===== MENSAGENS =====
function logMessage(phoneKey, direction, content, instance, stepId, delivered = true) {
    getDb().prepare('INSERT INTO messages_log (phone_key, direction, content, instance, step_id, delivered) VALUES (?, ?, ?, ?, ?, ?)').run(phoneKey, direction, content ? content.substring(0, 500) : null, instance, stepId, delivered ? 1 : 0);
    if (direction === 'in') processWordFrequency(content, null);
}
function processWordFrequency(text, productId) {
    if (!text || text.length < 2 || text.startsWith('[')) return;
    const stopWords = new Set(['o','a','os','as','um','uma','de','da','do','em','no','na','por','para','com','que','se','não','nao','sim','ok','oi','ola','olá','e','é','eu','me','te','seu','sua','meu','minha','ai','aí','né','ne','ta','tá','tô','to']);
    const words = text.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9\s]/g, ' ').split(/\s+/).filter(w => w.length >= 3 && !stopWords.has(w));
    for (const word of words) {
        getDb().prepare("INSERT INTO word_frequency (word, product_id, count, last_seen) VALUES (?, ?, 1, datetime('now')) ON CONFLICT(word, product_id) DO UPDATE SET count = count + 1, last_seen = datetime('now')").run(word, productId || 'ALL');
    }
}
function getTopWords(productId, limit = 30) {
    return productId && productId !== 'ALL'
        ? getDb().prepare('SELECT word, count FROM word_frequency WHERE product_id = ? ORDER BY count DESC LIMIT ?').all(productId, limit)
        : getDb().prepare('SELECT word, SUM(count) as count FROM word_frequency GROUP BY word ORDER BY count DESC LIMIT ?').all(limit);
}

// ===== INSTÂNCIAS =====
function ensureInstance(name, isNotification = false) {
    getDb().prepare('INSERT OR IGNORE INTO instances (name, is_notification) VALUES (?, ?)').run(name, isNotification ? 1 : 0);
}
function getInstances() { return getDb().prepare('SELECT * FROM instances ORDER BY is_notification, name').all(); }
function updateInstanceStats(name, messagesSent = 0, converted = false) {
    const today = new Date().toISOString().split('T')[0];
    getDb().prepare("UPDATE instances SET messages_total = messages_total + ?, last_seen = datetime('now') WHERE name = ?").run(messagesSent, name);
    if (converted) getDb().prepare('UPDATE instances SET conversions = conversions + 1 WHERE name = ?').run(name);
    getDb().prepare("INSERT INTO instance_daily_stats (instance, date, messages_sent) VALUES (?, ?, ?) ON CONFLICT(instance, date) DO UPDATE SET messages_sent = messages_sent + ?").run(name, today, messagesSent, messagesSent);
    if (converted) getDb().prepare("INSERT INTO instance_daily_stats (instance, date, conversions) VALUES (?, ?, 1) ON CONFLICT(instance, date) DO UPDATE SET conversions = conversions + 1").run(name, today);
}
function getInstanceStats(days = 7) {
    return getDb().prepare('SELECT instance, date, messages_sent, leads_attended, conversions FROM instance_daily_stats WHERE datetime(date) >= datetime(\'now\', \'-\' || ? || \' days\') ORDER BY date DESC, messages_sent DESC').all(days);
}
function setInstancePaused(name, paused) { getDb().prepare('UPDATE instances SET paused = ? WHERE name = ?').run(paused ? 1 : 0, name); }
function setInstanceConnected(name, connected) {
    const now = new Date().toISOString();
    connected
        ? getDb().prepare("UPDATE instances SET connected = 1, last_connected = ? WHERE name = ?").run(now, name)
        : getDb().prepare("UPDATE instances SET connected = 0, last_disconnected = ? WHERE name = ?").run(now, name);
}
function getFunnelDropoff() {
    return getDb().prepare('SELECT funnel_id, step_index, COUNT(*) as count FROM conversations WHERE waiting_for_response = 1 AND canceled = 0 AND completed = 0 GROUP BY funnel_id, step_index ORDER BY count DESC').all();
}
function getNotificationInstance() {
    return getDb().prepare('SELECT * FROM instances WHERE is_notification = 1 AND connected = 1 LIMIT 1').get();
}

module.exports = {
    initDatabase, getDb,
    getLocationFromPhone,
    getProducts, getActiveProducts, getProductByOfferId, saveProduct, toggleProduct, updateProductABFunnels,
    getFunnels, getFunnelById, saveFunnel, recordABResult,
    getTriggers, saveTrigger, deleteTrigger,
    isBlacklisted, addToBlacklist, getBlacklist, removeFromBlacklist,
    getConversation, saveConversation, getConversations, getCompletedConversationsByPhone, deleteOldConversations,
    recordEvent, getEventStats, getTodayStats, getPeriodStats,
    logMessage, processWordFrequency, getTopWords,
    ensureInstance, getInstances, updateInstanceStats, getInstanceStats,
    setInstancePaused, setInstanceConnected, getFunnelDropoff, getNotificationInstance
};
