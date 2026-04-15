const path = require('path');
const fs = require('fs');

// Usa better-sqlite3 (síncrono, ideal para Node.js single-thread)
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
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(product_id) REFERENCES products(id)
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
            amount TEXT,
            pix_code TEXT,
            payment_method TEXT DEFAULT 'PIX',
            waiting_for_response INTEGER DEFAULT 0,
            pix_waiting INTEGER DEFAULT 0,
            sticky_instance TEXT,
            canceled INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            has_error INTEGER DEFAULT 0,
            transferred_from_pix INTEGER DEFAULT 0,
            paused INTEGER DEFAULT 0,
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
            payment_method TEXT,
            order_code TEXT,
            order_bumps TEXT DEFAULT '[]',
            instance TEXT,
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
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS instances (
            name TEXT PRIMARY KEY,
            paused INTEGER DEFAULT 0,
            connected INTEGER DEFAULT 1,
            messages_total INTEGER DEFAULT 0,
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
            UNIQUE(instance, date)
        );

        CREATE TABLE IF NOT EXISTS daily_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE NOT NULL,
            pix_generated INTEGER DEFAULT 0,
            pix_paid INTEGER DEFAULT 0,
            card_paid INTEGER DEFAULT 0,
            total_revenue REAL DEFAULT 0,
            avg_ticket REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS word_frequency (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL,
            product_id TEXT,
            count INTEGER DEFAULT 1,
            last_seen TEXT DEFAULT (datetime('now')),
            UNIQUE(word, product_id)
        );

        CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
        CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
        CREATE INDEX IF NOT EXISTS idx_messages_phone ON messages_log(phone_key);
        CREATE INDEX IF NOT EXISTS idx_messages_created ON messages_log(created_at);
        CREATE INDEX IF NOT EXISTS idx_conv_created ON conversations(created_at);
    `);

    // Produto padrão GRUPO VIP
    const existingProduct = db.prepare('SELECT id FROM products WHERE id = ?').get('GRUPO_VIP');
    if (!existingProduct) {
        db.prepare('INSERT INTO products (id, name, active) VALUES (?, ?, 1)').run('GRUPO_VIP', 'GRUPO VIP');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', 'e79419d3-5b71-4f90-954b-b05e94de8d98', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', '06539c76-40ee-4811-8351-ab3f5ccc4437', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run('GRUPO_VIP', '564bb9bb-718a-4e8b-a843-a2da62f616f0', 'kirvano');
        db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run('GRUPO_VIP_PIX', 'GRUPO_VIP', 'PIX', 'GRUPO VIP - PIX Pendente', '[]');
        db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run('GRUPO_VIP_APROVADA', 'GRUPO_VIP', 'APROVADA', 'GRUPO VIP - Compra Aprovada', '[]');
    }

    console.log('✅ Banco de dados Orion inicializado');
    return db;
}

// ===== PRODUTOS =====
function getProducts() {
    return getDb().prepare('SELECT * FROM products ORDER BY name').all();
}

function getActiveProducts() {
    return getDb().prepare('SELECT * FROM products WHERE active = 1').all();
}

function getProductByOfferId(offerId) {
    const row = getDb().prepare(`
        SELECT p.* FROM products p
        JOIN product_offers po ON po.product_id = p.id
        WHERE po.offer_id = ? AND p.active = 1
    `).get(offerId);
    return row || null;
}

function saveProduct(product) {
    const db = getDb();
    db.prepare('INSERT OR REPLACE INTO products (id, name, active) VALUES (?, ?, ?)').run(product.id, product.name, product.active ? 1 : 0);
    if (product.offers) {
        db.prepare('DELETE FROM product_offers WHERE product_id = ?').run(product.id);
        for (const offer of product.offers) {
            db.prepare('INSERT INTO product_offers (product_id, offer_id, platform) VALUES (?, ?, ?)').run(product.id, offer.offer_id, offer.platform || 'kirvano');
        }
    }
    // Cria funis padrão se não existirem
    const pixId = product.id + '_PIX';
    const aprovadaId = product.id + '_APROVADA';
    db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run(pixId, product.id, 'PIX', product.name + ' - PIX Pendente', '[]');
    db.prepare('INSERT OR IGNORE INTO funnels (id, product_id, type, name, steps) VALUES (?, ?, ?, ?, ?)').run(aprovadaId, product.id, 'APROVADA', product.name + ' - Compra Aprovada', '[]');
}

function toggleProduct(productId, active) {
    getDb().prepare('UPDATE products SET active = ? WHERE id = ?').run(active ? 1 : 0, productId);
}

// ===== FUNIS =====
function getFunnels() {
    return getDb().prepare('SELECT * FROM funnels ORDER BY product_id, type').all().map(f => ({
        ...f,
        steps: JSON.parse(f.steps || '[]')
    }));
}

function getFunnelById(id) {
    const f = getDb().prepare('SELECT * FROM funnels WHERE id = ?').get(id);
    if (!f) return null;
    return { ...f, steps: JSON.parse(f.steps || '[]') };
}

function saveFunnel(funnel) {
    getDb().prepare('INSERT OR REPLACE INTO funnels (id, product_id, type, name, steps, updated_at) VALUES (?, ?, ?, ?, ?, datetime(\'now\'))').run(funnel.id, funnel.product_id, funnel.type, funnel.name, JSON.stringify(funnel.steps || []));
}

// ===== CONVERSAS =====
function getConversation(phoneKey) {
    const c = getDb().prepare('SELECT * FROM conversations WHERE phone_key = ?').get(phoneKey);
    if (!c) return null;
    return { ...c, order_bumps: JSON.parse(c.order_bumps || '[]') };
}

function saveConversation(conv) {
    getDb().prepare(`
        INSERT OR REPLACE INTO conversations 
        (phone_key, remote_jid, funnel_id, step_index, order_code, customer_name, 
         product_id, product_name, order_bumps, amount, pix_code, payment_method,
         waiting_for_response, pix_waiting, sticky_instance, canceled, completed, 
         has_error, transferred_from_pix, paused, created_at, last_message_at, 
         last_reply_at, completed_at, canceled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
        conv.phone_key, conv.remote_jid, conv.funnel_id, conv.step_index,
        conv.order_code, conv.customer_name, conv.product_id, conv.product_name,
        JSON.stringify(conv.order_bumps || []), conv.amount, conv.pix_code,
        conv.payment_method || 'PIX',
        conv.waiting_for_response ? 1 : 0, conv.pix_waiting ? 1 : 0,
        conv.sticky_instance, conv.canceled ? 1 : 0, conv.completed ? 1 : 0,
        conv.has_error ? 1 : 0, conv.transferred_from_pix ? 1 : 0,
        conv.paused ? 1 : 0,
        conv.created_at || new Date().toISOString(),
        conv.last_message_at, conv.last_reply_at,
        conv.completed_at, conv.canceled_at
    );
}

function getConversations(limit = 100) {
    return getDb().prepare(`
        SELECT * FROM conversations ORDER BY created_at DESC LIMIT ?
    `).all(limit).map(c => ({ ...c, order_bumps: JSON.parse(c.order_bumps || '[]') }));
}

function deleteOldConversations(days = 7) {
    const result = getDb().prepare(`
        DELETE FROM conversations 
        WHERE (completed = 1 OR canceled = 1) 
        AND datetime(created_at) < datetime('now', '-' || ? || ' days')
    `).run(days);
    return result.changes;
}

// ===== EVENTOS =====
function recordEvent(type, data) {
    getDb().prepare(`
        INSERT INTO events (type, phone_key, product_id, product_name, amount, payment_method, order_code, order_bumps, instance, extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(type, data.phone_key, data.product_id, data.product_name, data.amount, data.payment_method, data.order_code, JSON.stringify(data.order_bumps || []), data.instance, data.extra ? JSON.stringify(data.extra) : null);

    // Snapshot diário
    const today = new Date().toISOString().split('T')[0];
    if (type === 'PIX_GENERATED') {
        getDb().prepare('INSERT INTO daily_snapshots (date, pix_generated) VALUES (?, 1) ON CONFLICT(date) DO UPDATE SET pix_generated = pix_generated + 1').run(today);
    } else if (type === 'PIX_PAID') {
        getDb().prepare('INSERT INTO daily_snapshots (date, pix_paid, total_revenue) VALUES (?, 1, ?) ON CONFLICT(date) DO UPDATE SET pix_paid = pix_paid + 1, total_revenue = total_revenue + ?').run(today, data.amount || 0, data.amount || 0);
    } else if (type === 'CARD_PAID') {
        getDb().prepare('INSERT INTO daily_snapshots (date, card_paid, total_revenue) VALUES (?, 1, ?) ON CONFLICT(date) DO UPDATE SET card_paid = card_paid + 1, total_revenue = total_revenue + ?').run(today, data.amount || 0, data.amount || 0);
    }
}

function getEventStats(days = 7) {
    const rows = getDb().prepare(`
        SELECT 
            date(created_at) as day,
            SUM(CASE WHEN type = 'PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
            SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN 1 ELSE 0 END) as paid,
            SUM(CASE WHEN type = 'PIX_PAID' THEN 1 ELSE 0 END) as pix_paid,
            SUM(CASE WHEN type = 'CARD_PAID' THEN 1 ELSE 0 END) as card_paid,
            SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(amount,0) ELSE 0 END) as revenue
        FROM events
        WHERE datetime(created_at) >= datetime('now', '-' || ? || ' days')
        GROUP BY date(created_at)
        ORDER BY day DESC
    `).all(days);
    return rows;
}

function getTodayStats() {
    return getDb().prepare(`
        SELECT 
            SUM(CASE WHEN type = 'PIX_GENERATED' THEN 1 ELSE 0 END) as pix_generated,
            SUM(CASE WHEN type = 'PIX_PAID' THEN 1 ELSE 0 END) as pix_paid,
            SUM(CASE WHEN type = 'CARD_PAID' THEN 1 ELSE 0 END) as card_paid,
            SUM(CASE WHEN type IN ('PIX_PAID','CARD_PAID') THEN COALESCE(amount,0) ELSE 0 END) as revenue
        FROM events
        WHERE date(created_at) = date('now')
    `).get() || { pix_generated: 0, pix_paid: 0, card_paid: 0, revenue: 0 };
}

// ===== MENSAGENS =====
function logMessage(phoneKey, direction, content, instance, stepId) {
    getDb().prepare('INSERT INTO messages_log (phone_key, direction, content, instance, step_id) VALUES (?, ?, ?, ?, ?)').run(phoneKey, direction, content ? content.substring(0, 500) : null, instance, stepId);
    if (direction === 'in') processWordFrequency(content, null);
}

function processWordFrequency(text, productId) {
    if (!text || text.length < 2) return;
    const stopWords = new Set(['o', 'a', 'os', 'as', 'um', 'uma', 'de', 'da', 'do', 'em', 'no', 'na', 'por', 'para', 'com', 'que', 'se', 'não', 'nao', 'sim', 'ok', 'ok!', 'oi', 'ola', 'olá', 'e', 'é', 'eu', 'me', 'te', 'seu', 'sua', 'meu', 'minha']);
    const words = text.toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9\s]/g, ' ')
        .split(/\s+/)
        .filter(w => w.length >= 3 && !stopWords.has(w));

    for (const word of words) {
        getDb().prepare(`
            INSERT INTO word_frequency (word, product_id, count, last_seen) VALUES (?, ?, 1, datetime('now'))
            ON CONFLICT(word, product_id) DO UPDATE SET count = count + 1, last_seen = datetime('now')
        `).run(word, productId || 'ALL');
    }
}

function getTopWords(productId, limit = 30) {
    const query = productId && productId !== 'ALL'
        ? `SELECT word, count FROM word_frequency WHERE product_id = ? ORDER BY count DESC LIMIT ?`
        : `SELECT word, SUM(count) as count FROM word_frequency GROUP BY word ORDER BY count DESC LIMIT ?`;
    return productId && productId !== 'ALL'
        ? getDb().prepare(query).all(productId, limit)
        : getDb().prepare(query).all(limit);
}

// ===== INSTÂNCIAS =====
function ensureInstance(name) {
    getDb().prepare('INSERT OR IGNORE INTO instances (name) VALUES (?)').run(name);
}

function getInstances() {
    return getDb().prepare('SELECT * FROM instances ORDER BY name').all();
}

function updateInstanceStats(name, messagesSent = 0) {
    const today = new Date().toISOString().split('T')[0];
    getDb().prepare('UPDATE instances SET messages_total = messages_total + ?, last_seen = datetime(\'now\') WHERE name = ?').run(messagesSent, name);
    getDb().prepare(`
        INSERT INTO instance_daily_stats (instance, date, messages_sent, leads_attended) VALUES (?, ?, ?, 0)
        ON CONFLICT(instance, date) DO UPDATE SET messages_sent = messages_sent + ?
    `).run(name, today, messagesSent, messagesSent);
}

function getInstanceStats(days = 7) {
    return getDb().prepare(`
        SELECT instance, date, messages_sent, leads_attended 
        FROM instance_daily_stats 
        WHERE datetime(date) >= datetime('now', '-' || ? || ' days')
        ORDER BY date DESC, messages_sent DESC
    `).all(days);
}

function setInstancePaused(name, paused) {
    getDb().prepare('UPDATE instances SET paused = ? WHERE name = ?').run(paused ? 1 : 0, name);
}

function setInstanceConnected(name, connected) {
    const now = new Date().toISOString();
    if (connected) {
        getDb().prepare('UPDATE instances SET connected = 1, last_connected = ? WHERE name = ?').run(now, name);
    } else {
        getDb().prepare('UPDATE instances SET connected = 0, last_disconnected = ? WHERE name = ?').run(now, name);
    }
}

function getFunnelDropoff() {
    return getDb().prepare(`
        SELECT funnel_id, step_index, COUNT(*) as count
        FROM conversations
        WHERE waiting_for_response = 1 AND canceled = 0 AND completed = 0
        GROUP BY funnel_id, step_index
        ORDER BY funnel_id, step_index
    `).all();
}

module.exports = {
    initDatabase, getDb,
    getProducts, getActiveProducts, getProductByOfferId, saveProduct, toggleProduct,
    getFunnels, getFunnelById, saveFunnel,
    getConversation, saveConversation, getConversations, deleteOldConversations,
    recordEvent, getEventStats, getTodayStats,
    logMessage, processWordFrequency, getTopWords,
    ensureInstance, getInstances, updateInstanceStats, getInstanceStats,
    setInstancePaused, setInstanceConnected, getFunnelDropoff
};
