import WebSocket, { WebSocketServer } from "ws";
import fetch from "node-fetch";
import dotenv from "dotenv";
import cron from "node-cron";
import { RetrocesoScannerCrypto } from "./js/retrocesoScannerCrypto.js";
import { RetrocesoScannerForex } from "./js/retrocesoScannerForex.js";
import { Common } from "./js/common.js";
import fs from "fs";

dotenv.config();

const API_KEY = process.env.OANDA_API_KEY;
const ACCOUNT_ID = process.env.OANDA_ACCOUNT_ID;
const PORT = 3000;

//Cron job 
cron.schedule("*/2 * * * *", async () => {
    console.log("🕒 Cron ejecutado:", new Date().toLocaleString());
    let scannerCrypto = new RetrocesoScannerCrypto();
    let scannerForex = new RetrocesoScannerForex();
    const { GetDataCrypto, GetDataForex } = new Common();

    const listCurrencies = [
        // CRYPTO
        { type: "crypto", symbol: "BTC-USD" },
        { type: "crypto", symbol: "ETH-USD" },
        { type: "crypto", symbol: "XRP-USD" },
        // FOREX
        { type: "forex", symbol: "EUR_USD" },
        { type: "forex", symbol: "USD_JPY" },
        { type: "forex", symbol: "GBP_USD" },
        { type: "forex", symbol: "USD_CHF" },
        { type: "forex", symbol: "USD_CAD" },
        { type: "forex", symbol: "AUD_USD" },
        { type: "forex", symbol: "NZD_USD" },
        { type: "forex", symbol: "EUR_JPY" },
        { type: "forex", symbol: "GBP_JPY" },
        { type: "forex", symbol: "XAU_USD" },
    ];

    const interval = ["5", "15", "30", "60"];

    try {
        for (let currency of listCurrencies) {
            for (let inter of interval) {
                let secondsPerCandle, rangeSeconds, granularity;

                switch (inter) {
                    case "5":
                        secondsPerCandle = 300;
                        rangeSeconds = 86400; // 1 día
                        granularity = currency.type === "crypto" ? "FIVE_MINUTE" : "M5";
                        break;
                    case "15":
                        secondsPerCandle = 900;
                        rangeSeconds = 259200; // 3 días
                        granularity = currency.type === "crypto" ? "FIFTEEN_MINUTE" : "M15";
                        break;
                    case "30":
                        secondsPerCandle = 1800;
                        rangeSeconds = 604800; // 7 días
                        granularity = currency.type === "crypto" ? "THIRTY_MINUTE" : "M30";
                        break;
                    case "60":
                        secondsPerCandle = 3600;
                        rangeSeconds = 1209600; // 14 días
                        granularity = currency.type === "crypto" ? "ONE_HOUR" : "H1";
                        break;
                }

                const to = Math.floor(Date.now() / 1000);
                const from = to - rangeSeconds;

                console.log(`\n🧭 Procesando ${currency.symbol} (${currency.type}) — ${inter}m`);
                console.log(`   from: ${new Date(from * 1000).toISOString()}`);
                console.log(`   to:   ${new Date(to * 1000).toISOString()}`);

                let resultData = [];

                if (currency.type === "forex") {
                    const url = `https://api-fxpractice.oanda.com/v3/instruments/${currency.symbol}/candles?granularity=${granularity}&from=${new Date(from * 1000).toISOString()}&to=${new Date(to * 1000).toISOString()}`;
                    resultData = await GetDataForex(url);
                } else {
                    const url = `https://api.coinbase.com/api/v3/brokerage/market/products/${currency.symbol}/candles`;
                    resultData = await GetDataCrypto(url, from, to, secondsPerCandle, granularity);
                }

                const uniqueBars = Array.from(new Map(resultData.map(b => [b.time, b])).values())
                    .sort((a, b) => a.time - b.time)
                    .filter(b => b.time <= Date.now());

                const resultScanner = (currency.type === "forex")
                    ? scannerForex.fillbars(uniqueBars)
                    : scannerCrypto.fillbars(uniqueBars);

                console.log(`📊 Resultado para ${currency.symbol} (${inter}m):`, resultScanner);
            }
        }
    } catch (error) {
        console.error("❌ Error en el cron job:", error);
    }
});



// === Servidor WebSocket local ===
const wss = new WebSocketServer({ port: PORT });

console.log(`🚀 Servidor WebSocket en ws://localhost:${PORT}`);

wss.on("connection", (client) => {
    console.log("🟢 Cliente conectado");
    client.send(JSON.stringify({ message: "Conectado al servidor" }));

    let aborter = null;
    let buffer = "";

    // Esperamos el mensaje del cliente con el instrumento
    client.on("message", async (msg) => {
        try {
            const { instrument } = JSON.parse(msg);
            if (!instrument) return;

            console.log(`🎯 Solicitado instrumento: ${instrument}`);
            client.send(JSON.stringify({ message: `Suscrito a ${instrument}` }));

            // variables para control de reconexión
            let active = true;
            let retries = 0;

            // === función que inicia el stream ===
            const startStream = async () => {
                try {
                    // si había un stream anterior, cancelarlo
                    if (aborter) {
                        aborter.abort();
                        aborter = null;
                    }
                    buffer = "";
                    aborter = new AbortController();

                    const url = `https://stream-fxpractice.oanda.com/v3/accounts/${ACCOUNT_ID}/pricing/stream?instruments=${instrument}`;
                    const res = await fetch(url, {
                        headers: {
                            Authorization: `Bearer ${API_KEY}`,
                            Connection: "keep-alive",
                            "Accept-Datetime-Format": "RFC3339",
                        },
                        signal: aborter.signal,
                    });

                    if (!res.ok || !res.body) {
                        throw new Error(`HTTP ${res.status}`);
                    }

                    console.log(`🟢 Stream iniciado para ${instrument}`);
                    retries = 0; // reinicia contador de reintentos

                    for await (const chunk of res.body) {
                        buffer += chunk.toString("utf8");
                        const lines = buffer.split(/\r?\n/);
                        buffer = lines.pop() ?? "";

                        for (const line of lines) {
                            const text = line.trim();
                            if (!text || text[0] !== "{") continue;

                            let data;
                            try {
                                data = JSON.parse(text);
                            } catch {
                                continue;
                            }

                            if (data.type === "PRICE") {
                                const bid = parseFloat(data.bids[0].price);
                                const ask = parseFloat(data.asks[0].price);
                                const mid = (bid + ask) / 2;

                                const payload = {
                                    type: "PRICE",
                                    instrument: data.instrument,
                                    bid,
                                    ask,
                                    mid,
                                    time: data.time,
                                };

                                if (client.readyState === WebSocket.OPEN) {
                                    client.send(JSON.stringify(payload));
                                }
                            }
                        }
                    }

                    // si llegamos aquí, el stream terminó
                    throw new Error("Premature close");
                } catch (err) {
                    console.log("❌ Error stream:", err.message);

                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ type: "error", message: err.message }));
                    }

                    // Si el cliente sigue conectado, reintenta
                    if (active && client.readyState === WebSocket.OPEN) {
                        retries++;
                        const wait = Math.min(1000 * retries, 10000); // máximo 10s
                        console.log(`♻️ Reintentando stream en ${wait / 1000}s...`);
                        await new Promise((r) => setTimeout(r, wait));
                        startStream();
                    }
                }
            };

            // === Inicia el stream por primera vez ===
            startStream();

            // === Si el cliente se desconecta, detener stream ===
            client.on("close", () => {
                console.log("🔴 Cliente desconectado");
                active = false;
                if (aborter) aborter.abort();
            });
        } catch (err) {
            console.log("⚠️ Error al procesar mensaje:", err.message);
        }
    });
});
