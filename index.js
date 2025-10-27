import WebSocket, { WebSocketServer } from "ws";
import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

const API_KEY = process.env.OANDA_API_KEY;
const ACCOUNT_ID = process.env.OANDA_ACCOUNT_ID;
const PORT = 3000;

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
