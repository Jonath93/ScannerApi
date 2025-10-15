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

    // Por defecto enviamos un mensaje de bienvenida
    client.send(JSON.stringify({ message: "Conectado al servidor" }));

    // Escuchamos cuando el cliente envía el instrumento
    client.on("message", async (msg) => {
        const { instrument } = JSON.parse(msg);
        if (!instrument) return;

        console.log(`🎯 Solicitado instrumento: ${instrument}`);
        client.send(JSON.stringify({ message: `Suscrito a ${instrument}` }));

        // Nos conectamos al stream de OANDA para ese instrumento
        const url = `https://stream-fxpractice.oanda.com/v3/accounts/${ACCOUNT_ID}/pricing/stream?instruments=${instrument}`;
        const res = await fetch(url, {
            headers: { Authorization: `Bearer ${API_KEY}` },
        });

        // Enviamos los datos al cliente mientras la conexión esté abierta
        try {
            for await (const chunk of res.body) {
                const text = chunk.toString().trim();
                console.log(text);
                if (!text.startsWith("{")) continue;

                const data = JSON.parse(text);
                console.log(data);
                if (data.type === "PRICE") {
                    const bid = parseFloat(data.bids[0].price);
                    const ask = parseFloat(data.asks[0].price);
                    const mid = (bid + ask) / 2;

                    const payload = {
                        instrument: data.instrument,
                        bid,
                        ask,
                        mid,
                        time: data.time,
                        type: data.type
                    };

                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify(payload));
                    }
                }
            }
        } catch (err) {
            console.log("❌ Error stream:", err.message);
        }
    });

    client.on("close", () => console.log("🔴 Cliente desconectado"));
});
