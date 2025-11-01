export class Common {
    constructor() {

    }
    async GetDataCrypto(apiUrl, from, to, secondsPerCandle, granularity) {
        let allBars = [];
        const MAX_CANDLES = 350;
        let currentFrom = from;

        const maxRange = secondsPerCandle * MAX_CANDLES;
        while (currentFrom < to) {
            let currentTo = Math.min(currentFrom + maxRange, to);

            const url = `${apiUrl}?&start=${currentFrom}&end=${currentTo}&granularity=${granularity}`;
            const res = await fetch(url);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            if (data.candles && data.candles.length > 0) {
                // Coinbase entrega DESCENDENTE → debemos invertirlo
                const ordered = data.candles.sort((a, b) => a.start - b.start);

                const bars = ordered.map(c => ({
                    time: c.start * 1000, // en ms
                    open: parseFloat(c.open),
                    high: parseFloat(c.high),
                    low: parseFloat(c.low),
                    close: parseFloat(c.close),
                    volume: parseFloat(c.volume),
                }));


                allBars = allBars.concat(bars);
            }

            currentFrom = currentTo;
            await new Promise(r => setTimeout(r, 200));
        }
        console.log("Última vela:", allBars[allBars.length - 1]);

        return allBars;
    }
    async GetDataForex(apiUrl) {
        let allBars = [];

        const url = apiUrl;
        const testtoken = process.env.OANDA_API_KEY;
        const res = await fetch(url, {
            method: "GET",
            headers: {
                "Authorization": `Bearer ${process.env.OANDA_API_KEY}`,
                "Content-Type": "application/json"
            }
        });

        const data = await res.json();
        allBars = data.candles.map(x => ({
            time: new Date(x.time.replace("000000000", "000")).getTime(),// en ms
            open: parseFloat(x.mid.o),
            high: parseFloat(x.mid.h),
            low: parseFloat(x.mid.l),
            close: parseFloat(x.mid.c),
            volume: parseFloat(x.volume),
        }))

        return allBars;
    }
}
