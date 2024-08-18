const WebSocket = require('ws');
const Redis = require('ioredis');

const wss = new WebSocket.Server({ port: 8080 });
const redis = new Redis();

wss.on('connection', (ws) => {
    console.log('New client connected');

    redis.subscribe(['funding:top:5:tickets', 'funding:top:5:tickets:volume'], (err, count) => {
        if (err) {
            console.error('Failed to subscribe: ', err);
        } else {
            console.log(`Subscribed to ${count} channel(s).`);
        }
    });

    redis.on('message', (channel, message) => {
        console.log(`Received message from ${channel}: ${message}`);

        ws.send(`Message from ${channel}: ${message}`);
    });

    ws.on('message', (message) => {
        console.log(`Received: ${message}`);

        ws.send(`Server received: ${message}`);
    });

    ws.on('close', () => {
        console.log('Client disconnected');
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
});

console.log('WebSocket server is running on ws://localhost:8080');
