'use strict';

const WebSocket = require('ws');

const WS_HEARTBEAT_INTERVAL = 5000;
const WS_RESTART_TIMEOUT = 1000; // initial 1s wait before reconnect

/**
 *
 */
class WebsocketController {
    /**
     * Creates a new WebsocketController instance.
     *
     * @param {object} adapter - The ioBroker adapter instance.
     */
    constructor(adapter) {
        this.adapter = adapter;

        // FIX: Instanz-Properties statt Modul-globaler Variablen
        this.wsClient = null;
        this.ping = null;
        this.pingTimeout = null;
        this._noReconnect = false;  // Flag: Kein Reconnect bei gewolltem Close
        this._everConnected = false; // true nach erstem erfolgreichen Verbindungsaufbau
        this._hadSuccessfulPong = false; // true nach erstem erfolgreichen Pong
        this._missedPongs = 0; // Zähler für ausgebliebene Pongs (erst nach 5 trennen)
        this._allowRetry = false; // true wenn Retry auch ohne erfolgreichen Pong erlaubt ist (energy-toggle)
        this._retryCount = 0;
        this.autoRestartTimeout = null;
        this._maxMissedPongs = (this.adapter.config && this.adapter.config.waitForPong) || 5;
    }

    /**
     *
     */
    initWsClient() {
        try {
            let wsURL = `ws://${this.adapter.config.wsServerIP}:${this.adapter.config.wsServerPort}/websocket`;

            this.wsClient = new WebSocket(wsURL, { rejectUnauthorized: false });

            this.wsClient.on('open', () => {
                this.sendPingToServer();
                this.wsHeartbeat();
            });

            this.wsClient.on('pong', () => {
                clearTimeout(this.pingTimeout);
                clearTimeout(this.ping);
                this._hadSuccessfulPong = true;
                this._missedPongs = 0; // Pong erhalten → Zähler zurücksetzen
                this.adapter.setStateChanged('info.online', true, true);
                if (!this._noReconnect) {
                    this.wsHeartbeat();
                }
            });

            this.wsClient.on('close', () => {
                clearTimeout(this.pingTimeout);
                clearTimeout(this.ping);
                this.autoRestart().catch(e => this.adapter.log.error(`autoRestart error: ${e.message}`));
            });

            this.wsClient.on('error', (err) => {
                // FIX: err.message statt komplettes Objekt loggen
                this.adapter.log.warn(`<anycubic> WebSocket error: ${err.message}`);
                this.adapter.setStateChanged('info.online', false, true);
            });

            return this.wsClient;
        } catch (err) {
            this.adapter.log.error(`<anycubic> initWsClient failed: ${err.message}`);
            return null;
        }
    }

    /**
     * @param {object} subscribeObjects
     * @param {function} onMessageCallback
     */
    start(subscribeObjects, onMessageCallback, allowRetry = false) {
        this._noReconnect = false;  // Reset für neue Verbindung
        this._everConnected = false; // Start als "noch nie verbunden" markieren
        this._hadSuccessfulPong = false; // Start als "noch nie erfolgreicher Pong" markieren
        this._missedPongs = 0; // Reset für neue Verbindung
        this._allowRetry = allowRetry; // Reset für neuen Verbindungsversuch
        this._subscribeObjects = subscribeObjects;
        this._onMessageCallback = onMessageCallback;

        const wsClient = this.initWsClient();
        if (!wsClient) {
            this.adapter.log.error('<anycubic> initWsClient returned null — websocket not started.');
            return;
        }

        wsClient.on('open', () => {
            this.adapter.log.info('Connect to anycubic over websocket connection.');
            this.send(JSON.stringify({
                jsonrpc: "2.0",
                method: "printer.objects.subscribe",
                params: {
                    objects: this._subscribeObjects
                },
                id: 102
            }));
            this._everConnected = true; // Erfolgreich verbunden gewesen
        });

        wsClient.on('message', (message) => {
            if (this._onMessageCallback) {
                this._onMessageCallback(message);
            }
        });

        wsClient.on('close', async () => {
            this.adapter.setStateChanged('info.online', false, true);
            this.adapter.log.info('Websocket connection closed.');
        });
    }

    /**
     *
     * @param message
     */
    send(message) {
        // FIX: Null-Check für wsClient
        if (!this.wsClient || this.wsClient.readyState !== WebSocket.OPEN) {
            this.adapter.log.warn('<anycubic> Cannot send message, no open websocket connection.');
            return;
        }
        this.wsClient.send(message);
    }

    /**
     * Sends a WebSocket ping to the server and schedules the next ping.
     */
    sendPingToServer() {
        if (!this.wsClient || this.wsClient.readyState !== WebSocket.OPEN) {
            return;
        }
        this.wsClient.ping();
        this.ping = setTimeout(() => {
            this.sendPingToServer();
        }, WS_HEARTBEAT_INTERVAL);
    }

    /**
     * Checks if a pong was missed; after 5 consecutive missed pongs the connection is terminated.
     */
    _checkPongTimeout() {
        if (this._noReconnect) {
            return;
        }

        this._missedPongs++;

        if (this._missedPongs >= this._maxMissedPongs) {
            this.adapter.log.warn(`<anycubic> ${this._missedPongs} consecutive missed pongs - printer unreachable.`);
            if (this.wsClient) {
                this.wsClient.terminate();
            }
            return;
        }

        this.adapter.log.debug(`<anycubic> Missed pong ${this._missedPongs}/${this._maxMissedPongs}. Retrying...`);
        this.sendPingToServer();
        this.pingTimeout = setTimeout(() => this._checkPongTimeout(), 8000);
    }

    /**
     * Resets the heartbeat timeout; checks for missed pongs instead of terminating immediately.
     */
    wsHeartbeat() {
        clearTimeout(this.pingTimeout);
        this.pingTimeout = setTimeout(() => this._checkPongTimeout(), 8000);
    }

    /**
     * Closes the WebSocket connection if it is currently open.
     */
    closeConnection() {
        this._noReconnect = true;
        this._everConnected = false; // Bei absichtlichem Close zurücksetzen
        this._hadSuccessfulPong = false; // Bei absichtlichem Close zurücksetzen
        this._missedPongs = 0; // Bei absichtlichem Close zurücksetzen
        this.allTimerClear();  // Timer sofort löschen!
        if (this.wsClient && this.wsClient.readyState !== WebSocket.CLOSED) {
            this.wsClient.close();
        }
        this.wsClient = null;  // Referenz löschen
    }

    /**
     * Clears all active timers (ping, pingTimeout).
     */
    allTimerClear() {
        clearTimeout(this.pingTimeout);
        clearTimeout(this.ping);
    }

    /**
     * Auto-Restart with exponential backoff. Reconnects only if
     * - not intentionally closed (_noReconnect)
     * - was ever successfully connected (_everConnected)
     */
    async autoRestart() {
        // Bei absichtlichem Close (energy=false) nicht reconnecten
        if (this._noReconnect) {
            this._noReconnect = false;
            this._everConnected = false;
            this._retryCount = 0;
            return;
        }

        // Noch nie erfolgreicher Pong (Start mit offline-Drucker) → nicht reconnecten
        // Ausnahme: _allowRetry=true (energy-toggle, Drucker bootet noch)
        if (!this._hadSuccessfulPong && !this._allowRetry) {
            this.adapter.log.warn('<anycubic> Printer unreachable on startup. No automatic retry. Use energy toggle to reconnect.');
            return;
        }

        // === ENERGY CHECK: Nur reconnecten wenn energy_id = true ===
        if (this.adapter.config.energy_id) {
            try {
                const energyState = await this.adapter.getStateAsync(this.adapter.config.energy_id);
                if (energyState && energyState.val === false) {
                    this.adapter.log.info('<anycubic> Energy is off - not reconnecting websocket.');
                    this._retryCount = 0;
                    return;
                }
            } catch (e) {
                this.adapter.log.warn(`<anycubic> Could not check energy state: ${e.message}`);
            }
        }

        // Hier: war mal verbunden (Betrieb), also reconnecten
        const backoffMs = Math.min(
            Math.pow(2, (this._retryCount || 0)) * 1000,
            60000 // max 60s
        );
        this._retryCount = (this._retryCount || 0) + 1;

        this.adapter.log.warn(`<anycubic> Connection lost. Reconnecting in ${backoffMs / 1000}s... (attempt ${this._retryCount})`);

        this.autoRestartTimeout = setTimeout(() => {
            this.start(this._subscribeObjects, this._onMessageCallback, this._allowRetry);
        }, backoffMs);
    }
}

module.exports = {
    WebsocketController,
};
