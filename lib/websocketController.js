'use strict';

const WebSocket = require('ws');
const utils = require('./utils');

// Source key used for info.dataError bookkeeping in this module
const ERR_SOURCE_WS = 'websocket';

// Interval between two pings on a healthy connection
const WS_HEARTBEAT_INTERVAL = 5000;
// Max time to wait for a pong after a ping has been sent
const WS_PONG_TIMEOUT = 8000;
// Base delay for the exponential reconnect backoff (1s, 2s, 4s, ... 60s)
const WS_RESTART_TIMEOUT = 1000;
// Upper limit for the reconnect backoff
const WS_MAX_RESTART_TIMEOUT = 60000;
// Handshake timeout for newly created WebSocket connections
const WS_CONNECT_TIMEOUT = 10000;
// Default timeout for JSON-RPC requests sent via request()
const WS_REQUEST_TIMEOUT = 10000;
// Fixed JSON-RPC id of the printer.objects.subscribe request
const SUBSCRIBE_REQUEST_ID = 102;
// First id used by the monotonic counter for request() (JSON-RPC ids)
const REQUEST_ID_START = 200;

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

        // Instanz-Properties statt Modul-globaler Variablen
        this.wsClient = null;
        this.ping = null; // Timer: nächster Ping
        this.pingTimeout = null; // Timer: Warten auf Pong des zuletzt gesendeten Pings
        this.autoRestartTimeout = null;
        this._noReconnect = false; // Flag: Kein Reconnect bei gewolltem Close
        this._everConnected = false; // true nach erstem erfolgreichen Verbindungsaufbau
        this._hadSuccessfulPong = false; // true nach erstem erfolgreichen Pong
        this._missedPongs = 0; // Zähler für ausgebliebene Pongs (erst nach N trennen)
        this._allowRetry = false; // true wenn Retry auch ohne erfolgreichen Pong gewünscht ist (energy-toggle)
        this._retryCount = 0;
        this._maxMissedPongs = (this.adapter.config && this.adapter.config.waitForPong) || 5;

        // Offene JSON-RPC Requests: id -> { resolve, reject, timer, method }
        this._pendingRequests = new Map();
        this._requestId = REQUEST_ID_START;

        this._subscribeObjects = null;
        this._onMessageCallback = null;
    }

    /**
     * True if the underlying socket is open and messages can be sent.
     *
     * @returns {boolean} True when the WebSocket is open.
     */
    isOpen() {
        return !!this.wsClient && this.wsClient.readyState === WebSocket.OPEN;
    }

    /**
     * Creates the WebSocket client and registers the low level handlers.
     *
     * @returns {WebSocket|null} The created client or null on failure.
     */
    initWsClient() {
        try {
            const wsURL = `ws://${this.adapter.config.wsServerIP}:${this.adapter.config.wsServerPort}/websocket`;

            const wsClient = new WebSocket(wsURL, {
                rejectUnauthorized: false,
                handshakeTimeout: WS_CONNECT_TIMEOUT,
            });
            this.wsClient = wsClient;

            // No 'open' listener here — consolidated into start() to avoid duplicate listeners.
            // info.connection=true is only set on a successful pong response.

            wsClient.on('pong', () => {
                if (this.wsClient !== wsClient) {
                    return; // stale socket
                }
                // Pong erhalten: Pong-Timeout abbrechen, Zähler zurücksetzen,
                // Backoff zurücksetzen und den nächsten Ping planen.
                clearTimeout(this.pingTimeout);
                this.pingTimeout = null;
                this._hadSuccessfulPong = true;
                this._missedPongs = 0;
                this._retryCount = 0; // Verbindung ist gesund → Backoff wieder bei 1s beginnen
                this.adapter.setStateChanged('info.connection', true, true);
                if (!this._noReconnect) {
                    this.wsHeartbeat();
                }
            });

            wsClient.on('close', () => {
                if (this.wsClient !== wsClient) {
                    return; // stale socket
                }
                this.allTimerClear();
                this.wsClient = null;
                this._rejectPendingRequests('WebSocket connection closed');

                // Guard: bei absichtlichem Close keine States schreiben und nicht reconnecten
                if (this._noReconnect) {
                    return;
                }
                this.adapter.setStateChanged('info.connection', false, true);
                utils.setDataError(this.adapter, ERR_SOURCE_WS, 'WebSocket connection closed unexpectedly');
                this.autoRestart().catch(e => this.adapter.log.error(`<anycubic> autoRestart error: ${e.message}`));
            });

            wsClient.on('error', (err) => {
                if (this.wsClient !== wsClient) {
                    return; // stale socket
                }
                this.adapter.log.debug(`<anycubic> WebSocket error: ${err.message}`);
                if (this._noReconnect) {
                    return; // absichtlicher Close → keine States schreiben
                }
                this.adapter.setStateChanged('info.connection', false, true);
                utils.setDataError(this.adapter, ERR_SOURCE_WS, err.message);
                // Falls der Socket noch nicht geschlossen ist: hart beenden, damit der
                // close-Handler (und damit der Reconnect) sicher greift.
                if (wsClient.readyState !== WebSocket.CLOSED) {
                    wsClient.terminate();
                }
            });

            return wsClient;
        } catch (err) {
            this.adapter.log.error(`<anycubic> initWsClient failed: ${err.message}`);
            utils.setDataError(this.adapter, ERR_SOURCE_WS, err.message);
            return null;
        }
    }

    /**
     * Opens the websocket connection and starts the ping chain.
     *
     * @param {object} subscribeObjects - Objects passed to printer.objects.subscribe.
     * @param {Function} onMessageCallback - Called for every non-correlated incoming message.
     * @param {boolean} [allowRetry] - Hint that retrying is wanted (energy toggle / cold start).
     */
    start(subscribeObjects, onMessageCallback, allowRetry = false) {
        // Reset für neuen Verbindungsversuch (_retryCount bleibt erhalten → Backoff läuft weiter)
        this._noReconnect = false;
        this._everConnected = false;
        this._hadSuccessfulPong = false;
        this._missedPongs = 0;
        this._allowRetry = !!allowRetry;
        this._subscribeObjects = subscribeObjects;
        this._onMessageCallback = onMessageCallback;

        const wsClient = this.initWsClient();
        if (!wsClient) {
            this.adapter.log.error('<anycubic> initWsClient returned null — websocket not started, retrying.');
            this.autoRestart().catch(e => this.adapter.log.error(`<anycubic> autoRestart error: ${e.message}`));
            return;
        }

        wsClient.on('open', () => {
            if (this.wsClient !== wsClient) {
                return; // stale socket
            }
            this.adapter.log.info('Connect to anycubic over websocket connection.');
            utils.clearDataError(this.adapter, ERR_SOURCE_WS);
            this._everConnected = true;

            try {
                this.send(JSON.stringify({
                    jsonrpc: '2.0',
                    method: 'printer.objects.subscribe',
                    params: {
                        objects: this._subscribeObjects
                    },
                    id: SUBSCRIBE_REQUEST_ID
                }));
            } catch (err) {
                this.adapter.log.error(`<anycubic> subscribe failed: ${err.message}`);
                utils.setDataError(this.adapter, ERR_SOURCE_WS, err.message);
                wsClient.terminate();
                return;
            }

            // Genau eine Ping-Kette: erster Ping sofort, danach plant der Pong-Handler weiter.
            this.sendPingToServer();
        });

        wsClient.on('message', (message) => this._handleMessage(message));
    }

    /**
     * Handles an incoming websocket frame. Correlated JSON-RPC responses are
     * handed to the pending request, everything else goes to the adapter.
     *
     * @param {*} message - Raw websocket message.
     */
    _handleMessage(message) {
        let parsed = null;
        try {
            parsed = JSON.parse(typeof message === 'string' ? message : message.toString());
        } catch {
            parsed = null; // kein JSON → unverändert an den Adapter weiterreichen
        }

        if (parsed && parsed.id !== null && parsed.id !== undefined && this._pendingRequests.has(parsed.id)) {
            this._settlePendingRequest(parsed);
            return; // korrelierte Antwort nicht zusätzlich an main.js geben
        }

        if (this._onMessageCallback) {
            this._onMessageCallback(message);
        }
    }

    /**
     * Settles a pending request with the matching JSON-RPC response.
     *
     * @param {object} response - Parsed JSON-RPC response.
     */
    _settlePendingRequest(response) {
        const pending = this._pendingRequests.get(response.id);
        if (!pending) {
            return;
        }
        this._pendingRequests.delete(response.id);
        clearTimeout(pending.timer);

        if (response.error) {
            const errMsg = response.error.message || JSON.stringify(response.error);
            pending.reject(new Error(errMsg));
        } else {
            pending.resolve(response.result);
        }
    }

    /**
     * Rejects and clears all pending requests (e.g. on close/unload) so no
     * promise stays pending forever.
     *
     * @param {string} reason - Rejection reason.
     */
    _rejectPendingRequests(reason) {
        if (this._pendingRequests.size === 0) {
            return;
        }
        const count = this._pendingRequests.size;
        for (const pending of this._pendingRequests.values()) {
            clearTimeout(pending.timer);
            pending.reject(new Error(reason));
        }
        this._pendingRequests.clear();
        this.adapter.log.debug(`<anycubic> ${count} pending request(s) rejected: ${reason}`);
    }

    /**
     * Next id of the monotonic JSON-RPC request counter.
     *
     * @returns {number} Request id.
     */
    _nextRequestId() {
        return this._requestId++;
    }

    /**
     * Sends a JSON-RPC request and resolves with its result.
     * Rejects on transport error, JSON-RPC error or timeout.
     *
     * @param {string} method - JSON-RPC method (e.g. printer.gcode.script).
     * @param {object} params - Method parameters.
     * @param {number} [timeoutMs] - Timeout in ms.
     * @returns {Promise<*>} Result of the request.
     */
    request(method, params, timeoutMs = WS_REQUEST_TIMEOUT) {
        return new Promise((resolve, reject) => {
            const id = this._nextRequestId();

            const timer = setTimeout(() => {
                if (this._pendingRequests.delete(id)) {
                    reject(new Error(`Timeout after ${timeoutMs}ms waiting for response to ${method} (id=${id})`));
                }
            }, timeoutMs);

            this._pendingRequests.set(id, { resolve, reject, timer, method });

            try {
                this.send(JSON.stringify({
                    jsonrpc: '2.0',
                    method,
                    params,
                    id
                }));
            } catch (err) {
                clearTimeout(timer);
                this._pendingRequests.delete(id);
                reject(err);
            }
        });
    }

    /**
     * Sends a raw message over the websocket.
     *
     * @param {string|Buffer} message - Message to send.
     */
    send(message) {
        if (!this.isOpen()) {
            throw new Error('Cannot send message: websocket is not open.');
        }
        this.wsClient.send(message);
    }

    /**
     * Sends a WebSocket ping and arms the pong timeout for it.
     */
    sendPingToServer() {
        clearTimeout(this.ping);
        this.ping = null;

        if (!this.isOpen()) {
            return;
        }

        this.wsClient.ping();

        clearTimeout(this.pingTimeout);
        this.pingTimeout = setTimeout(() => this._checkPongTimeout(), WS_PONG_TIMEOUT);
    }

    /**
     * Called when no pong arrived for the last sent ping. Increments the counter
     * and terminates the connection once _maxMissedPongs is reached.
     */
    _checkPongTimeout() {
        this.pingTimeout = null;

        if (this._noReconnect) {
            return;
        }

        this._missedPongs++;

        if (this._missedPongs >= this._maxMissedPongs) {
            this.adapter.log.warn(`<anycubic> ${this._missedPongs} consecutive missed pongs - printer unreachable.`);
            utils.setDataError(
                this.adapter,
                ERR_SOURCE_WS,
                `Printer unreachable: ${this._missedPongs} missed pongs`
            );
            if (this.wsClient && this.wsClient.readyState !== WebSocket.CLOSED) {
                this.wsClient.terminate();
            }
            return;
        }

        this.adapter.log.debug(`<anycubic> Missed pong ${this._missedPongs}/${this._maxMissedPongs}. Retrying...`);
        this.sendPingToServer();
    }

    /**
     * Schedules the next ping (single ping chain). Called after a pong was received.
     */
    wsHeartbeat() {
        clearTimeout(this.ping);
        this.ping = setTimeout(() => this.sendPingToServer(), WS_HEARTBEAT_INTERVAL);
    }

    /**
     * Closes the WebSocket connection intentionally (no reconnect, no state writes).
     */
    closeConnection() {
        this._noReconnect = true;
        this._everConnected = false;
        this._hadSuccessfulPong = false;
        this._missedPongs = 0;
        this.allTimerClear(); // Timer sofort löschen!
        this._rejectPendingRequests('WebSocket connection closed intentionally');

        const wsClient = this.wsClient;
        this.wsClient = null;

        if (!wsClient) {
            return;
        }

        try {
            // Alle Handler entfernen, damit späte close/error-Events keine States mehr schreiben.
            wsClient.removeAllListeners();
            // ws emittiert 'error' asynchron; ein No-op-Listener verhindert einen uncaught error.
            wsClient.on('error', () => {});
            if (wsClient.readyState !== WebSocket.CLOSED && wsClient.readyState !== WebSocket.CLOSING) {
                wsClient.close();
            }
        } catch (e) {
            this.adapter.log.debug(`<anycubic> closeConnection failed: ${e.message}`);
        }
    }

    /**
     * Clears all active timers (ping, pingTimeout, autoRestart) and resets the fields.
     */
    allTimerClear() {
        clearTimeout(this.ping);
        this.ping = null;
        clearTimeout(this.pingTimeout);
        this.pingTimeout = null;
        clearTimeout(this.autoRestartTimeout);
        this.autoRestartTimeout = null;
    }

    /**
     * Checks whether the configured energy state prevents a connection.
     *
     * @returns {Promise<boolean>} True when energy is configured and switched off.
     */
    async _isEnergyOff() {
        const energyId = this.adapter.config.energy_id;
        if (!energyId) {
            return false;
        }
        try {
            const energyState = await this.adapter.getStateAsync(energyId);
            return !!energyState && energyState.val === false;
        } catch (e) {
            this.adapter.log.warn(`<anycubic> Could not check energy state: ${e.message}`);
            return false;
        }
    }

    /**
     * Auto-Restart with exponential backoff. Retries as long as the connection was
     * not closed intentionally and the energy gate (config.energy_id) is open —
     * also on cold start where the printer was off while the adapter started.
     */
    async autoRestart() {
        // Bei absichtlichem Close (energy=false) nicht reconnecten
        if (this._noReconnect) {
            this._noReconnect = false;
            this._everConnected = false;
            this._retryCount = 0;
            return;
        }

        // === ENERGY CHECK: Nur reconnecten wenn energy_id = true ===
        if (await this._isEnergyOff()) {
            this.adapter.log.info('<anycubic> Energy is off - not reconnecting websocket.');
            this._retryCount = 0; // Backoff nicht weiter hochzählen
            return;
        }

        const backoffMs = Math.min(Math.pow(2, this._retryCount || 0) * WS_RESTART_TIMEOUT, WS_MAX_RESTART_TIMEOUT);
        this._retryCount = (this._retryCount || 0) + 1;

        this.adapter.log.warn(
            `<anycubic> Connection lost. Reconnecting in ${backoffMs / 1000}s... (attempt ${this._retryCount})`
        );

        clearTimeout(this.autoRestartTimeout);
        this.autoRestartTimeout = setTimeout(async () => {
            this.autoRestartTimeout = null;

            // Re-check energy_id before starting — energy may have been turned off during backoff
            if (await this._isEnergyOff()) {
                this.adapter.log.debug('<anycubic> Energy is off - not reconnecting websocket (backoff expired).');
                this._retryCount = 0;
                return;
            }

            try {
                this.start(this._subscribeObjects, this._onMessageCallback, this._allowRetry);
            } catch (e) {
                this.adapter.log.error(`<anycubic> reconnect failed: ${e.message}`);
                this.autoRestart().catch(err =>
                    this.adapter.log.error(`<anycubic> autoRestart error: ${err.message}`)
                );
            }
        }, backoffMs);
    }
}

module.exports = {
    WebsocketController,
    SUBSCRIBE_REQUEST_ID,
    WS_HEARTBEAT_INTERVAL,
    WS_PONG_TIMEOUT,
    WS_CONNECT_TIMEOUT,
    WS_REQUEST_TIMEOUT,
};
