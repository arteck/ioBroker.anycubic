'use strict';

const core = require('@iobroker/adapter-core');
const {WebsocketController} = require('./lib/websocketController');
const {Helper} = require('./lib/helper');

let obj102_done = false;

class anycubic extends core.Adapter {
    constructor(options) {
        super({
            ...options,
            name: 'anycubic',
        });

        // Instanz-State statt Modul-globale Variablen

        this.websocketController = null;
        this.subscribeParameter = {};
        this.messageParseMutex = Promise.resolve();
        this.parseOptions = {write: false};
        this.helper = new Helper(this);

        this.on('ready', () => {
            this.onReady().catch((e) => this.log.error(`onReady error: ${e}`));
        });
        this.on('stateChange', (id, state) => {
            this.onStateChange(id, state).catch((e) => this.log.error(`onStateChange error: ${e}`));
        });

        this.on('unload', this.onUnload.bind(this));

    }

    async onReady() {
        this.setStateChanged('info.connection', false, true);

        // WebSocket-Verbindung
        if (!this.config.wsServerIP) {
            this.log.warn('Please configure the Websocket connection!');
            return;
        }

        this.startWebsocket();
    }


    startWebsocket() {
        this.websocketController = new WebsocketController(this);
        const wsClient = this.websocketController.initWsClient();

        if (!wsClient) {
            this.log.error('<anycubic> initWsClient returned null — websocket not started.');
            return;
        }

        wsClient.on('open', () => {
            this.log.info('Connect to anycubic over websocket connection.');

            this.websocketController.send(JSON.stringify({
                jsonrpc: "2.0",
                method:"printer.objects.list",
                id: 100
            }));
            this.setStateChanged('info.connection', true, true);
            this.setStateChanged('info.online', true, true);
        });

        wsClient.on('message', (message) => {
            this.messageParse(message);
        });

        wsClient.on('close', async () => {
            this.setStateChanged('info.connection', false, true);
            this.setStateChanged('info.online', false, true);
            this.log.info('Websocket connection closed. Attempting to reconnect...');
        });
    }

    async messageParse(message) {
        const lock = new Promise((resolve) => resolve());
        const prev = this.messageParseMutex;
        this.messageParseMutex = lock;
        await prev;

        try {
            let messageObj = JSON.parse(message);

            this.log.debug(`--->>> fromAnycubic_RAW_1 -> ${JSON.stringify(messageObj)}`);

            const method = messageObj?.method;

            let request;

            if (messageObj.id == 100) {
                const param = await this.helper.removeGCodeObjects(messageObj.result.objects);
                const obj = {};
                for (const p of param) {
                    obj[p] = null;
                }
                this.subscribeParameter = {objects: obj};

                let shouldQuery = true;
                if (this.config.energy_id) {
                    try {
                        const s = await this.getForeignStateAsync(this.config.energy_id);
                        shouldQuery = s && s.val === true;
                    } catch (e) {
                        this.log.warn(`Could not read energy state ${this.config.energy_id}: ${e.message}`);
                    }
                }

                if (shouldQuery) {
                    this.websocketController.send(JSON.stringify({
                        jsonrpc: "2.0",
                        method: "printer.objects.query",
                        params: {
                            objects: {
                                "*": null
                            }
                        },
                        id: 102
                    }));
                }
            }

            if (messageObj.id == 102) {
                if (!obj102_done) {
                    const status = messageObj.result.status;

                    for (const key of Object.keys(status)) {
                        if (key !== '*') {
                            if (!(key in this.subscribeParameter.objects)) {
                                this.subscribeParameter.objects[key] = null;
                            }
                        }
                    }

                    // Only subscribe if energy state is true (or not configured)
                    let shouldSubscribe = true;
                    if (this.config.energy_id) {
                        try {
                            const s = await this.getForeignStateAsync(this.config.energy_id);
                            shouldSubscribe = s && s.val === true;
                        } catch (e) {
                            this.log.warn(`Could not read energy state ${this.config.energy_id}: ${e.message}`);
                        }
                    }

                    if (shouldSubscribe) {
                        this.websocketController.send(JSON.stringify({
                            jsonrpc: "2.0",
                            method: "printer.objects.subscribe",
                            params: this.subscribeParameter,
                            id: 1
                        }));
                    }

                    obj102_done = true;
                }
            }

            if (messageObj?.method) {
                if (method === undefined ) {
                    request = messageObj.result.status;
                    await this.helper.parseStart(request, this.parseOptions);
                } else {
                    request = messageObj.params;
                    await this.helper.parseMethod(request, this.parseOptions);
                }
            }

        } catch (err) {
            this.log.error(err);
            this.log.error(`<anycubic> error message -->> ${message}`);
        }
    }

    async onUnload(callback) {
        try {
            if (this.websocketController) {
                try {
                    await this.websocketController.allTimerClear();
                    this.websocketController.closeConnection();
                } catch (e) {
                    this.log.error(e);
                }
            }
            this.setStateChanged('info.connection', false, true);
        } finally {
            callback();
        }
    }


    async onStateChange(id, state) {
        if (!state || state.ack) {
return;
}

        // If energy state changed to true, re-trigger subscription
        if (this.config.energy_id && id === this.config.energy_id && state.val === true) {
            this.log.debug(`Energy state changed to true - (re)starting printer connection`);
            obj102_done = false;

            // Close existing connection first
            try {
                this.websocketController.closeConnection();
            } catch (e) {
                // ignore
            }

            const waitSeconds = parseInt(this.config.waitForPrinter) || 0;
            if (waitSeconds > 0) {
                this.log.debug(`Waiting ${waitSeconds}s for printer to boot up...`);
                await new Promise(resolve => setTimeout(resolve, waitSeconds * 1000));
            }

            this.startWebsocket();
            return;
        }

        // If energy state changed to false, close the websocket connection
        if (this.config.energy_id && id === this.config.energy_id && state.val === false) {
            this.log.debug(`Energy state changed to false - closing websocket connection`);
            obj102_done = false;
            try {
                this.websocketController.closeConnection();
                this.setStateChanged('info.online', false, true);
            } catch (e) {
                this.log.warn(`Error closing websocket: ${e.message}`);
            }
            return;
        }

        if (!this.allNodesCreated) {
            return;
        }

        if (state && state.ack === false) {
            if (id.endsWith('info.debugId')) {
                this.setStateChanged(id, state.val, true);
                return;
            }

            const obj = await this.getObjectAsync(id);
            if (obj) {
                const nativeObj = obj.native || {};

                const m = id.match(/nodeID_0*(\d+)/i);
                if (!m) {
                    this.log.warn(`<anycubic> Could not extract nodeId from state id: ${id}`);
                    return;
                }
                const nodeId = Number(m[1]);

                const message = {
                    messageId: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
                    command: 'node.set_value',
                    nodeId,
                    valueId: nativeObj.valueId,
                    value: state.val,
                };

                const sendMessageAllowed = await this.getStateAsync('info.sendMessageAllowed');

                if (sendMessageAllowed && sendMessageAllowed.val === true) {
                    if (this.websocketController) {
                        this.websocketController.send(JSON.stringify(message));
                    } else {
                        this.log.warn('<anycubic> websocketController not initialised, cannot send message.');
                    }
                }

                this.setStateChanged('info.debugmessages', JSON.stringify(message), true);
                this.log.debug(`<anycubic> message onStateChange ${JSON.stringify(message)}`);
            }
        }
    }
}

if (require.main !== module) {
    module.exports = (options) => new anycubic(options);
} else {
    new anycubic();
}
