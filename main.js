'use strict';

const core = require('@iobroker/adapter-core');
const {StatesController} = require('./lib/statesController');
const {WebsocketController} = require('./lib/websocketController');


class anycubic extends core.Adapter {
    constructor(options) {
        super({
            ...options,
            name: 'anycubic',
        });

        // Instanz-State statt Modul-globale Variablen

        this.websocketController = null;
        this.statesController = null;

        this.messageParseMutex = Promise.resolve();
        this.parseOptions = {write: false};

        this.on('ready', () => {
            this.onReady().catch((e) => this.log.error(`onReady error: ${e}`));
        });
        this.on('stateChange', (id, state) => {
            this.onStateChange(id, state).catch((e) => this.log.error(`onStateChange error: ${e}`));
        });

        this.on('unload', this.onUnload.bind(this));

    }

    async onReady() {
        this.statesController = new StatesController(this);

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
        });

        wsClient.on('message', (message) => {
            this.messageParse(message);
        });

        wsClient.on('close', async () => {
            this.setStateChanged('info.connection', false, true);
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

            this.log.debug(`--->>> fromZ2W_RAW_1 -> ${JSON.stringify(messageObj)}`);

            const type = messageObj?.type;

            // TODO: Process message based on type
            if (!type) {
                this.log.warn(`<anycubic> received message without type: ${JSON.stringify(messageObj)}`);
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

            try {
                if (this.statesController) {
                    await this.statesController.setAllAvailableToFalse();
                }
            } catch (e) {
                this.log.error(e);
            }

            this.setStateChanged('info.connection', false, true);
        } finally {
            callback();
        }
    }


    async onStateChange(id, state) {
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
