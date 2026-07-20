'use strict';

const core = require('@iobroker/adapter-core');
const {WebsocketController} = require('./lib/websocketController');
const {Helper} = require('./lib/helper');
const {Command} = require('./lib/command');

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
        this.command = new Command(this);

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
        this.energyId = this.config.energy_id || null;

        // Prüfen ob energy_id konfiguriert ist und der Wert false ist
        if (this.energyId) {
            const energyState = await this.getStateAsync(this.energyId);
            if (energyState && energyState.val === false) {
                this.log.info('Energy is off - not starting WebSocket connection. Toggle energy to connect.');
                this.setStateChanged('info.connection', false, true);
                await this.command.createCommandStates();
                return;
            }
        }

        this.startWebsocket(false);

        // Command-States anlegen
        await this.command.createCommandStates();
    }


    async messageParse(message) {
        const lock = new Promise((resolve) => resolve());
        const prev = this.messageParseMutex;
        this.messageParseMutex = lock;
        await prev;

        let messageObj = JSON.parse(message);
        this.log.debug(`--->>> fromAnycubic_RAW_1 -> ${JSON.stringify(messageObj)}`);
        let request;
        let shouldQuery = true;

        try {
            if (messageObj?.method) {
                request = messageObj.params;
                await this.helper.parseMethod(request, this.parseOptions);
            } else if (messageObj?.result?.status) {
                request = messageObj.result.status;
                await this.helper.parseStart(request, this.parseOptions);
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

    startWebsocket(allowRetry = false) {
        this.websocketController = new WebsocketController(this);
        this.websocketController.start(
            {
            //    "motion_report": null,
                "configfile": null,
                "heaters": null,
            //    "respond": null,
           //     "display_status": null,
                "exclude_object": null,
                "extruder": null,
                "fan": null,
                "heater_bed": null,
                "mcu": null,
                "mcu nozzle_mcu": null,
                "ota_filament_hub": null,
                "pause_resume": null,
                "pause_resume/cancel": null,
                "print_stats": null,
                "toolhead": null,
            //    "verify_heater extrude": null,
                "verify_heater heater_bed": null,
                "virtual_sdcard": null,
                "webhooks": null,
            //    "bed_mesh": null,
            //    "bed_mesh default": null,
                "idle_timeout": null,
                "fan_generic air_filter_fan": null,
                "fan_generic box_fan": null,
                "mmu_machine": null,
                "mmu": null,
            },
            (message) => this.messageParse(message),
            allowRetry
        );
    }

    async onStateChange(id, state) {
        if (!state || state.ack) {
            return;
        }

        // If energy state changed to true, re-trigger subscription
        if (this.energyId && id === this.energyId && state.val === true) {
            this.log.debug(`Energy state changed to true - (re)starting printer connection`);
            obj102_done = false;

            // Close existing connection first
            try {
                if (this.websocketController) {
                    this.websocketController.closeConnection();
                }
            } catch (e) {
                this.log.debug(`Error closing websocket on energy true: ${e.message}`);
            }
            this.setStateChanged('info.connection', false, true);

            const waitSeconds = parseInt(this.config.waitForPrinter) || 0;
            if (waitSeconds > 0) {
                this.log.debug(`Waiting ${waitSeconds}s for printer to boot up...`);
                await new Promise(resolve => setTimeout(resolve, waitSeconds * 1000));
            }

            this.startWebsocket(true);
            return;
        }

        // If energy state changed to false, close the websocket connection
        if (this.energyId && id === this.energyId && state.val === false) {
            this.log.debug(`Energy state changed to false - closing websocket connection`);
            obj102_done = false;
            this.setStateChanged('info.connection', false, true);
            try {
                if (this.websocketController) {
                    this.websocketController.closeConnection();
                }
            } catch (e) {
                this.log.warn(`Error closing websocket: ${e.message}`);
            }
            return;
        }

        // === Command States ausführen (ausgelagert in command.js) ===
        if (await this.command.handleCommand(id, state)) {
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
        }
    }
}

if (require.main !== module) {
    module.exports = (options) => new anycubic(options);
} else {
    new anycubic();
}
