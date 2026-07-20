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

        // Track print progress for finish time estimation
        this.printDuration = null;
        this.printProgress = null;
        this.lastFinishTime = null;
        this.lastPrintDuration = null;

        // State write buffer: stores path -> { value, ack } for deferred writes
        this._stateBuffer = new Map();
        this._flushInterval = null;

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

        // Expose buffer method to helper so all dynamic states go through the buffer
        this._bufferStateChange = this._bufferStateChange.bind(this);

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

        // Start the 15-second state write buffer flush interval
        this._flushInterval = setInterval(() => this._flushBuffer(), 15000);
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

            // Track print progress data and calculate finish time
            this._updateFinishTime(messageObj);

        } catch (err) {
            this.log.error(err);
            this.log.error(`<anycubic> error message -->> ${message}`);
        }
    }

    _updateFinishTime(messageObj) {
        // Fix 3: Only process notify_status_update messages (carries printer status data)
        if (messageObj?.method !== 'notify_status_update') {
            return;
        }

        const params = messageObj.params;
        let data;
        if (Array.isArray(params)) {
            data = params[0];
        } else {
            data = params;
        }

        if (!data || typeof data !== 'object') {
            return;
        }

        const state = data.print_stats?.state;

        // Fix 1: Only calculate finish time when actively printing
        if (state === 'printing') {
            // Extract print_duration from print_stats
            const pd = (data.print_stats && typeof data.print_stats.print_duration === 'number')
                ? data.print_stats.print_duration
                : null;

            // Only recalculate when print_duration actually ticks forward
            if (pd === this.lastPrintDuration) {
                return;
            }
            this.lastPrintDuration = pd;
            if (pd != null) {
                this.printDuration = pd;
            }

            // Extract progress from virtual_sdcard
            if (data.virtual_sdcard && typeof data.virtual_sdcard.progress === 'number') {
                this.printProgress = data.virtual_sdcard.progress;
            }

            // Calculate finish time when both values are available and progress > 0
            if (this.printDuration != null && this.printProgress != null && this.printProgress > 0) {
                const elapsed = this.printDuration;
                const remaining = (elapsed / this.printProgress) - elapsed;
                const hours = String(Math.floor(remaining / 3600)).padStart(2, '0');
                const minutes = String(Math.floor((remaining % 3600) / 60)).padStart(2, '0');
                const formattedTime = `${hours}:${minutes}`;
                if (formattedTime !== this.lastFinishTime) {
                    this._bufferStateChange('info.finishTime', formattedTime, true);
                    this.lastFinishTime = formattedTime;
                }
            }
        } else {
            // Fix 1: Not printing, clear finish time
            if (this.lastFinishTime !== '') {
                this._bufferStateChange('info.finishTime', '', true);
                this.lastFinishTime = '';
            }

            // Fix 2: Reset instance variables on print end states
            if (state === 'complete' || state === 'cancelled' || state === 'error' || state === 'standby') {
                this.printDuration = null;
                this.printProgress = null;
                this.lastFinishTime = null;
                this.lastPrintDuration = null;
            }
        }
    }

    /**
     * Buffers a state change for deferred write (15-second flush interval).
     * Only the latest value per path is kept — previous writes are overwritten.
     *
     * @param {string} path - The ioBroker state path.
     * @param {*} value - The value to write.
     * @param {boolean} [ack=true] - Acknowledged flag.
     */
    _bufferStateChange(path, value, ack = true) {
        this._stateBuffer.set(path, { value, ack });
    }

    /**
     * Flushes all buffered state changes to ioBroker immediately.
     */
    _flushBuffer() {
        if (this._stateBuffer.size === 0) {
            return;
        }
        const count = this._stateBuffer.size;
        for (const [path, { value, ack }] of this._stateBuffer) {
            this.setStateChanged(path, value, ack);
        }
        this._stateBuffer.clear();
        this.log.debug(`_flushBuffer: flushed ${count} state(s)`);
    }

    async onUnload(callback) {
        try {
            // Clear flush interval and flush any remaining buffered states
            if (this._flushInterval) {
                clearInterval(this._flushInterval);
                this._flushInterval = null;
            }
            this._flushBuffer();

            if (this.websocketController) {
                try {
                    await this.websocketController.allTimerClear();
                    this.websocketController.closeConnection();
                } catch (e) {
                    this.log.error(e);
                }
            }
            // info.connection must NOT go through buffer — write immediately
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
