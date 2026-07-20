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
        this.lastPrintDuration = null;
        this.printState = null; // cached print_stats.state for incremental diff handling
        this.estimatedTime = null; // cached estimated_time from job metadata
        this.lastTotalTime = null;

        // State write buffer: stores path -> { value, ack } for deferred writes
        this._stateBuffer = new Map();
        this._flushInterval = null;

        this.on('ready', () => {
            this.onReady().catch((e) => this.log.error(`onReady error: ${e}`));
        });
        this.on('stateChange', (id, state) => {
            this.onStateChange(id, state).catch((e) => this.log.error(`onStateChange error: ${e}`));
        });

        this.on('message', (obj) => {
            this.onMessage(obj).catch((e) => this.log.error(`onMessage error: ${e}`));
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

    async onMessage(obj) {
        if (!obj || !obj.command) {
            return;
        }

        if (obj.command === 'queryConfig') {
            if (this.websocketController) {
                const payload = JSON.stringify({
                    jsonrpc: '2.0',
                    method: 'printer.objects.query',
                    params: { objects: { configfile: ['config', 'settings'] } },
                    id: 110,
                });
                this.websocketController.send(payload);
                this.log.info('JSON-RPC queryConfig sent: printer.objects.query for configfile');
            } else {
                this.log.warn('Cannot send queryConfig: websocket not connected');
            }
        }
    }

    async messageParse(message) {
        const lock = new Promise((resolve) => resolve());
        const prev = this.messageParseMutex;
        this.messageParseMutex = lock;
        await prev;

        let messageObj = JSON.parse(message);
        this.log.debug(`--->>> fromAnycubic_RAW_1 -> ${JSON.stringify(messageObj)}`);
        if (messageObj?.id === 110) {
            this.log.info(`Query Config Response: ${JSON.stringify(messageObj.result || messageObj)}`);
        }
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

        // Moonraker's notify_status_update sends INCREMENTAL diffs — only changed
        // fields are present.  "state" stays "printing" for the entire job so it
        // is typically absent after the very first update.  Cache the real state
        // so later diffs continue to treat the printer as "printing".
        const rawState = data.print_stats?.state;
        if (rawState === 'printing') {
            this.printState = rawState;
        }
        const state = this.printState;

        // Only calculate remaining time when actively printing
        if (state === 'printing') {
            // Extract and cache estimated_time from job metadata (printer provides this once after slicing)
            const estimated = (data.job?.metadata && typeof data.job.metadata.estimated_time === 'number')
                ? data.job.metadata.estimated_time
                : null;
            if (estimated != null) {
                this.estimatedTime = estimated;
            }

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

            // Calculate remaining time: estimated_time - print_duration
            if (this.estimatedTime != null && this.printDuration != null) {
                const remaining = Math.max(0, this.estimatedTime - this.printDuration);
                const hours = String(Math.floor(remaining / 3600)).padStart(2, '0');
                const minutes = String(Math.floor((remaining % 3600) / 60)).padStart(2, '0');
                const seconds = String(Math.floor(remaining % 60)).padStart(2, '0');
                const formattedTotal = `${hours}:${minutes}:${seconds}`;
                if (formattedTotal !== this.lastTotalTime) {
                    this._bufferStateChange('info.totalTime', formattedTotal, true);
                    this.lastTotalTime = formattedTotal;
                }
            }
        } else if (rawState !== undefined) {
            // rawState is explicitly set to a non-printing value (complete,
            // cancelled, error, standby, paused).  Only act on explicit state
            // transitions — don't clear just because state is absent from a diff.
            if (this.lastTotalTime !== '') {
                this._bufferStateChange('info.totalTime', '', true);
                this.lastTotalTime = '';
            }

            // Reset instance variables on print end states
            if (rawState === 'complete' || rawState === 'cancelled' || rawState === 'error' || rawState === 'standby') {
                this.printState = null;
                this.printDuration = null;
                this.lastPrintDuration = null;
                this.estimatedTime = null;
                this.lastTotalTime = null;
            }
        }
    }

    /**
     * Buffers a state change for deferred write (15-second flush interval).
     * Only the latest value per path is kept — previous writes are overwritten.
     *
     * @param {string} path - The ioBroker state path.
     * @param {*} value - The value to write.
     * @param {boolean} [ack] - Acknowledged flag.
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
                configfile: null,
                heaters: null,
            //    "respond": null,
           //     "display_status": null,
                exclude_object: null,
                extruder: null,
                fan: null,
                heater_bed: null,
                mcu: null,
                "mcu nozzle_mcu": null,
                ota_filament_hub: null,
                pause_resume: null,
                "pause_resume/cancel": null,
                print_stats: null,
                toolhead: null,
            //    "verify_heater extrude": null,
                "verify_heater heater_bed": null,
                virtual_sdcard: null,
                webhooks: null,
            //    "bed_mesh": null,
            //    "bed_mesh default": null,
                idle_timeout: null,
                "fan_generic air_filter_fan": null,
                "fan_generic box_fan": null,
                mmu_machine: null,
                mmu: null,
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
