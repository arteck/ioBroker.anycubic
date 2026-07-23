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
        this.allNodesCreated = false;
        this.printDuration = null;
        this.lastPrintDuration = null;
        this.printState = null; // cached print_stats.state for incremental diff handling
        this.estimatedTime = null; // cached estimated_time from job metadata
        this.currentLayer = null; // cached current_layer from print_stats.info
        this.lastCurrentLayer = null; // track last current_layer for change detection
        this.totalLayer = null; // cached total_layer from print_stats.info
        this.lastTotalLayer = null; // track last total_layer for change detection
        this.lastTotalTime = null;
        this.lastFilename = null; // track filename changes for metadata fetch

        // State write buffer: stores path -> { value, ack } for deferred writes
        this._stateBuffer = new Map();
        this._flushInterval = null;
        this._lastEnergyVal = undefined;

        this.on('ready', () => {
            this.onReady().catch((e) => {
                this.log.error(`onReady error: ${e}`);
                this.setStateChanged('info.dataError', e.message, true);
            });
        });
        this.on('stateChange', (id, state) => {
            this.onStateChange(id, state).catch((e) => {
                this.log.error(`onStateChange error: ${e}`);
                this.setStateChanged('info.dataError', e.message, true);
            });
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
        this.setStateChanged('info.waitForPrinter', parseInt(this.config.waitForPrinter) || 0, true);

        if (this.energyId) {
            await this.subscribeForeignStatesAsync(this.energyId);
        }

        // Prüfen ob energy_id konfiguriert ist und der Wert false ist
        if (this.energyId) {
            const energyState = await this.getStateAsync(this.energyId);
            if (energyState && energyState.val === false) {
                this.log.info('Energy is off - not starting WebSocket connection. Toggle energy to connect.');
                this.setStateChanged('info.connection', false, true);
                await this.command.createCommandStates();
                this.allNodesCreated = true;
                return;
            }
        }

        this.startWebsocket(false);

        // Command-States anlegen
        await this.command.createCommandStates();
        this.allNodesCreated = true;

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
            this.setStateChanged('info.dataError', err.message || String(err), true);
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

            // Detect filename changes and fetch metadata
            const currentFilename = data.print_stats?.filename;
            if (currentFilename && currentFilename !== this.lastFilename) {
                this.lastFilename = currentFilename;
                this._fetchFileMetadata(currentFilename).catch(e =>
                    this.log.warn(`Failed to fetch file metadata: ${e.message}`)
                );
            }

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

            // Extract current_layer from print_stats.info
            const cl = (data.print_stats?.info && typeof data.print_stats.info.current_layer === 'number')
                ? data.print_stats.info.current_layer
                : null;
            if (cl != null) {
                this.currentLayer = cl;
            }

            // Extract total_layer from print_stats.info
            const tl = (data.print_stats?.info && typeof data.print_stats.info.total_layer === 'number')
                ? data.print_stats.info.total_layer
                : null;
            if (tl != null) {
                this.totalLayer = tl;
            }

            // Cache print_duration when present
            this.lastPrintDuration = pd;
            this.lastCurrentLayer = cl;
            this.lastTotalLayer = tl;
            if (pd != null) {
                this.printDuration = pd;
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
                this.currentLayer = null;
                this.lastCurrentLayer = null;
                this.totalLayer = null;
                this.lastTotalLayer = null;
                this.lastTotalTime = null;
            }
        }
    }

    /**
     * Berechnet info.totalTime aus den gecachten Werten.
     * Formel: ((estimatedTime - printDuration) + (estimatedTime * (1 - currentLayer / totalLayer))) / 2
     * Wird aufgerufen, wenn print_stats.print_duration sich ändert.
     */
    _calcTotalTime() {
        if (this.printState !== 'printing') {
            return;
        }

        if (this.estimatedTime != null && this.printDuration != null
            && this.currentLayer != null && this.totalLayer != null
            && this.totalLayer !== 0) {
            const method1 = Math.max(0, this.estimatedTime - this.printDuration);
            const method2 = Math.max(0, this.estimatedTime * (1 - this.currentLayer / this.totalLayer));
            const remaining = Math.max(0, (method1 + method2) / 2);
            const hours = String(Math.floor(remaining / 3600)).padStart(2, '0');
            const minutes = String(Math.floor((remaining % 3600) / 60)).padStart(2, '0');
            const seconds = String(Math.floor(remaining % 60)).padStart(2, '0');
            const formattedTotal = `${hours}:${minutes}:${seconds}`;
            if (formattedTotal !== this.lastTotalTime) {
                this._bufferStateChange('info.totalTime', formattedTotal, true);
                this.lastTotalTime = formattedTotal;
            }
        }
    }

    /**
     * Fetches file metadata from the printer's HTTP API.
     * Extracts estimated_time for finish-time calculation and thumbnail data.
     * @param {string} filename - The filename to fetch metadata for.
     */
    async _fetchFileMetadata(filename) {
        const ip = this.config.webUIServer;
        const port = this.config.webUIPort || 4409;

        if (!ip) {
            this.log.debug('webUIServer not configured – skipping metadata fetch');
            return;
        }

        const url = `http://${ip}:${port}/server/files/metadata?filename=${encodeURIComponent(filename)}`;
        this.setStateChanged('info.dataError', '', true);

        try {
            const response = await fetch(url, { signal: AbortSignal.timeout(5000) });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const json = await response.json();
            const result = json?.result;

            if (result?.estimated_time != null) {
                this.estimatedTime = result.estimated_time;
                await this.setObjectNotExistsAsync('job.metadata.estimated_time', {
                    type: 'state',
                    common: { name: 'Estimated print time', type: 'number', role: 'value', unit: 's', read: true, write: false, def: 0 },
                });
                await this.setStateAsync('job.metadata.estimated_time', result.estimated_time, true);
                this.log.debug(`estimated_time set to ${this.estimatedTime}s for "${filename}"`);
            }

            if (result?.thumbnails && Array.isArray(result.thumbnails)) {
                for (let i = 0; i < result.thumbnails.length; i++) {
                    const tn = result.thumbnails[i];

                    await this.setObjectNotExistsAsync(`job.metadata.thumbnails.${i}.relative_path`, {
                        type: 'state',
                        common: { name: `Thumbnail ${i} path`, type: 'string', role: 'text', read: true, write: false, def: '' },
                    });
                    await this.setStateAsync(`job.metadata.thumbnails.${i}.relative_path`, tn.relative_path ?? null, true);

                    if (tn.size != null) {
                        await this.setObjectNotExistsAsync(`job.metadata.thumbnails.${i}.size`, {
                            type: 'state',
                            common: { name: `Thumbnail ${i} size`, type: 'number', role: 'value', unit: 'bytes', read: true, write: false, def: 0 },
                        });
                        await this.setStateAsync(`job.metadata.thumbnails.${i}.size`, tn.size, true);
                    }
                    if (tn.width != null) {
                        await this.setObjectNotExistsAsync(`job.metadata.thumbnails.${i}.width`, {
                            type: 'state',
                            common: { name: `Thumbnail ${i} width`, type: 'number', role: 'value', unit: 'px', read: true, write: false, def: 0 },
                        });
                        await this.setStateAsync(`job.metadata.thumbnails.${i}.width`, tn.width, true);
                    }
                    if (tn.height != null) {
                        await this.setObjectNotExistsAsync(`job.metadata.thumbnails.${i}.height`, {
                            type: 'state',
                            common: { name: `Thumbnail ${i} height`, type: 'number', role: 'value', unit: 'px', read: true, write: false, def: 0 },
                        });
                        await this.setStateAsync(`job.metadata.thumbnails.${i}.height`, tn.height, true);
                    }
                }
                this.log.debug(`Thumbnails data written to job.metadata for "${filename}" (${result.thumbnails.length} entries)`);
            }

            // Clear data error on successful fetch
            this.setStateChanged('info.dataError', '', true);
        } catch (err) {
            if (err.name === 'AbortError' || err.name === 'TimeoutError') {
                this.log.warn(`Metadata fetch timed out for "${filename}"`);
            } else {
                this.log.warn(`Metadata fetch error for "${filename}": ${err.message}`);
            }
            this.setStateChanged('info.dataError', err.message, true);
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

            // Clear countdown interval if active
            if (this._waitPrinterInterval) {
                clearInterval(this._waitPrinterInterval);
                this._waitPrinterInterval = null;
            }

            // Clear getInfo timeout if active
            if (this._getInfoTimeout) {
                clearTimeout(this._getInfoTimeout);
                this._getInfoTimeout = null;
            }

            this._flushBuffer();

            if (this.websocketController) {
                try {
                    await this.websocketController.allTimerClear();
                    this.websocketController.closeConnection();
                } catch (e) {
                    this.log.error(e);
                    this.setStateChanged('info.dataError', e.message || String(e), true);
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
                job: null,
                mcu: null,
                print_stats: null,
                "mcu nozzle_mcu": null,
                ota_filament_hub: null,
                pause_resume: null,
                "pause_resume/cancel": null,
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
        // Energy state changes must be processed regardless of ack flag,
        // because external device adapters (e.g. Shelly) send updates with ack: true.
        if (this.energyId && id === this.energyId) {
            if (state && state.val === this._lastEnergyVal) {
                return;
            }
            this._lastEnergyVal = state ? state.val : undefined;

            if (state && state.val === true) {
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

                // Clear any existing countdown interval
                if (this._waitPrinterInterval) {
                    clearInterval(this._waitPrinterInterval);
                    this._waitPrinterInterval = null;
                }

                const waitSeconds = parseInt(this.config.waitForPrinter) || 0;

                // Start countdown: set initial value and start decrement interval
                let countdownRemaining = waitSeconds;
                this.setStateChanged('info.waitForPrinter', countdownRemaining, true);

                if (countdownRemaining > 0) {
                    this._waitPrinterInterval = setInterval(() => {
                        countdownRemaining--;
                        if (countdownRemaining <= 0) {
                            clearInterval(this._waitPrinterInterval);
                            this._waitPrinterInterval = null;
                            this.setStateChanged('info.waitForPrinter', 0, true);
                        } else {
                            this.setStateChanged('info.waitForPrinter', countdownRemaining, true);
                        }
                    }, 1000);
                }

                if (waitSeconds > 0) {
                    this.log.debug(`Waiting ${waitSeconds}s for printer to boot up...`);
                    await new Promise(resolve => setTimeout(resolve, waitSeconds * 1000));
                }

                // Ensure countdown is cleared after the wait completes
                if (this._waitPrinterInterval) {
                    clearInterval(this._waitPrinterInterval);
                    this._waitPrinterInterval = null;
                }
                this.setStateChanged('info.waitForPrinter', 0, true);

                this.startWebsocket(true);
                return;
            }

            if (state && state.val === false) {
                this.log.debug(`Energy state changed to false - closing websocket connection`);
                obj102_done = false;
                this.setStateChanged('info.connection', false, true);

                // Clear any existing countdown interval
                if (this._waitPrinterInterval) {
                    clearInterval(this._waitPrinterInterval);
                    this._waitPrinterInterval = null;
                }
                // Reset countdown state to configured value
                this.setStateChanged('info.waitForPrinter', parseInt(this.config.waitForPrinter) || 0, true);

                try {
                    if (this.websocketController) {
                        this.websocketController.closeConnection();
                    }
                } catch (e) {
                    this.log.warn(`Error closing websocket: ${e.message}`);
                }
                return;
            }

            // Energy ID matched but state is null/undefined – nothing to do
            return;
        }

        // All other state changes must pass the ack filter
        if (!state || state.ack) {
            return;
        }

        // === print_stats.print_duration: trigger totalTime recalculation ===
        if (id === `${this.namespace}.print_stats.print_duration`) {
            this.printDuration = state.val;
            this._calcTotalTime();
            return;
        }

        // === Command States ausführen (ausgelagert in command.js) ===
        if (await this.command.handleCommand(id, state)) {
            return;
        }

        // === Manual refresh button (info.getInfo) ===
        if (id === `${this.namespace}.info.getInfo` && state.val === true) {
            // Ignore if a 10-second cooldown is already active
            if (this._getInfoTimeout) {
                this.log.debug('Manual refresh button ignored – still in cooldown');
                return;
            }

            this.log.debug('Manual refresh button pressed');

            if (this.lastFilename) {
                this.log.debug(`Triggering metadata refresh for "${this.lastFilename}"`);
                this._fetchFileMetadata(this.lastFilename).catch(e =>
                    this.log.warn(`Failed to fetch file metadata: ${e.message}`)
                );
            } else {
                this.log.info('Kein aktiver Druckjob - Refresh nicht möglich');
            }

            // Reset button state back to false after 10 seconds
            this._getInfoTimeout = setTimeout(() => {
                this._getInfoTimeout = null;
                this.setStateChanged('info.getInfo', false, true);
            }, 10000);
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
