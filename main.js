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
        this.printProgress = null; // cached progress from virtual_sdcard (0.0 - 1.0)

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

        // Readonly-State info.webIP im Format "webUIServer:webUIPort" anlegen/aktualisieren
        await this.setObjectNotExistsAsync('info.webIP', {
            type: 'state',
            common: {
                name: 'WebUi IP-Address',
                type: 'string',
                role: 'info.ip',
                read: true,
                write: false,
                def: '',
            },
            native: {},
        });
        const webIP = `${this.config.webUIServer || ''}:${this.config.webUIPort || ''}`;
        this.setStateChanged('info.webIP', webIP, true);

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
                this.subscribeStates('info.getInfo');
                return;
            }
        }

        this.startWebsocket(false);

        // Command-States anlegen
        await this.command.createCommandStates();
        this.allNodesCreated = true;

        // Restore cached progress values from existing states so a restart
        // mid-print can still calculate info.totalTime before new WS data arrives.
        await this._restoreProgressCache();

        // Subscribe to the manual refresh button so onStateChange receives its presses
        this.subscribeStates('info.getInfo');

        // Start the 15-second state write buffer flush interval
        this._flushInterval = setInterval(() => this._flushBuffer(), this.config.wsRefreshRate * 1000 || 15000);
    }

    /**
     * Restores cached progress variables (estimatedTime, layers, filename) from
     * already persisted states. Needed after an adapter restart mid-print, where
     * the WebSocket only sends incremental diffs that may omit these fields.
     */
    async _restoreProgressCache() {
        try {
            const est = await this.getStateAsync('job.metadata.estimated_time');
            if (est && typeof est.val === 'number') {
                this.estimatedTime = est.val;
            }
            const fn = await this.getStateAsync('print_stats.filename');
            if (fn && typeof fn.val === 'string' && fn.val) {
                this.lastFilename = fn.val;
            }
            this.log.debug(
                `_restoreProgressCache: estimatedTime=${this.estimatedTime}, lastFilename=${this.lastFilename}`
            );
        } catch (e) {
            this.log.debug(`_restoreProgressCache failed: ${e.message}`);
        }
    }

    async messageParse(message) {
        const lock = new Promise((resolve) => resolve());
        const prev = this.messageParseMutex;
        this.messageParseMutex = lock;
        await prev;

        let messageObj = JSON.parse(message);
        this.log.debug(`--->>> fromAnycubic_RAW_1 -> ${JSON.stringify(messageObj)}`);

        let request;

        try {
                if (messageObj?.method) {
                    request = messageObj.params;
                    await this.helper.parseMethod(request, this.parseOptions);

                    // Track print progress data and calculate finish time
                    // (only notify_status_update carries incremental status diffs)
                    if (messageObj.method === 'notify_status_update') {
                        const params = messageObj.params;
                        const data = Array.isArray(params) ? params[0] : params;
                        this._updateFinishTime(data);
                    }
                } else if (messageObj?.result?.status) {
                    request = messageObj.result.status;
                    await this.helper.parseStart(request, this.parseOptions);

                    // The initial full-status response also carries print_stats/job data.
                    // Feed it into the finish-time logic so a (re)connect mid-print
                    // correctly initialises filename, state and estimatedTime.
                    this._updateFinishTime(messageObj.result.status);
                } else if (messageObj?.error) {
                    // JSON-RPC error response (e.g. an unknown gcode macro was sent
                    // via command.*). Without this branch such errors were silently
                    // swallowed, so failed commands looked like "nothing happened".
                    const errMsg = messageObj.error?.message || JSON.stringify(messageObj.error);
                    this.log.warn(`<anycubic> command/request failed (id=${messageObj.id}): ${errMsg}`);
                    this.setStateChanged('info.dataError', errMsg, true);
                } else if (typeof messageObj?.id === 'number' && messageObj.id >= 200 && messageObj.id < 1200 && 'result' in messageObj) {
                    // Acknowledgement of a gcode command sent from handleCommand()
                    // (message ids are 200..1199). Log it so successful sends are visible.
                    this.log.debug(`<anycubic> command ack (id=${messageObj.id}): ${JSON.stringify(messageObj.result)}`);
                }
        } catch (err) {
            this.log.error(err);
            this.log.error(`<anycubic> error message -->> ${message}`);
            this.setStateChanged('info.dataError', err.message || String(err), true);
        }
    }

    _updateFinishTime(data) {
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

            // Extract progress from virtual_sdcard (0.0 - 1.0)
            const prog = (data.virtual_sdcard && typeof data.virtual_sdcard.progress === 'number')
                ? data.virtual_sdcard.progress
                : null;
            if (prog != null) {
                this.printProgress = prog;
            }

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

            // Recalculate totalTime after all relevant values are refreshed
            this._calcTotalTime().catch(e =>
                this.log.warn(`_calcTotalTime failed: ${e.message}`)
            );
        } else if (rawState !== undefined) {
            // rawState is explicitly set to a non-printing value (complete,
            // cancelled, error, standby, paused).  Only act on explicit state
            // transitions — don't clear just because state is absent from a diff.

            // Reset instance variables on print end states
            if (rawState === 'complete' || rawState === 'cancelled' || rawState === 'error' || rawState === 'standby') {
                // For "complete", set totalTime to the formatted print_duration
                // (actual total elapsed time) before variables are nullified
                if (rawState === 'complete' && this.printDuration != null) {
                    const pd = this.printDuration;
                    const hours = String(Math.floor(pd / 3600)).padStart(2, '0');
                    const minutes = String(Math.floor((pd % 3600) / 60)).padStart(2, '0');
                    const seconds = String(Math.floor(pd % 60)).padStart(2, '0');
                    const formattedDuration = `${hours}:${minutes}:${seconds}`;
                    this._bufferStateChange('info.totalTime', formattedDuration, true);
                    this.lastTotalTime = formattedDuration;
                } else if (this.lastTotalTime !== '--:--:--') {
                    // For cancelled, error, standby — reset to placeholder
                    this._bufferStateChange('info.totalTime', '--:--:--', true);
                    this.lastTotalTime = '--:--:--';
                }

                this.printState = null;
                this.printDuration = null;
                this.lastPrintDuration = null;
                this.estimatedTime = null;
                this.currentLayer = null;
                this.lastCurrentLayer = null;
                this.totalLayer = null;
                this.lastTotalLayer = null;
                // Don't reset lastTotalTime for "complete" — it holds the formatted duration
                if (rawState !== 'complete') {
                    this.lastTotalTime = '--:--:--';
                }
            } else if (this.lastTotalTime !== '--:--:--') {
                // For non-terminal states like "paused", still reset totalTime
                this._bufferStateChange('info.totalTime', '--:--:--', true);
                this.lastTotalTime = '--:--:--';
            }
        }
    }

    /**
     * Berechnet info.totalTime aus den gecachten Werten.
     * Formel: Wie Mainsail's "Schätzung" - basiert auf virtuellem SD-Karten Fortschritt
     * (print_duration / progress) - print_duration
     * Wird von _updateFinishTime() nach jeder notify_status_update-Nachricht aufgerufen.
     */
    async _calcTotalTime() {
        if (this.printState !== 'printing') {
            return;
        }

        // Fallback: pull any missing value directly from the persisted state tree.
        // Moonraker sends incremental diffs, so a single field may not yet be cached
        // even though its state already exists in the object tree.
        if (this.printDuration == null) {
            const s = await this.getStateAsync('print_stats.print_duration');
            if (s && typeof s.val === 'number') {
                this.printDuration = s.val;
            }
        }
        if (this.printProgress == null) {
            const s = await this.getStateAsync('virtual_sdcard.progress');
            if (s && typeof s.val === 'number') {
                this.printProgress = s.val;
            }
        }

        // Layer-Informationen werden weiterhin geladen für andere Zwecke
        if (this.currentLayer == null) {
            const s = await this.getStateAsync('print_stats.info.current_layer');
            if (s && typeof s.val === 'number') {
                this.currentLayer = s.val;
            }
        }
        if (this.totalLayer == null) {
            const s = await this.getStateAsync('print_stats.info.total_layer');
            if (s && typeof s.val === 'number') {
                this.totalLayer = s.val;
            }
        }

        // Mainsail's "Schätzung" Formel: Fortschritts-basierte Extrapolation
        // Restzeit = (verstrichene_Zeit / Fortschritt) - verstrichene_Zeit
        if (this.printDuration != null && this.printProgress != null && this.printProgress > 0) {
            const totalEstimated = this.printDuration / this.printProgress;
            const remaining = Math.max(0, totalEstimated - this.printDuration);
            const hours = String(Math.floor(remaining / 3600)).padStart(2, '0');
            const minutes = String(Math.floor((remaining % 3600) / 60)).padStart(2, '0');
            const seconds = String(Math.floor(remaining % 60)).padStart(2, '0');
            const formattedTotal = `${hours}:${minutes}:${seconds}`;
            if (formattedTotal !== this.lastTotalTime) {
                this._bufferStateChange('info.totalTime', formattedTotal, true);
                this.lastTotalTime = formattedTotal;
            }
        } else {
            this.log.debug(
                `_calcTotalTime skipped - missing values: printDuration=${this.printDuration}, ` +
                `printProgress=${this.printProgress}`
            );
        }
    }

    /**
     * Fetches file metadata from the printer's HTTP API.
     * Extracts estimated_time for finish-time calculation and thumbnail data.
     *
     * @param {string} filename - The filename to fetch metadata for.
     */
    async _fetchFileMetadata(filename) {
        const ip = this.config.webUIServer;
        const port = this.config.webUIPort || 4409;

        if (!ip) {
            this.log.debug('webUIServer not configured – skipping metadata fetch');
            return;
        }

        const filePath = filename.includes('/') ? filename : `gcodes/${filename}`;
        const encodedPath = filePath.split('/').map(part => encodeURIComponent(part)).join('/');
        const url = `http://${ip}:${port}/server/files/metadata?filename=${encodedPath}`;
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

            // Dynamically map every scalar metadata field into job.metadata.*
            // (skips estimated_time – handled above – and thumbnails – handled below).
            if (result && typeof result === 'object') {
                for (const [key, value] of Object.entries(result)) {
                    if (key === 'estimated_time' || key === 'thumbnails') {
                        continue;
                    }
                    await this._writeMetadataField(`job.metadata.${key}`, key, value);
                }
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
     * Writes a single metadata field to the object tree, creating the state
     * definition on demand. Scalars (string/number/boolean) are written directly;
     * arrays/objects are stored as JSON strings so nothing gets lost.
     *
     * @param {string} path - Full state path (e.g. "job.metadata.slicer").
     * @param {string} key - The metadata key (used for the state name).
     * @param {*} value - The value to write.
     */
    async _writeMetadataField(path, key, value) {
        if (value === null || value === undefined) {
            return;
        }

        let type;
        let role;
        let outValue = value;

        if (typeof value === 'number') {
            type = 'number';
            role = 'value';
        } else if (typeof value === 'boolean') {
            type = 'boolean';
            role = 'indicator';
        } else if (typeof value === 'string') {
            type = 'string';
            role = 'text';
        } else {
            // Arrays / nested objects → store as JSON string
            type = 'string';
            role = 'json';
            outValue = JSON.stringify(value);
        }

        await this.setObjectNotExistsAsync(path, {
            type: 'state',
            common: { name: key, type, role, read: true, write: false },
        });
        await this.setStateAsync(path, outValue, true);
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

        // NOTE: print_stats.print_duration handling was removed from here because
        // the ack filter above (line 534: `if (!state || state.ack)`) rejects all
        // adapter-written states (ack: true), making this branch unreachable.
        // _calcTotalTime() is now called directly from _updateFinishTime() via
        // WebSocket notify_status_update messages instead.

        // === Manual refresh button (info.getInfo) === must be handled BEFORE
        // handleCommand so the button is always reset, even if a command dispatch returns.
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
                this.log.info('No active print job - refresh not possible');
            }

            // Reset button state back to false after 10 seconds
            this._getInfoTimeout = setTimeout(() => {
                this._getInfoTimeout = null;
                this.setStateAsync('info.getInfo', false, true).catch(e =>
                    this.log.warn(`Failed to reset info.getInfo: ${e.message}`)
                );
            }, 10000);
            return;
        }

        // === Command States ausführen (ausgelagert in command.js) ===
        if (await this.command.handleCommand(id, state)) {
            return;
        }

        if (!this.allNodesCreated) {
            return;
        }

        // NOTE: info.debugId handling was removed because the state was never
        // defined, created, or subscribed anywhere. The redundant `state && state.ack === false`
        // guard was also dead — line 537 already guarantees both conditions by this point.
    }
}

if (require.main !== module) {
    module.exports = (options) => new anycubic(options);
} else {
    new anycubic();
}
