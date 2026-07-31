'use strict';

const fs = require('fs');
const path = require('path');
const { COMMANDS } = require('./constants');

/**
 * Command class for creating and handling ioBroker command states
 */
class Command {
    /**
     * Creates a new Command instance.
     *
     * @param {object} adapter - The anycubic adapter instance.
     */
    constructor(adapter) {
        this.adapter = adapter;
        // Remembers the last successfully uploaded filename (on the printer)
        // so the startPrint button knows what to print.
        this.lastUploadedFile = null;
    }

    /**
     * Creates command channel and states and subscribes to them.
     */
    async createCommandStates() {
        // Channel anlegen
        await this.adapter.setObjectNotExistsAsync('command', {
            type: 'channel',
            common: {
                name: 'Command',
                role: 'channel'
            },
            native: {}
        });

        // Alle States aus COMMANDS erzeugen
        for (const [key, cmd] of Object.entries(COMMANDS)) {
            const common = {
                name: cmd.name,
                role: cmd.role,
                type: cmd.type,
                read: true,
                write: true,
                def: cmd.def
            };
            if (cmd.min !== undefined) {
                common.min = cmd.min;
            }
            if (cmd.max !== undefined) {
                common.max = cmd.max;
            }
            await this.adapter.setObjectNotExistsAsync(`command.${key}`, {
                type: 'state',
                common,
                native: {}
            });
        }

        // Subscribe auf command-States
        this.adapter.subscribeStates('command.*');

        // === Spezial-States: Datei-Upload & Druckstart ===
        // command.file: lokaler Dateipfad (z.B. /opt/iobroker/... oder Windows-Pfad).
        // Beim Schreiben wird die Datei per HTTP an den Drucker hochgeladen.
        await this.adapter.setObjectNotExistsAsync('command.file', {
            type: 'state',
            common: {
                name: 'Upload G-code file (local path)',
                role: 'text',
                type: 'string',
                read: true,
                write: true,
                def: ''
            },
            native: {}
        });

        // command.startPrint: Button, startet den Druck der zuletzt hochgeladenen Datei.
        await this.adapter.setObjectNotExistsAsync('command.startPrint', {
            type: 'state',
            common: {
                name: 'Start printing uploaded file',
                role: 'button',
                type: 'boolean',
                read: false,
                write: true,
                def: false
            },
            native: {}
        });

        // command.emergencyStop: Button für Notaus (M112 / emergency stop).
        // Sendet POST /printer/emergency_stop an Moonraker, um den Drucker
        // sofort zu stoppen (shutdown state).
        await this.adapter.setObjectNotExistsAsync('command.emergencyStop', {
            type: 'state',
            common: {
                name: 'Emergency stop the printer',
                role: 'button',
                type: 'boolean',
                read: false,
                write: true,
                def: false
            },
            native: {}
        });

        // Verwaiste command.* States entfernen (z.B. entfernte dryer_* States)
        await this.cleanupObsoleteCommandStates();
    }

    /**
     * Removes command.* states that are no longer defined in COMMANDS
     * (e.g. the removed dryer_start/dryer_temp/dryer_time states).
     */
    async cleanupObsoleteCommandStates() {
        // States, die nicht aus COMMANDS stammen, aber trotzdem behalten werden sollen.
        const keep = new Set(['file', 'startPrint', 'emergencyStop']);
        try {
            const existing = await this.adapter.getStatesOfAsync('command', '');
            for (const obj of existing || []) {
                const match = obj._id.match(/\.command\.([^.]+)$/);
                if (match && !COMMANDS[match[1]] && !keep.has(match[1])) {
                    await this.adapter.delObjectAsync(obj._id);
                    this.adapter.log.info(`<anycubic> Removed obsolete command state: ${obj._id}`);
                }
            }
        } catch (err) {
            this.adapter.log.warn(`<anycubic> cleanupObsoleteCommandStates failed: ${err.message}`);
        }
    }

    /**
     * Handles a command state change.
     *
     * @param {string} id - The full state id.
     * @param {object} state - The state object with val and ack.
     * @returns {Promise<boolean>} True if the command was handled, false otherwise.
     */
    async handleCommand(id, state) {
        // NOTE: state null/ack filtering is handled by the caller in main.js
        // (onStateChange line 537) before dispatch, so no need to repeat it here.
        const commandMatch = id.match(/^anycubic\.\d+\.command\.(.+)$/);
        if (!commandMatch) {
            return false;
        }

        const commandName = commandMatch[1];
        const value = state.val;

        // === Spezialbefehl: Datei-Upload ===
        if (commandName === 'file') {
            await this.uploadFile(value);
            return true;
        }

        // === Spezialbefehl: Druck starten ===
        if (commandName === 'startPrint') {
            if (value === true) {
                await this.startPrint();
                // Button zurücksetzen
                await this.adapter.setStateAsync('command.startPrint', { val: false, ack: true });
            }
            return true;
        }

        // === Spezialbefehl: Notaus (Emergency Stop) ===
        if (commandName === 'emergencyStop') {
            if (value === true) {
                await this.emergencyStop();
                // Button zurücksetzen
                await this.adapter.setStateAsync('command.emergencyStop', { val: false, ack: true });
            }
            return true;
        }

        // Lookup in COMMANDS
        const cmd = COMMANDS[commandName];
        if (!cmd) {
            const msg = `Unknown command "${commandName}".`;
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        // Script für den Wert suchen
        let script = cmd.scripts[value];

        // Fallback: wildcard script with placeholder substitution
        if (!script && cmd.scripts['*']) {
            script = cmd.scripts['*'].replace(/\{value\}/g, String(value));
        }

        if (!script) {
            const msg = `No script defined for command "${commandName}" with value "${value}".`;
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        // JSON-RPC Payload bauen und senden
        const messageId = 200 + Math.floor(Math.random() * 1000);
        const payload = JSON.stringify({
            jsonrpc: '2.0',
            method: 'printer.gcode.script',
            params: { script: script },
            id: messageId
        });

        const wsController = this.adapter.websocketController;
        const isConnected = wsController && wsController.wsClient
            && wsController.wsClient.readyState === 1; // WebSocket.OPEN

        if (!wsController) {
            const msg = 'websocketController not initialised, cannot send command.';
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        if (!isConnected) {
            const msg = `Cannot send command "${script}": no open websocket connection.`;
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        try {
            wsController.send(payload);
            this.adapter.log.info(`Sent command: ${script}`);
            await this.adapter.setStateAsync(id, { val: value, ack: true });
            return true;
        } catch (err) {
            const msg = `Failed to send command "${script}": ${err.message}`;
            this.adapter.log.error(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }
    }

    /**
     * Returns the printer's HTTP base URL from adapter config, or null if not configured.
     *
     * @returns {string|null} The base URL (e.g. http://192.168.1.10:4409) or null.
     */
    _getHttpBase() {
        const ip = this.adapter.config.webUIServer;
        const port = this.adapter.config.webUIPort || 4409;
        if (!ip) {
            const msg = 'webUIServer not configured – cannot use file upload / print start.';
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return null;
        }
        return `http://${ip}:${port}`;
    }

    /**
     * Uploads a local G-code file to the printer via Moonraker's
     * POST /server/files/upload endpoint (multipart/form-data).
     *
     * @param {string} localPath - Absolute path to the local file to upload.
     * @returns {Promise<boolean>} True on success, false otherwise.
     */
    async uploadFile(localPath) {
        if (!localPath || typeof localPath !== 'string') {
            this.adapter.log.warn('<anycubic> command.file: no file path provided.');
            return false;
        }

        const base = this._getHttpBase();
        if (!base) {
            return false;
        }

        if (!fs.existsSync(localPath)) {
            const msg = `command.file: local file not found: ${localPath}`;
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        const fileName = path.basename(localPath);
        this.adapter.setStateChanged('info.dataError', '', true);

        try {
            const buffer = fs.readFileSync(localPath);
            // Node 18+: globale FormData/Blob/fetch verfügbar
            const form = new FormData();
            form.append('file', new Blob([buffer]), fileName);
            form.append('root', 'gcodes');
            form.append('print', 'false');

            const response = await fetch(`${base}/server/files/upload`, {
                method: 'POST',
                body: form,
                signal: AbortSignal.timeout(60000)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const json = await response.json().catch(() => ({}));
            // Moonraker liefert item.path zurück (relativer Pfad im gcodes-Root)
            this.lastUploadedFile = json?.item?.path || fileName;

            this.adapter.log.info(`<anycubic> File uploaded successfully: ${this.lastUploadedFile}`);
            await this.adapter.setStateAsync('command.file', { val: localPath, ack: true });
            return true;
        } catch (err) {
            const msg = `File upload failed for "${fileName}": ${err.message}`;
            this.adapter.log.error(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }
    }

    /**
     * Starts printing the previously uploaded file via Moonraker's
     * POST /printer/print/start?filename=... endpoint.
     *
     * @returns {Promise<boolean>} True on success, false otherwise.
     */
    async startPrint() {
        const base = this._getHttpBase();
        if (!base) {
            return false;
        }

        if (!this.lastUploadedFile) {
            const msg = 'command.startPrint: no uploaded file available – upload a file via command.file first.';
            this.adapter.log.warn(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }

        const filename = this.lastUploadedFile;
        this.adapter.setStateChanged('info.dataError', '', true);

        try {
            const url = `${base}/printer/print/start?filename=${encodeURIComponent(filename)}`;
            const response = await fetch(url, {
                method: 'POST',
                signal: AbortSignal.timeout(15000)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            this.adapter.log.info(`<anycubic> Print started: ${filename}`);
            return true;
        } catch (err) {
            const msg = `Failed to start print for "${filename}": ${err.message}`;
            this.adapter.log.error(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }
    }

    /**
     * Immediately halts the printer via Moonraker's
     * POST /printer/emergency_stop endpoint.
     *
     * This puts the printer into a "shutdown" state. It is the same
     * as sending M112 via the console and should be used for an
     * emergency stop button. Unlike sending M112 via printer.gcode.script
     * (which queues the command), this endpoint halts the printer immediately.
     *
     * @returns {Promise<boolean>} True on success, false otherwise.
     */
    async emergencyStop() {
        const base = this._getHttpBase();
        if (!base) {
            return false;
        }

        this.adapter.setStateChanged('info.dataError', '', true);

        try {
            const response = await fetch(`${base}/printer/emergency_stop`, {
                method: 'POST',
                signal: AbortSignal.timeout(15000)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            this.adapter.log.info('<anycubic> Emergency stop executed.');
            return true;
        } catch (err) {
            const msg = `Emergency stop failed: ${err.message}`;
            this.adapter.log.error(`<anycubic> ${msg}`);
            this.adapter.setStateChanged('info.dataError', msg, true);
            return false;
        }
    }
}

module.exports = { Command };
