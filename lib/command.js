'use strict';

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

        // Verwaiste command.* States entfernen (z.B. entfernte dryer_* States)
        await this.cleanupObsoleteCommandStates();
    }

    /**
     * Removes command.* states that are no longer defined in COMMANDS
     * (e.g. the removed dryer_start/dryer_temp/dryer_time states).
     */
    async cleanupObsoleteCommandStates() {
        try {
            const existing = await this.adapter.getStatesOfAsync('command', '');
            for (const obj of existing || []) {
                const match = obj._id.match(/\.command\.([^.]+)$/);
                if (match && !COMMANDS[match[1]]) {
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
}

module.exports = { Command };
