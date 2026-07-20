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
    }

    /**
     * Handles a command state change.
     *
     * @param {string} id - The full state id.
     * @param {object} state - The state object with val and ack.
     * @returns {Promise<boolean>} True if the command was handled, false otherwise.
     */
    async handleCommand(id, state) {
        const commandMatch = id.match(/^anycubic\.\d+\.command\.(.+)$/);
        if (!commandMatch || !state || state.ack) {
            return false;
        }

        const commandName = commandMatch[1];
        const value = state.val;

        // Lookup in COMMANDS
        const cmd = COMMANDS[commandName];
        if (!cmd) {
            return false;
        }

        // Script für den Wert suchen
        let script = cmd.scripts[value];

        // Fallback: wildcard script with placeholder substitution
        if (!script && cmd.scripts['*']) {
            script = cmd.scripts['*'].replace(/\{value\}/g, String(value));
        }

        if (!script) {
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

        if (this.adapter.websocketController) {
            this.adapter.websocketController.send(payload);
        } else {
            this.adapter.log.warn('<anycubic> websocketController not initialised, cannot send command.');
        }
        this.adapter.log.info(`Sent command: ${script}`);

        await this.adapter.setStateAsync(id, { val: value, ack: true });

        return true;
    }
}

module.exports = { Command };
