'use strict';

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
        // Channel
        await this.adapter.setObjectNotExistsAsync('command', {
            type: 'channel',
            common: {
                name: 'Command',
                role: 'channel'
            },
            native: {}
        });

        // dryer_status
        await this.adapter.setObjectNotExistsAsync('command.dryer_status', {
            type: 'state',
            common: {
                name: 'Dryer status',
                role: 'switch',
                type: 'boolean',
                read: true,
                write: true,
                def: false
            },
            native: {}
        });

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
        const match = id.match(/^anycubic\.\d+\.command\.(.+)$/);
        if (!match || !state || state.ack === true) {
            return false;
        }

        const commandName = match[1];
        const value = state.val;
        let script = null;

        if (commandName === 'dryer_status') {
            script = value === true
                ? 'SET_HEATER_TEMPERATURE HEATER=temperature_fan_dryer TARGET=55'
                : 'SET_HEATER_TEMPERATURE HEATER=temperature_fan_dryer TARGET=0';
        }

        if (script) {
            const payload = {
                jsonrpc: '2.0',
                method: 'printer.gcode.script',
                params: {script},
                id: 200 + Math.floor(Math.random() * 1000)
            };

            if (this.adapter.websocketController) {
                this.adapter.websocketController.send(JSON.stringify(payload));
            }

            this.adapter.log.info(`Command: ${commandName} = ${value} → ${script}`);

            await this.adapter.setStateAsync(id, {val: value, ack: true});
        }

        return true;
    }
}

module.exports = { Command };
