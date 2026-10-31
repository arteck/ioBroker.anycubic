// Command definitions used by Command.createCommandStates() / Command.handleCommand().
// 1. Key = state name, becomes the path command.<key> (no spaces / special characters)
// 2. name = display name, type = 'boolean' | 'number' | 'string', role, def (optional: min/max)
// 3. scripts = { <state value>: '<gcode macro>' } -- a '*' key acts as wildcard and
//    gets {value} replaced by the written state value.
// The macros below are the Moonraker/Klipper defaults shipped with Rinkhals on an
// Anycubic Kobra; unknown macros are reported back by Moonraker as JSON-RPC errors.
const COMMANDS = {
    pause: {
        name: 'Pause print',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'PAUSE'
        }
    },
    resume: {
        name: 'Resume print',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'RESUME'
        }
    },
    cancel: {
        name: 'Cancel print',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'CANCEL_PRINT'
        }
    },
    home: {
        name: 'Home all axes',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'G28'
        }
    },
    light: {
        name: 'Light on/off',
        type: 'boolean',
        role: 'switch',
        def: false,
        scripts: {
            true: 'SET_LED LED=cavity WHITE=1',
            false: 'SET_LED LED=cavity WHITE=0'
        }
    },
    fan: {
        name: 'Fan speed (part cooling, PWM 0-255)',
        type: 'number',
        role: 'level.speed',
        def: 0,
        min: 0,
        max: 255,
        scripts: {
            '*': 'M106 S{value}'
        }
    },
    fanOff: {
        name: 'Fan off',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'M107'
        }
    },
    nozzleTemp: {
        name: 'Nozzle target temperature (°C)',
        type: 'number',
        role: 'level.temperature',
        def: 0,
        min: 0,
        max: 300,
        scripts: {
            0: 'M104 S0',
            '*': 'M104 S{value}'
        }
    },
    bedTemp: {
        name: 'Bed target temperature (°C)',
        type: 'number',
        role: 'level.temperature',
        def: 0,
        min: 0,
        max: 120,
        scripts: {
            0: 'M140 S0',
            '*': 'M140 S{value}'
        }
    },
    motorsOff: {
        name: 'Disable steppers',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'M84'
        }
    },
    nozzleClean: {
        name: 'Clean nozzle',
        type: 'boolean',
        role: 'button',
        def: false,
        scripts: {
            true: 'G12'
        }
    }
};

/**
 * Exception-Tabelle für zu filternde State-Definitionen.
 * 
 * skipIfEmpty: Liste von DP-Namen (Keys), die NICHT angelegt werden sollen,
 *              wenn ihr Wert null, undefined oder ein leerer/whitespace-only String ist.
 *              Z.B.: name, description, etc.
 */
const EXCEPTIONS = {
    skipIfEmpty: ['name'],
    skipIfKeyContains: ['"', '/', '\\']
};

/**
 * Blacklist für State-Pfade, die nicht als ioBroker States angelegt werden sollen.
 * Jeder Eintrag matcht jeden Pfad, der mit diesem String beginnt.
 * Z.B. 'exclude_object' matcht:
 *   - exclude_object
 *   - exclude_object.objects.0.polygon
 *   - exclude_object.* (alles darunter)
 */
const BLACKLIST = [
    'polygon',
];

module.exports = { COMMANDS, EXCEPTIONS, BLACKLIST };
