// Neue Befehle hier eintragen:
// 1. Key = State-Name (wird zum Pfad: command.<key>)
// 2. type = 'boolean' | 'number' | 'string'
// 3. scripts = { wert: 'G-Code', ... }
// Beispiel für einen weiteren Befehl:
//   extruder_temp: {
//       name: 'Extruder Temperature',
//       type: 'number',
//       role: 'level.temperature',
//       def: 0,
//       scripts: {
//           200: 'SET_HEATER_TEMPERATURE HEATER=extruder TARGET=200',
//           0: 'SET_HEATER_TEMPERATURE HEATER=extruder TARGET=0'
//       }
//   }

const COMMANDS = {
    dryer_status: {
        name: 'Dryer status',
        type: 'boolean',
        role: 'switch',
        def: false,
        scripts: {
            true: 'SET_HEATER_TEMPERATURE HEATER=temperature_fan_dryer TARGET=55',
            false: 'SET_HEATER_TEMPERATURE HEATER=temperature_fan_dryer TARGET=0'
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
    skipIfEmpty: ['name']
};

module.exports = { COMMANDS, EXCEPTIONS };
