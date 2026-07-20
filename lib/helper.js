const utils = require("./utils");
const { EXCEPTIONS } = require('./constants');


/**
 * Helper class for creating and managing ioBroker objects 
 */
class Helper {
    /**
     * Creates a new Helper instance.
     *
     * @param {object} adapter - The ioBroker adapter instance.
     * @param {object} [alreadyCreatedObjects] - Cache of already created object paths.
     */
    constructor(adapter, alreadyCreatedObjects = {}) {
        this.adapter = adapter;
        this.alreadyCreatedObjects = alreadyCreatedObjects;

    }

    /**
     * Prüft ob ein State aufgrund von Exception-Regeln übersprungen werden soll.
     * @param {string} key - Der DP-Name (z.B. "name")
     * @param {*} value - Der Wert des DPs
     * @returns {boolean} - true wenn der State NICHT angelegt werden soll
     */
    _shouldSkipState(key, value) {
        // Keys mit kritischen Zeichen ignorieren (z.B. bed_mesh "default", pause_resume/cancel)
        if (typeof key === 'string') {
            for (const char of EXCEPTIONS.skipIfKeyContains) {
                if (key.includes(char)) {
                    return true;
                }
            }
        }

        // Prüfen ob der Key in der skipIfEmpty-Liste ist
        if (EXCEPTIONS.skipIfEmpty.includes(key)) {
            // Überspringen wenn value null oder undefined ist
            if (value === null || value === undefined) {
                return true;
            }
            // Überspringen wenn value ein leerer oder whitespace-only String ist
            if (typeof value === 'string' && value.trim() === '') {
                return true;
            }
        }
        return false;
    }

    /**
     * Normalises any value to a valid ioBroker common.type string.
     * Valid types: "number" | "string" | "boolean" | "array" | "object" | "mixed" | "file"
     *
     * @param {*} value - The raw value whose type should be determined.
     * @param {string} [hint] - An optional type hint (e.g. from metadata.type).
     * @returns {string} A valid ioBroker type string.
     */
    normalizeType(value, hint) {
        const VALID = new Set(["number", "string", "boolean", "array", "object", "mixed", "file"]);
        if (hint && VALID.has(hint)) {
            return hint;
        }
        if (Array.isArray(value)) {
            return "array";
        }
        const t = typeof value;
        if (t === "number") {
            return "number";
        }
        if (t === "boolean") {
            return "boolean";
        }
        // strings are stored as "mixed" to allow numeric/bool changes later
        return "mixed";
    }

    /**
     *
     * @param statusObj
     * @param options
     */
    async parseStart(statusObj, options = {write: false}) {
        if (!statusObj || typeof statusObj !== 'object' || Array.isArray(statusObj)) {
            return;
        }

        for (const key of Object.keys(statusObj)) {
            const value = statusObj[key];
            if (value !== null) {
                await this.parse(key, value, options, true);
            }
        }
    }

    /**
     *
     * @param data
     * @param options
     */
    async parseMethod(data, options = {write: false}) {
        if (!data) {
            return;
        }
        
        let statusObj;
        if (Array.isArray(data)) {
            statusObj = data[0];
        } else if (typeof data === 'object' && data !== null) {
            statusObj = data;
        } else {
            return;
        }
        
        if (!statusObj || typeof statusObj !== 'object' || Array.isArray(statusObj)) {
            return;
        }
        
        for (const key of Object.keys(statusObj)) {
            const value = statusObj[key];
            if (value !== null) {
                await this.parse(key, value, options, true);
            }
        }
    }

    /**
     * Recursively parses an element and creates the corresponding ioBroker objects and states.
     *
     * @param {string} path - The ioBroker object path to write to.
     * @param {*} element - The value or object to parse and persist.
     * @param {object} [options] - Parsing options (e.g. write, channelName, descriptions).
     * @param {boolean} [change] - If true, forces setState instead of setStateChanged.
     */
    async parse(path, element, options = {write: false}, change = false) {
        // Keys mit kritischen Zeichen im Pfad ignorieren (z.B. pause_resume/cancel)
        if (typeof path === 'string') {
            for (const char of EXCEPTIONS.skipIfKeyContains) {
                if (path.includes(char)) {
                    return;
                }
            }
        }

        let parsePath = utils.deleteLastDot(utils.formatObject(path));

        if (element === undefined || element === null) {
            this.adapter.log.error(`Skip undefined value for ${parsePath}`);
            return;
        }

        if (typeof element === "string" || typeof element === "number" || typeof element === "boolean") {
            // Prüfen ob dieser State aufgrund von Exception-Regeln übersprungen werden soll
            if (this._shouldSkipState(path, element)) {
                return;
            }

            let valDp = element ?? 0;

            let typeDp = typeof element;

            if (!this.alreadyCreatedObjects[parsePath]) {
                try {
                    let common;
                    if (typeof element === "boolean") {
                        common = {
                            id: parsePath,
                            name: parsePath,
                            role: "switch",
                            type: "boolean",
                            write: options.write,
                            read: true,
                            def: false,
                        };
                    } else {
                        // string or number
                        common = {
                            id: parsePath,
                            name: parsePath,
                            role: this.getRole(element, options.write),
                            type: typeDp,
                            write: options.write,
                            read: true,
                        };
                    }
                    await this.adapter.setObjectNotExistsAsync(parsePath, {
                        type: 'state',
                        common,
                        native: {},
                    });

                    if (common.write === true) {
                        this.adapter.subscribeStates(parsePath);
                    }

                    this.alreadyCreatedObjects[parsePath] = {};
                } catch (error) {
                    this.adapter.log.error(`parse error ${parsePath}`);
                    this.adapter.log.error(error);
                }
            }

            await this.changeState(parsePath, valDp, change);

            return;
        }

        const channelName = utils.getLastSegment(parsePath);

        if (!this.alreadyCreatedObjects[parsePath]) {
            try {
                await this.adapter.setObjectNotExistsAsync(parsePath, {
                    type: "channel",
                    common: {
                        name: channelName || ""
                    },
                    native: {},
                });

                this.alreadyCreatedObjects[parsePath] = {};
            } catch (error) {
                this.adapter.log.error(`parse error ${parsePath}`);
                this.adapter.log.error(error);
            }
        }

        if (Array.isArray(element)) {
            await this.extractArray(element, "", parsePath, options);
            return;
        }

        // ------------------------           info schleife

        const hasName2 = "name" in (element ?? {});
        if (!hasName2 && this.isObject(element)) {
            element.name = "";

        }

        for (const key of Object.keys(element)) {
            // Keys mit Anführungszeichen überspringen (z.B. bed_mesh "default")
            if (typeof key === 'string' && key.includes('"')) {
                continue;
            }

            const normalizedKey = key
                .replace(/([a-z])([A-Z])/g, '$1_$2')  // camelCase: displayStatus → display_Status
                .toLowerCase();
            let fullPath = utils.formatObject(`${parsePath}.${normalizedKey}`);
            let valDP = element[key];

            // Rekursion: if value is an object (but not a primitive wrapper), call parse recursively
            if (this.isObject(valDP) && !Array.isArray(valDP) && Object.keys(valDP).length > 0) {
                await this.parse(fullPath, valDP, options, change);
                continue;
            }

            // Leere Objekte (z.B. display_status: {}): Channel anlegen, keinen State
            if (this.isObject(valDP) && !Array.isArray(valDP) && Object.keys(valDP).length === 0) {
                if (!this.alreadyCreatedObjects[fullPath]) {
                    try {
                        await this.adapter.setObjectNotExistsAsync(fullPath, {
                            type: 'channel',
                            common: {
                                name: key,
                            },
                            native: {},
                        });
                        this.alreadyCreatedObjects[fullPath] = {};
                    } catch (error) {
                        this.adapter.log.error(`parse error ${fullPath}: ${error.message}`);
                    }
                }
                continue;
            }

            // Arrays: use extractArray
            if (Array.isArray(valDP)) {
                await this.extractArray(element, key, parsePath, options);
                continue;
            }

            // Prüfen ob dieser State aufgrund von Exception-Regeln übersprungen werden soll
            if (this._shouldSkipState(key, valDP)) {
                continue;
            }

            if (!this.alreadyCreatedObjects[fullPath]) {
                const objectName = options.descriptions?.[key] || key;
                let typeDp = this.normalizeType(valDP);

                fullPath = utils.deleteLastDot(fullPath);

                const common = {
                    id: objectName,
                    name: objectName,
                    role: this.getRole(valDP, options, key),
                    type: typeDp,
                    write: options.write,
                    read: true,
                };
                try {
                    await this.adapter.setObjectNotExistsAsync(fullPath, {
                        type: 'state',
                        common: common,
                        native: {},
                    });

                    this.alreadyCreatedObjects[fullPath] = {};
                    if (options.write) {
                        this.adapter.subscribeStates(fullPath);
                    }
                } catch (error) {
                    this.adapter.log.error(`parse error ${fullPath}`);
                    this.adapter.log.error(error);
                }
            }

            await this.changeState(fullPath, valDP, change);
        }
    }

    /**
     * Checks whether a value is a non-null object.
     *
     * @param {*} value - The value to check.
     * @returns {boolean}
     */
    isObject(value) {
        return value !== null && typeof value === "object";
    }

    /**
     * Extracts and processes an array from an element, creating ioBroker objects for each entry.
     *
     * @param {object|Array} element - The element containing the array, or the array itself.
     * @param {string} key - The key of the array within the element, or empty string if element is the array.
     * @param {string} path - The ioBroker base path to write to.
     * @param {object} options - Parsing options forwarded to the parse method.
     */
    async extractArray(element, key, path, options) {
        try {
            const array = key ? element[key] : element;

            for (let i = 0; i < array.length; i++) {
                const arrayElement = array[i];

                if (typeof arrayElement === "string") {
                    const segKey = (key === undefined || key === "") ? arrayElement : `${key}.${i}`;
                    await this.parse(
                        `${path}.${segKey}`,
                        arrayElement,
                        options,
                    );
                    continue;
                }

                if (key === undefined || key === "") {
                    await this.parse(`${path}.${i}`, arrayElement, options);
                } else {
                    await this.parse(`${path}.${key}.${i}`, arrayElement, options);
                }
            }
        } catch (error) {
            this.adapter.log.error(`Cannot extract array ${path}`);
        }
    }

    /**
     * Determines the ioBroker role string for a datapoint based on its value and metadata.
     *
     * @param {*} element - The value or metadata object to derive the role from.
     * @param {object|boolean} options - Parsing options or write flag.
     * @param {string} [dpName] - The datapoint name used to detect time-based roles.
     * @returns {string} The ioBroker role string (e.g. "state", "switch", "text").
     */
    getRole(element, options, dpName) {
        // const write = options.write;
        const hasStates = element && typeof element === "object" && element.states !== undefined;


        if (hasStates) {
            if (element.type === "boolean") {
                delete element.states;
                return "button";
            }
            return "switch";
        }

        if (typeof element === "string") {
            return "text";
        }

        if (typeof element === "boolean") {
            return "switch";
        }


        return "state";
    }

    /**
     * Entfernt alle GCode-Makros und GCode-Objekte aus der Objektliste.
     *
     * @param objects
     */
    async removeGCodeObjects(objects) {
        return objects.filter(obj => !obj.toLowerCase().startsWith('gcode'));
    }

    /**
     * Sets or conditionally updates an ioBroker state value.
     *
     * @param {string} path - The ioBroker state ID to set.
     * @param {*} value - The value to write to the state.
     * @param {boolean} [change] - If true, uses setState (unconditional); otherwise uses setStateChanged.
     */
    async changeState(path, value, change = false) {
        if (this.adapter._bufferStateChange) {
            this.adapter._bufferStateChange(path, value, true);
        } else if (change) {
            this.adapter.setState(path, value, true);
        } else {
            await this.adapter.setStateChangedAsync(path, value, true);
        }
    }

}

module.exports = {
    Helper: Helper,
};
