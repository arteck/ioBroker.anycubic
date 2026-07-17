
/**
 * Returns the last segment of a dot- or slash-separated string.
 *
 * @param {string} input - The input string to parse.
 */
function getLastSegment(input) {
    if (typeof input !== "string") {
        return "";
    }
    const parts = input.split(/[./]/).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : "";
}

/**
 * Checks whether a value is numeric (finite number or numeric string).
 *
 * @param {any} value - The value to check.
 * @returns {boolean}
 */
function isNumeric(value) {
    if (value === null || value === undefined) {
        return false;
    }
    if (typeof value === "number") {
        return Number.isFinite(value);
    }
    if (typeof value === "string") {
        const s = value.trim();
        if (s === "") {
            return false;
        }
        return /^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$/.test(s);
    }
    return false;
}

/**
 * Removes a trailing dot from a string if present.
 *
 * @param {string|undefined} str - The string to process.
 */
function deleteLastDot(str) {
    if (typeof str !== "string") {
        return "";
    }
    return str.endsWith(".") ? str.slice(0, -1) : str;
}

/**
 * Trims and normalises an object name string.
 *
 * @param {string} str - The string to format.
 */
function formatObject(str) {
    if (typeof str !== "string") {
        return "";
    }
    return str.trim().replace(/₂/g, "2").replace(/\s+/g, "_");
}


/**
 * Returns a human-readable status text for a given node status code.
 *
 * @param {number} status - The numeric status code.
 */
function getStatusText(status) {
    const nodeStatus = {
        0: "Unknown",
        1: "asleep",
        2: "awake",
        3: "dead",
        4: "alive",
    };

    return nodeStatus[status] || "Unknown";
}

module.exports = {
    getLastSegment,
    isNumeric,
    deleteLastDot,
    formatObject,

    getStatusText,
};
