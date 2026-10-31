
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


// Default Moonraker HTTP port, used when the adapter config has no webUIPort
const DEFAULT_WEB_UI_PORT = 4409;

// Defaults for fetchWithRetry()
const FETCH_ATTEMPTS = 3;
const FETCH_RETRY_DELAY_MS = 500;
const FETCH_TIMEOUT_MS = 15000;

// Active error sources per adapter instance (adapter -> Map(source -> message))
const _dataErrors = new WeakMap();

/**
 * Builds the printer's HTTP base URL (Moonraker web server) from the adapter config.
 * Single source of truth for the "http://<ip>:<port>" part used by main.js and command.js.
 *
 * @param {object} config - The adapter config (uses webUIServer / webUIPort).
 * @returns {string|null} Base URL like "http://192.168.1.10:4409", or null when no server is configured.
 */
function getHttpBase(config) {
	const ip = config?.webUIServer;
	if (!ip) {
		return null;
	}
	const port = config?.webUIPort || DEFAULT_WEB_UI_PORT;
	return `http://${ip}:${port}`;
}

/**
 * Waits for the given amount of milliseconds.
 *
 * @param {number} ms - Milliseconds to wait.
 * @returns {Promise<void>} Resolves after the given time.
 */
function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Performs a fetch() with a per-attempt timeout and a small retry/backoff loop.
 * Only transient failures are retried (network errors and 5xx responses).
 * 4xx responses are returned immediately and never repeated.
 *
 * @param {string} url - The request URL.
 * @param {object} [options] - fetch() options (method, body, headers, ...).
 * @param {object} [retryOptions] - Retry settings.
 * @param {number} [retryOptions.attempts] - Total number of attempts (default 3).
 * @param {number} [retryOptions.delayMs] - Base backoff delay in ms (default 500).
 * @param {number} [retryOptions.timeoutMs] - Timeout per attempt in ms (default 15000).
 * @returns {Promise<Response>} The fetch response (may be !ok for non-retryable 4xx responses).
 */
async function fetchWithRetry(url, options = {}, retryOptions = {}) {
	const attempts = Math.max(1, retryOptions.attempts ?? FETCH_ATTEMPTS);
	const baseDelay = Math.max(0, retryOptions.delayMs ?? FETCH_RETRY_DELAY_MS);
	const timeoutMs = retryOptions.timeoutMs ?? FETCH_TIMEOUT_MS;

	let lastError = null;

	for (let attempt = 1; attempt <= attempts; attempt++) {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), timeoutMs);

		try {
			const response = await fetch(url, { ...options, signal: controller.signal });

			// Success or a non-retryable client error (4xx) → hand back to the caller
			if (response.ok || response.status < 500) {
				return response;
			}

			// 5xx → possibly transient, retry
			lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
		} catch (err) {
			// Network error / timeout → transient, retry
			lastError = err;
		} finally {
			clearTimeout(timer);
		}

		if (attempt < attempts) {
			await sleep(baseDelay * Math.pow(2, attempt - 1));
		}
	}

	throw lastError || new Error(`Request to ${url} failed`);
}

/**
 * Returns the internal error map of an adapter instance, creating it on demand.
 *
 * @param {object} adapter - The adapter instance.
 * @returns {Map<string, string>} Map of source -> message.
 */
function _errorMap(adapter) {
	let map = _dataErrors.get(adapter);
	if (!map) {
		map = new Map();
		_dataErrors.set(adapter, map);
	}
	return map;
}

/**
 * Writes the composed info.dataError text of all active error sources to the adapter.
 *
 * @param {object} adapter - The adapter instance.
 */
function _publishDataError(adapter) {
	const text = Array.from(_errorMap(adapter).entries())
		.map(([source, message]) => `${source}: ${message}`)
		.join(' | ');
	adapter.setStateChanged('info.dataError', text, true);
}

/**
 * Registers/updates the error of one source and refreshes info.dataError.
 * Errors of other sources stay visible instead of being overwritten.
 *
 * @param {object} adapter - The adapter instance.
 * @param {string} source - Error source, e.g. "metadata", "upload", "websocket".
 * @param {string} message - The error message (falsy clears the source).
 */
function setDataError(adapter, source, message) {
	if (!message) {
		clearDataError(adapter, source);
		return;
	}
	_errorMap(adapter).set(source, String(message));
	_publishDataError(adapter);
}

/**
 * Removes the error of one source and refreshes info.dataError.
 *
 * @param {object} adapter - The adapter instance.
 * @param {string} source - Error source to clear.
 */
function clearDataError(adapter, source) {
	if (_errorMap(adapter).delete(source)) {
		_publishDataError(adapter);
	}
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

    // HTTP helpers (shared by main.js and command.js)
    DEFAULT_WEB_UI_PORT,
    getHttpBase,
    fetchWithRetry,

    // Central info.dataError bookkeeping
    setDataError,
    clearDataError,
};
