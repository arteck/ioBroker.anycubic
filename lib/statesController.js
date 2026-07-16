
class StatesController {
  /**
   * Creates a new StatesController instance.
   *
   * @param {object} adapter - The ioBroker adapter instance.
   */
  constructor(adapter) {
    this.adapter = adapter;
  }

  /**
   * Sets a state value unconditionally, skipping null/undefined values.
   *
   * @param {string} stateName - The ioBroker state ID to set.
   * @param {*} value - The value to write to the state.
   */
  async setStateSafelyAsync(stateName, value) {
    if (value === undefined || value === null) {
      return;
    }
    this.adapter.setState(stateName, value, true);
  }

  /**
   * Sets a state value only if it has changed, skipping null/undefined values.
   *
   * @param {string} stateName - The ioBroker state ID to set.
   * @param {*} value - The value to write to the state.
   */
  async setStateChangedSafelyAsync(stateName, value) {
    if (value === undefined || value === null) {
      return;
    }
    await this.adapter.setStateChangedAsync(stateName, value, true);
  }

  async subscribeAllWritableExistsStates() {
    const writableStates = {};

    const ns = `${this.adapter.namespace}.`;
    const res = await this.adapter.getObjectViewAsync("system", "state", {
      startkey: ns,
      endkey: `${ns}\u9999`,
    });

    for (const row of res.rows) {
      const obj = row.value;
      if (obj?.common?.write === true) {
        writableStates[obj._id] = {
          write: true,
          subst: null,
        };
      }
    }

    return writableStates;
  }

}

module.exports = {
  StatesController,
};
