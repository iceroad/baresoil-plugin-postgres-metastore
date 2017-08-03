class EventLog {
  sql(type, arg, queryTimeMs) {
    if (!this.enabled_) return;
    const caller = this.callsite(null, [
      /^EventLog/,
    ]).summary;
    if (type === 'query') {
      console.debug(`${JSON.stringify(caller)}: SQL Query: ${arg.substr(0, 2048)}`);
    }
    if (type === 'result') {
      console.debug(
        `${JSON.stringify(caller)}: SQL Result: rows: ${arg.length}, queryTimeMs: ${queryTimeMs}`);
    }
  }
}

module.exports = EventLog;
