const bunyan = require('bunyan');
let logger = null;

module.exports = {
  get() {
    if( !logger ) {
      logger = bunyan.createLogger({
        name: '@ucd-lib/node-kafka-default-logger',
        level: 'info',
        streams: [
          { stream: process.stdout }
        ]
      });
    }

    return logger;
  },
  set(_logger) {
    logger = _logger;
  }
}