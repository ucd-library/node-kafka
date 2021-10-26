const KafkaClient = require('./KafkaClient');

class Producer extends KafkaClient {

  constructor(globalConfig, topicConfig, opts) {
    super('producer', globalConfig, topicConfig, opts);
  }

  /**
   * @method send
   * @description send message.  If message value is object it will be automatically turned
   * into JSON string.  If value is string, it will be automatically turned into Buffer.
   * Sets message timestamp to Date.now()
   * 
   * @param {Object} msg
   * @param {Object|String|Buffer} msg.value message payload
   * @param {String} msg.topic topic to send message to
   * @param {String} msg.key Optional. key for message
   * @param {Number} msg.timestamp Optional. timestamp for message, defaults to Date.now()
   * @param {Number} msg.partition Optional. partition to send to
   * @param {Number} msg.attributes Optional. controls compression of the message set. It supports the following values; 0: No compression, 1: Compress using GZip, 2: Compress using snappy
   */
  produce(msg) {
    if( msg.value !== undefined ) {
      msg.messages = msg.value;
      delete msg.value;
    }

    if( typeof msg.messages === 'object' && !(msg.messages instanceof Buffer)) {
      msg.messages = JSON.stringify(msg.messages);
    }
    if( typeof msg.value === 'string' ) {
      msg.messages = Buffer.from(msg.messages);
    }

    if( !msg.timestamp ) {
      msg.timestamp = Date.now();
    }

    // second param null is to manually set partition, going to always say let kafka decide
    return new Promise((resolve, reject) => {
      this.client.send([msg], e => {
        if( e ) reject(e);
        else resolve();
      });
    });
  }

}

module.exports = Producer;