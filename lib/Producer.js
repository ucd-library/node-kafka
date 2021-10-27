const KafkaClient = require('./KafkaClient');

class Producer extends KafkaClient {

  constructor(globalConfig, topicConfig, opts) {
    super('producer', globalConfig, topicConfig, opts);
  }

  /**
   * @method produce
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
   */
  produce(msg) {
    if( typeof msg.value === 'object' && !(msg.value instanceof Buffer)) {
      msg.value = JSON.stringify(msg.value);
    }
    if( typeof msg.value === 'string' ) {
      msg.value = Buffer.from(msg.value);
    }

    this.client.produce(msg.topic, msg.partition || null, msg.value, msg.key, msg.timestamp)
    // this.client.send([msg], e => {
    //   if( e ) reject(e);
    //   else resolve();
    // });
  }

}

module.exports = Producer;