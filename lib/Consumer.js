const KafkaClient = require('./KafkaClient');
const delay = require('delay');
const logger = require('./logger');


class Consumer extends KafkaClient {

  constructor(globalConfig, topicConfig, opts) {
    super('consumer', globalConfig, topicConfig, opts);


    this.loopTimer = -1;
    this.loopInterval = 500;
    this.consuming = true;
    this.backoff = {
      min: 1000,
      max: 2000
    }
    this.consumeErrorCount = 0;
  }

  /**
   * @method consume
   * @description Attempts to consume one message at a time.  Callback function should
   * return a promise and consume loop will wait until promise resolves to continue.
   * If there are no messages on topic, will pull this.loopInterval (default 500ms)
   * for a message.  If there are message, the next message will be read immediately.
   * 
   * @param {function} callback 
   */
  async consume(callback) {
    while( 1 ) {
      try {
        if( !this.consuming ) break;

        let result = await this.consumeOne();
        this.consumeErrorCount = 0;

        if( result ) await callback(result);
        else await delay(this.loopInterval);
      } catch(e) {
        logger.get().error('kafka consume error', e);
        this.consumeErrorCount++;
        await delay.range(
          this.backoff.min * (this.consumeErrorCount < 10 ? this.consumeErrorCount : 10),
          this.backoff.max * (this.consumeErrorCount < 10 ? this.consumeErrorCount : 10)
        )
      }
    }
  }

  /**
   * @method consumeOne
   * @description attempt to pull one message off topic
   */
  consumeOne() {
    return new Promise((resolve, reject) => {
      this.client.consume(1, (e, msgs) => {
        if( e ) reject(e);
        else if( !msgs.length ) resolve(null);
        else resolve(msgs[0]);
      });
    });
  }


  subscribe(topics) {
    return this.client.subscribe(topics);
  }

  /**
   * @method committed
   * @description get committed offset for topic/partition
   * 
   * @param {String|Object|Array} topic 
   */
  committed(topic, attempt=0) {
    topic = this._topicHelper(topic);

    return new Promise((resolve, reject) => {
      this.client.committed(topic, 10000, (err, result) => {
        if( err && attempt < 10 ) {
          logger.get().warn('Failed to get get committed offset, will try again.  attempt='+attempt, err, result);
          setTimeout(async () => {
            try {
              attempt++;
              resolve(await this.committed(topic, attempt))
            } catch(e) { reject(e) }
          }, 1000);
          return;
        }

        if( err ) reject(err);
        else resolve(result);
      });
    });
  }


}

module.exports = Consumer;