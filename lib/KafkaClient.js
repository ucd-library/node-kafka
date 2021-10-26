const Kafka = require('node-rdkafka');
const logger = require('./logger');

class KafkaClient {

  constructor(type, globalConfig, topicConfig, opts={}) {
    this.globalConfig = globalConfig;
    this.topicConfig = topicConfig;
    this.opts = opts;
    this._waitForTopics = {};
    this._waitForTopicsInterval = 1000 * 5; // default 5 seconds

    if( type === 'consumer' ) {
      this.client = new Kafka.KafkaConsumer(globalConfig, topicConfig);
    } else if( type === 'producer' ) {
      this.client = new Kafka.Producer(globalConfig, topicConfig);
    } else {
      throw new Error('Unknown KafkaClient type: '+type);
    }

    this.connected = false;
    this.connecting = null; // promise
    if( opts.eventHandlers ) {
      for( let event in opts.eventHandlers ) {
        this.client.on(event, opts.eventHandlers[event]);
      }
    } 
  }

  /**
   * @method connect
   * @description connect client
   * 
   * @param {Object} opts 
   */
  connect(opts={}, autoconnect=true) {
    if( !this.connecting ) {
      this.connecting = new Promise((resolve, reject) => {
        this.connectingResolver = {resolve, reject};
        this._connect(opts, autoconnect);
      });
    }

    return this.connecting;
  }

  _connect(opts, autoconnect) {
    this.client.connect(opts, (err, data) => {

      if( err ) {
        if( autoconnect ) {
          logger.get().error('Failed to connect to kafka, retrying', err);
          return this._connect(opts, autoconnect);
        }
        logger.get().fatal('Failed to connect to kafka', err);
        this.connectingResolver.reject(err);
        return;
      }

      this.connected = true;
      this.connectingResolver.resolve(data);
      this.connecting = null;
      this.connectingResolver = null;
    });
  }

  /**
   * @method disconnect
   * @description disconnect client
   * 
   * @return {Promise}
   */
  disconnect() {
    return new Promise((resolve, reject) => {
      this.client.disconnect((err, data) => {
        if( err ) return reject(err);

        this.connected = false;
        resolve(data);
      });
    });
  }

  getMetadata() {
    return new Promise((resolve, reject) => {
      this.client.getMetadata({}, (err, resp) => {
        if( err ) reject(err);
        else resolve(resp);
      })
    });
  }

  waitForTopics(topics=[]) {
    if( !Array.isArray(topics) ) topics = [topics];
    let id = topics.join(',');
    if( this._waitForTopics[id] ) {
      return this._waitForTopics[id].promise;
    }

    let prr = {id, topics};
    this._waitForTopics[id] = prr;

    prr.promise = new Promise((resolve, reject) => {
      prr.resolve = resolve;
      prr.reject = reject;
      this._checkForTopics(prr);
    });
    return prr.promise;
  }

  async _checkForTopics(ppr) {
    let existingTopics = (await this.getMetadata()).topics.map(topic => topic.name);
    let ready = true;

    for( let topic of ppr.topics ) {
      if( !existingTopics.includes(topic) ) {
        ready = false;
        break;
      }
    }

    if( !ready ) {
      setTimeout(() => this._checkForTopics(ppr), this._waitForTopicsInterval);
      return;
    }

    delete this._checkForTopics[ppr.id];
    ppr.resolve();
  }

  /**
   * @method queryWatermarkOffsets
   * @description get watermark offsets for topic/partition
   * 
   * @param {String|Object} topic 
   */
  queryWatermarkOffsets(topic) {
    if( typeof topic === 'string' ) topic = {topic};
    if( !topic.partition ) topic.partition = 0; 
    
    return new Promise((resolve, reject) => {
      this.client.queryWatermarkOffsets(topic.topic, topic.partition, 10000, (err, offsets) => {
        if( err ) reject(err);
        else resolve(offsets);
      });
    });
  }

  /**
   * @method _topicHelper
   * @description given a topic as a string or object, ensures the topic
   * is a Array or objects that have the partition set to 0.  This structure
   * is how most methods of kafka library expect topics.
   * 
   * @param {Object|String} topic 
   * 
   * @returns {Array}
   */
  _topicHelper(topic) {
    if( typeof topic === 'string' ) topic = {topic};
    if( !Array.isArray(topic) ) topic = [topic];
    
    topic.forEach(t => {
      if( !t.partition ) t.partition = 0; 
    });

    return topic;
  }

}

module.exports = KafkaClient;