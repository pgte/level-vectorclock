var EventEmitter = require('events').EventEmitter;
var inherits     = require('util').inherits;
var PassThrough  = require('stream').PassThrough;
var vectorclock  = require('vectorclock');
var hash         = require('xxhash').hash;
var readRepair   = require('./read_repair');

var SEPARATOR =     '\0';
var SEPARATOR_END = '\1';
var PUT = 'put';
var DEL = 'del';

module.exports = VectorClocked;

function VectorClocked(db, node, options) {
  EventEmitter.call(this);

  if (! options) options = {};

  if (! node) throw new Error('need node argument');
  this._db      = db;
  this._node    = node;
  this._seed    = options.seed || 0xcafebabe;
}

inherits(VectorClocked, EventEmitter);

var VC = VectorClocked.prototype;


/// put

VC.put = function put(key, value, meta, cb) {
  if (arguments.length < 4) {
    cb = meta;
    meta = undefined;
  }

  if (! meta) meta = {};
  if (! meta.clock) meta.clock = {};
  vectorclock.increment(meta.clock, this._node);
  var subKey = calcSubKey(meta, this._seed);
  this._db.batch([
      { key: composeKeys(key, subKey, 'k'), value: value, type: PUT },
      { key: composeKeys(key, subKey, 'm'), value: JSON.stringify(meta), type: PUT }
    ], onBatch);

  function onBatch(err) {
    if (err) cb(err);
    else cb(null, meta);
  }
};


/// get

VC.get = function get(key, cb) {
  var self = this;
  var s = this._db.createReadStream({ start: key, end: key +  SEPARATOR_END});
  s.on  ('data', onData);
  s.once('end', onEnd);

  var error;
  var value, meta;
  var reads = [];

  function onData(rec) {
    var k = rec.key;
    if (! value) value = rec.value;
    else {
      try {
        meta = JSON.parse(rec.value);
      } catch(err) {
        error = err;
        self.emit('error', err);
      }
      reads.push({key: key, value: value, meta: meta});
      meta = value = undefined;
    }
  }

  function onEnd() {
    if (error) cb(error);
    else {
      repair.call(self, readRepair(reads), cb);
    }
  }

};

function repair(changes, cb) {
  var batch = (changes.discarded || []).map(function(discardRec) {
    return { type: DEL, key: discardRec.key }
  });

  if (batch.length)
    this._db.batch(batch, next);
  else next();

  function next(err) {
    if (err) cb(err);
    else cb(null, changes.repaired);
  }
}


/// createReadStream

VC.createReadStream = function createReadStream(options) {
  var reply = new PassThrough({objectMode: true});

  var s = this._db.createReadStream(options);


  s.on('data', onData);
  s.on('end',  onEnd);

  var reads = [];
  var currentKey;
  var currentSet;
  var meta, value;

  function onData(d) {
    var key = extractKey(d.key);
    var set = extractSet(d.key, key);

    if (currentSet && set != currentSet && meta) {
      reads.push({key: currentKey, value: value, meta: meta});
      value = null;
      meta = null;
    }

    if (set != currentSet) currentSet = set;

    if (currentKey && key != currentKey)
      dispatch();

    if (key != currentKey) currentKey = key;

    if (isMetaKey(d.key)) {
      try {
        meta = JSON.parse(d.value);
      } catch(err) {
        reply.emit('error', err);
      }
    } else {
      value = d.value;
    }

  }

  function dispatch() {
    if (reads.length) {
      var instructions = readRepair(reads);
      reply.push(instructions.repaired);
      repair.call(this, instructions, defaultCallback.bind(this));
    }
  }

  function onEnd() {
    if (currentKey && meta && value != undefined) {
      reads.push({key: currentKey, value: value, meta: meta});
    }
    dispatch();
    reply.push();
  }

  function defaultCallback(err) {
    if (err) reply.emit('error', err);
  }

  return reply;
};

/// close

VC.close = function close(cb) {
  this._db.close(cb);
};


function calcSubKey(meta, seed) {
  // FIXME: get a decent random here
  return hash(new Buffer(JSON.stringify(meta) + Date.now().toString() + Math.random().toString()), seed).toString(32);
}

function composeKeys(keyA, prefix, keyB) {
  return keyA + SEPARATOR + prefix + SEPARATOR + keyB;
}

function extractKey(fullKey) {
  return fullKey.substring(0, fullKey.indexOf(SEPARATOR));
}

function extractSet(fullKey, key) {
  return fullKey.substring(key.length + 1, fullKey.lastIndexOf(SEPARATOR));
}

function isMetaKey(fullKey) {
  return fullKey.substring(fullKey.lastIndexOf(SEPARATOR) + 1) == 'm';
}