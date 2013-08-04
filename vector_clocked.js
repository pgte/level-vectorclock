var EventEmitter  = require('events').EventEmitter;
var inherits      = require('util').inherits;
var extend        = require('util')._extend;
var PassThrough   = require('stream').PassThrough;
var Writable      = require('stream').Writable;
var LWS           = require('level-writestream');
var async         = require('async');
var vectorclock   = require('vectorclock');
var readRepair    = require('./read_repair');

var SEPARATOR =     '\0';
var SEPARATOR_END = '\1';
var PUT = 'put';
var DEL = 'del';

module.exports = VectorClocked;

function VectorClocked(db, node, options) {
  EventEmitter.call(this);

  if (! options) options = {};
  if (! node) throw new Error('need node argument');

  LWS(db);

  this._db      = db;
  this._node    = node;
  this._seed    = options.seed || 0xcafebabe;
}

inherits(VectorClocked, EventEmitter);

var VC = VectorClocked.prototype;


/// put

VC.put = function put(key, value, meta, cb) {
  var self = this;

  if (arguments.length < 4) {
    cb = meta;
    meta = undefined;
  }

  preparePut.call(this, key, value, meta, onValues);

  function onValues(values, meta) {
    self._db.batch(values, onBatch);

    function onBatch(err) {
      if (err) cb(err);
      else cb(null, meta, key);
    }

  }

};

function preparePut(key, value, meta, cb) {

  if (! meta) meta = { clock: {}};
  if (! meta.clock) meta.clock = {};

  if (Array.isArray(meta)) {
    meta = meta.reduce(reduceMetas, {clock: {}});
  }

  vectorclock.increment(meta, this._node);

  var subKey = calcSubKey(meta, this._seed);

  cb([
       {
         key: composeKeys(key, subKey, 'k'),
         value: value, type: PUT },
       {
         key: composeKeys(key, subKey, 'm'),
         value: JSON.stringify(meta), type: PUT }
     ], meta);
}

function reduceMetas(prev, curr) {
  return vectorclock.merge(prev, curr);
}


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
  var batch = (changes.discarded || []).map(delRepairMap);

  if (batch.length)
    this._db.batch(batch, next);
  else next();

  function next(err) {
    if (err) cb(err);
    else cb(null, changes.repaired);
  }
}

function delRepairMap(rec) {
  return { type: DEL, key: rec.key };
}


/// del

VC.del = function del(key, cb) {
  prepareDel.call(this, key, onDelPrepared.bind(this));

  function onDelPrepared(err, commands) {
    if (err) cb(err);
    else this._db.batch(commands, cb);
  }
};

function prepareDel(key, cb) {
  var s = this._db.createReadStream({ start: key + SEPARATOR, end: key +  SEPARATOR_END});
  s.on  ('data', onData);
  s.once('end', onEnd);

  var error;
  var deleteKeys = [];

  function onData(rec) {
    deleteKeys.push(rec.key);
  }

  function onEnd() {
    if (error) cb(error);
    else cb(null, deleteKeys.map(mapKeyToDelete));
  }
}

function mapKeyToDelete(key) {
  return { type: 'del', key: key };
}


/// createReadStream

var defaultReadStreamOptions = {
  keys:   true,
  values: true
}

VC.createReadStream = function createReadStream(options) {
  var self = this;
  var reply = new PassThrough({objectMode: true});

  var opts = extend({}, defaultReadStreamOptions);
  opts = extend(opts, options);

  var mapper;
  var keysOnly   = ! opts.values && opts.keys;
  var valuesOnly = opts.values && ! opts.keys;
  if (valuesOnly)    mapper = valueMapper;
  else if (keysOnly) mapper = keyMapper;

  delete opts.keys;
  delete opts.values;

  var s = this._db.createReadStream(opts);

  s.on('data', onData);
  s.on('end',  onEnd);

  var reads = [];
  var currentKey;
  var currentSet;
  var meta, value;

  function onData(d) {
    var key = extractKey(d.key);
    var set = extractSet(d.key, key);

    if (set != currentSet && meta) {
      reads.push({key: currentKey, value: value, meta: meta});
      value = null;
      meta = null;
    }

    if (set != currentSet) currentSet = set;

    if (currentKey && key != currentKey) dispatch.call(this);

    if (key != currentKey) currentKey = key;

    if (isMetaKey(d.key)) {
      try {
        meta = JSON.parse(d.value);
      } catch(err) {
        reply.emit('error', err);
      }
    } else value = d.value;

  }

  function dispatch() {
    if (reads.length) {
      var instructions = readRepair(reads);
      pushOut(instructions.repaired);
      repair.call(this, instructions, defaultCallback.bind(this));
    }
  }

  function onEnd() {
    if (currentKey && meta && value != undefined) {
      reads.push({key: currentKey, value: value, meta: meta});
    }
    dispatch.call(self);
    reply.push();
  }

  function defaultCallback(err) {
    if (err) reply.emit('error', err);
  }

  function pushOut(out) {
    if (mapper) out = out.map(mapper);
    if (keysOnly) out = out[0];
    if (out) reply.push(out);
  }

  return reply;
};


VC.createValueStream = function createValueStream(options) {
  if (! options) options = {};
  options.values = true;
  options.keys = false;
  return this.createReadStream(options);
}

/// createWriteStream

VC.createWriteStream = function createWriteStream(options) {
  var ws = this._db.createWriteStream(options);

  var s = new Writable({objectMode: true});

  var del = options && options.type == DEL;

  s._write = (del ? delWrite : putWrite).bind(this);

  function putWrite(o, _, cb) {
    preparePut.call(this, o.key, o.value, o.meta, onPutPrepared);

    function onPutPrepared(commands, meta) {
      async.each(commands, ws.write.bind(ws), cb);
    }
  }

  function delWrite(o, _, cb) {
    prepareDel.call(this, o.key, onDelPrepared);

    function onDelPrepared(err, commands) {
      async.each(commands, ws.write.bind(ws), cb);
    }
  }

  ws.on('error', s.emit.bind(s, 'error'));
  s.once('finish', ws.end.bind(ws));

  return s;
};


/// close

VC.close = function close(cb) {
  this._db.close(cb);
};


/// isOpen

VC.isOpen = function isOpen() {
  return this._db.isOpen();
}

/// isClosed

VC.isClosed = function isOpen() {
  return this._db.isClosed();
}



/// Misc

function calcSubKey(meta, seed) {
  // FIXME: get a decent random here
  return Math.floor(Math.random() * 0x80000000).toString(32);
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

function valueMapper(rec) {
  return {value: rec.value, meta: rec.meta};
}

function keyMapper(rec) {
  return rec.key;
}