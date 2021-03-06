var assert = require('assert');
var test   = require('tap').test;
var utils  = require('./utils');

var MAX_DATA = 100;

var path = __dirname + '/.testdbs/read_stream'
var db = utils.setup(path, 'node1');

var metas = {};

test('puts several', function(t) {
  var i;
  for (i = 1; i <= MAX_DATA; i ++) {
    var padded = pad(i);
    db.put('key' + padded, 'value' + padded, onPut);
  }

  var puts = 0;
  function onPut(err, meta, key) {
    if (err) throw err;
    metas[key] = meta;
    if (++ puts == MAX_DATA) t.end();
  }
});

test('puts several again', function(t) {
  var padded, key, meta;
  for (var i = 1; i <= MAX_DATA; i ++) {
    padded = pad(i);
    key = 'key' + padded;
    meta = metas[key];
    db.put(key, 'value' + padded, meta, onPut);
  }

  var puts = 0;
  function onPut(err) {
    if (err) throw err;
    if (++ puts == MAX_DATA) t.end();
  }
});

test('gets a full read stream', function(t) {
  var s = db.createReadStream();
  s.on('data', onData);

  var datas = 1;
  function onData(d) {
    var padded = pad(datas);
    var expected = {
      key: 'key' + padded,
      value: 'value' + padded,
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(d, [expected]);
    if (++ datas > MAX_DATA) t.end();
  }
});

test('gets a partial read stream', function(t) {
  var s = db.createReadStream({start: 'key020', end: 'key030'});
  s.on('data', onData);

  var datas = 20;
  function onData(d) {
    var padded = pad(datas);
    var expected = {
      key: 'key' + padded,
      value: 'value' + padded,
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(d, [expected]);
    if (++ datas == 30) t.end();
  }

});

test('gets a partial read stream in reverse', function(t) {
  var s = db.createReadStream({start: 'key030', end: 'key020', reverse: true});
  s.on('data', onData);

  var datas = 29;
  function onData(d) {
    var padded = pad(datas);
    var expected = {
      key: 'key' + padded,
      value: 'value' + padded,
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(d, [expected]);
    if (-- datas == 19) t.end();
  }
});

test('gets a partial read stream with a partial key', function(t) {
  var s = db.createReadStream({start: 'key02', end: 'key03'});
  s.on('data', onData);

  var datas = 20;
  function onData(d) {
    var padded = pad(datas);
    var expected = {
      key: 'key' + padded,
      value: 'value' + padded,
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(d, [expected]);
    if (++ datas == 30) t.end();
  }
});

test('gets only keys', function(t) {
  var s = db.createReadStream({values: false});
  s.on('data', onData);

  var datas = 1;
  function onData(d) {
    var padded = pad(datas);
    var expected = 'key' + padded;

    t.deepEqual(d, expected);
    if (++ datas > MAX_DATA) t.end();
  }
});

test('gets only values', function(t) {
  var s = db.createReadStream({keys: false});
  s.on('data', onData);

  var datas = 1;
  function onData(d) {
    var padded = pad(datas);
    var expected = {
      value: 'value' + padded,
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(d, [expected]);
    if (++ datas > MAX_DATA) t.end();
  }
});

test('closes', function(t) {
  db.close(t.end.bind(t));
});


function pad(n) {
  var s = n.toString();
  if (n < 10)  s = '0' + s;
  if (n < 100) s = '0' + s;
  return s;
}