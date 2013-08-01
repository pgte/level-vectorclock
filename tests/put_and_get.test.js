var assert = require('assert');
var test   = require('tap').test;
var utils  = require('./utils');
var LVC    = require('../');

var path = __dirname + '/.testdbs/putandget'
var db = utils.setup(path, 'node1');

test('puts one', function(t) {
  db.put('key1', 'value1', function(err) {
    if (err) throw err;
    t.end();
  });
});

test('gets one', function(t) {
  db.get('key1', function(err, recs) {
    var expected = [{key: 'key1', value: 'value1', meta: { clock: { node1: 1}}}];
    t.deepEqual(recs, expected);
    t.end();
  });
});

test('gets nonexisting key', function(t) {
  db.get('doesnotexist', function(err, recs) {
    t.equal(recs.length, 0);
    t.end();
  });
});

test('produces sibilings', function(t) {
  db.put('key2', 'value2', onPut);
  db.put('key2', 'value3', onPut);

  var put = 0;
  function onPut(err) {
    if (err) throw err;
    if (++ put == 2) next();
  }

  function next() {
    db.get('key2', function(err, recs) {
      if (err) throw err;
      var expected = [
        {key: 'key2', value: 'value2', meta: {clock: {node1: 1}}},
        {key: 'key2', value: 'value3', meta: {clock: {node1: 1}}}
      ];

      t.deepEqual(recs.sort(sort), expected);
      t.end();
    });
  }
});

test('solves descendants', function(t) {
  db.put('key3', 'value4', onPut);

  function onPut(err, meta) {
    if (err) throw err;
    assert(meta);

    db.put('key3', 'value5', meta, onPut2);
  }

  function onPut2(err, meta) {
    if (err) throw err;

    db.get('key3', onGet);
  }

  function onGet(err, recs) {
    if (err) throw err;
    var expected = [{key: 'key3', value: 'value5', meta: { clock: {'node1': 2 }}}];
    t.deepEqual(recs, expected);
    t.end();
  }
});

test('user merges', function(t) {
  db.put('key4', 'value6', onPut);
  db.put('key4', 'value7', onPut);

  var puts = 0;
  function onPut(err) {
    if (err) throw err;
    if (++ puts == 2) next();
  }

  function next() {
    db.get('key4', onGet);
  }

  function onGet(err, values) {
    if (err) throw err;
    t.equal(values.length, 2);
    var metas = values.map(function(value) { return value.meta } );
    db.put('key4', 'value8', metas, onPut2);
  }

  function onPut2(err, meta) {
    if (err) throw err;
    t.deepEqual(meta, {clock: {node1: 2}});
    db.get('key4', onGet2);
  }

  function onGet2(err, values) {
    if (err) throw err;
    t.equal(values.length, 1);
    expectedValue = {
      key: 'key4',
      value: 'value8',
      meta: {
        clock: {
          node1: 2
        }
      }
    };

    t.deepEqual(values, [expectedValue]);
    t.end();
  }

});


test('closes', function(t) {
  db.close(t.end.bind(t));
});

function sort(a, b) {
  if (a.key < b.key) return -1;
  if (a.value < b.value) return -1;
  return 1;
}