var assert = require('assert');
var test   = require('tap').test;
var utils  = require('./utils');

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
    if (err) throw err;
    var expected = [{key: 'key1', value: 'value1', meta: { clock: { node1: 1}}}];
    t.deepEqual(recs, expected);
    t.end();
  });
});

test('deletes', function(t) {
  db.del('key1', function(err) {
    if (err) throw err;
    t.end();
  });
});

test('value is no longer present', function(t) {
  db.get('key1', function(err, values) {
    if (err) throw err;
    t.equal(values.length, 0);
    t.end();
  });
});

test('closes', function(t) {
  db.close(t.end.bind(t));
});