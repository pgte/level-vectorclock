var test = require('tap').test;
var utils = require('./utils');
var LVC = require('../');

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
    var expected = [{key: 'key1', value: 'value1', meta: {node1: 1}}];
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
        {key: 'key2', value: 'value2', meta: {node1: 1}},
        {key: 'key2', value: 'value3', meta: {node1: 1}}
      ];

      t.deepEqual(recs.sort(sort), expected);
      t.end();
    });
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