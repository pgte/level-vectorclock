var assert = require('assert');
var test   = require('tap').test;
var utils  = require('./utils');

var path = __dirname + '/.testdbs/write_stream'
var db = utils.setup(path, 'node1');

test('writes', function(t) {
  var ws = db.createWriteStream();

  ws.once('finish', function() {
    t.equal(pending, 0);
    t.end();
  });

  var pending = 0;
  for (var i = 0 ; i < 100; i++) {
    pending ++;
    var padded = pad(i);
    var rec = { key: 'key' + padded, value: 'value' + i };
    ws.write(rec, onWrite);
  }

  function onWrite(err) {
    if (err) throw err;
    pending --;
  }

  ws.end();
});

test('data is there after write', function(t) {
  var rs = db.createReadStream();

  rs.on('data', onData);
  rs.once('end', onEnd);

  var i = 0;
  function onData(d) {
    var padded = pad(i);
    var expected = [{ key: 'key' + padded, value: 'value' + i }];
    t.similar(d, expected);
    t.deepEqual(d[0].meta, { clock: { node1: 1}});
    i ++;
  }

  function onEnd() {
    t.equal(i, 100);
    t.end();
  }
});

test('deletes', function(t) {
  var ws = db.createWriteStream({type: 'del'});

  ws.once('finish', function() {
    t.equal(pending, 0);
    t.end();
  });

  var pending = 0;
  for (var i = 0 ; i < 100; i++) {
    pending ++;
    var padded = pad(i);
    var rec = { key: 'key' + padded };
    ws.write(rec, onWrite);
  }

  function onWrite(err) {
    if (err) throw err;
    pending --;
  }

  ws.end();

});

test('no data is there after dels', function(t) {
  var rs = db.createReadStream();

  rs.on('data', onData);
  rs.on('end', onEnd);

  var i = 0;
  function onData(d) {
    i ++;
  }

  function onEnd() {
    t.equal(i, 0);
    t.end();
  }

})

test('closes', function(t) {
  db.close(t.end.bind(t));
});

function pad(n) {
  var s = n.toString();
  if (n < 10)  s = '0' + s;
  if (n < 100) s = '0' + s;
  return s;
}