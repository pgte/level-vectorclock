var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var level  = require('level');
var LVC    = require('../');

exports.setup = setup;
function setup(dir, node) {
  rimraf.sync(dir);
  mkdirp.sync(dir);
  var db = level(dir);
  return LVC(db, node);
}