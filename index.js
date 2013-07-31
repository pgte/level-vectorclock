module.exports = wrap;

var VectorClocked = require('./vector_clocked');

function wrap(db, node, options) {
  return new VectorClocked(db, node, options);
}