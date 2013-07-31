var vectorClock = require('vectorclock');

module.exports = repair;

function repair(responses) {
  var repaired = [];
  var discarded = [];
  if (responses.length) repaired.push(responses.shift());
  if (responses.length) {
    responses.sort(sort);
    responses.forEach(function(response) {
      if (vectorClock.isConcurrent(response, repaired[0]) &&
          ! vectorClock.isIdentical(response, repaired[0])) {
        repaired.push(response);
      } else {
        discarded.push(response);
      }
    });
  }

  var ret = { repaired: repaired, discarded: discarded }
  return ret;
}

function sort(resA, resB) {
  return vectorClock.descSort(resA.meta, resB.meta);
}