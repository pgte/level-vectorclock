var vectorClock = require('vectorclock');

module.exports = repair;

function repair(responses) {
  var repaired = [];
  var discarded = [];
  var lastMeta, r;

  responses.sort(sort);
  if (responses.length) {
    r = responses.shift()
    repaired.push(r);
    lastMeta = r.meta;
  }
  if (responses.length) {
    responses.forEach(function(response) {
      if (mustKeep(response.meta, lastMeta)) {
        repaired.push(response);
        lastMeta = response.meta;
      } else discarded.push(response);

    });
  }

  var instructions = { repaired: repaired, discarded: discarded };
  return instructions;
}

function sort(a, b) {
  return vectorClock.descSort(a.meta, b.meta);
}

function mustKeep(a, b) {
  return vectorClock.isConcurrent(a, b);
}