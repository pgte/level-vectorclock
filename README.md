# Level Vector Clock

> LevelDB + Vector Clocks

## Install

```
$ npm install level-vectorclock --save
```

## Use

### Import

```javascript
var LVC   = require('level-vectorclock');
```


### Initialize Database

```javascript
var level = require('level');

var nodeId = 'node-44';
var db = LVC(level('/path/to/my/db'), nodeId);
```

### put(key, value[, meta], cb]);

Without metadata (may create sibilings):

```javascript
db.put('key', 'value', function(err, meta) {
  if (err) throw err;
  console.log('wrote, and here is meta: %j', meta);
});
```

With metadata:


```javascript
db.put('key', 'value', meta, function(err, meta) {
  if (err) throw err;
  console.log('wrote, and here is new meta: %j', meta);
});
```

### get(key, cb);

```javascript
db.get('key', function(err, records) {
  if (err) throw err;

  records.forEach(function(record) {
    console.log('value:', record.value);
    console.log('meta:',  record.meta);
  });
});
```

### Merge two sibilings

Since you may have had two conflicting writes, when you read you may get more than one record.

To resolve the conflict you need to provide all the metadatas of the records you want to resolve into a `put` call like this:


```javascript
var metas = [meta1, meta2];
db.put('key', 'value', metas, function(err, meta) {
  if (err) throw err;
  console.log('new meta: %j', meta);
});
```


### createReadStream(options)

Without options, streams all the data:

```javascript
var s = db.createReadStream();

s.on('data', function(rec) {
  console.log('key:', rec.key);
  console.log('value:', rec.value);
  console.log('meta:', rec.meta);
});
```

With options:

```javascript
var options = {
  start: 'C',
  end: 'A',
  reverse: true,
  values: false,
  keys: true
};

var s = db.createReadStream(options);
```

### createValueStream(options)

Alias for `createReadStream({values: true, keys: false});

### createReadStream(options)

Returns only the keys. Alias for `createReadStream({values: false, keys: true});


### createWriteStream(options)

Creates a writable stream.

Supports [the same options as levelup](https://github.com/rvagg/node-levelup#createWriteStream).

## License

MIT