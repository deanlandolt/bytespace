#!/usr/bin/env node

var after = require('after')
var levelup = require('levelup')
var list = require('list-stream')
var inspect = require('util').inspect
var rimraf = require('rimraf')
var space = require('../')
var test = require('tape')

var bytewise = space.bytewise
var testDb  = __dirname + '/__test.db'


function readStreamToList (readStream, cb) {
  readStream.pipe(list.obj(function (err, data) {
    if (err)
      return cb(err)

    data = data.map(function (entry) {
      return [ entry.key, entry.value ]
    })

    cb(null, data)
  }))
}

function dbEquals (ldb, t) {
  return function (expected, cb) {
    readStreamToList(ldb.createReadStream({
      keyEncoding: 'hex'
    }), function (err, data) {
      t.ifError(err, 'no error')
      t.deepEqual(data, expected, 'database contains expected entries')
      cb()
    })
  }
}

function dbWrap (testFn) {
  return function (t) {
    rimraf.sync(testDb)
    levelup(testDb, function (err, ldb) {
      t.ifError(err, 'no error')

      t.$end = t.end
      t.end = function (err) {
        if (err !== undefined)
          t.ifError(err, 'no error')

        ldb.close(function (err) {
          t.ifError(err, 'no error')
          rimraf.sync(testDb)
          t.$end()
        })
      }
      t.dbEquals = dbEquals(ldb, t)

      testFn(t, ldb)
    })
  }
}

function hex(key) {
  return Buffer(key).toString('hex')
}

function nsHex(ns, key) {
  return nsKey(ns, key).toString('hex')
}

function nsKey(ns, key) {
  if (typeof key === 'string')
    key = Buffer(key)

  return Buffer.concat([ bytewise.encode(ns), key ])
}


test('test puts', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bar0'), 'foo0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bar1'), 'foo1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 2' ], 'bar2'), 'foo2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test puts @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1 = space(ldb, 'test space 1')
  var sdb2 = space(ldb, 'test space 2')
  var sdb11 = space(sdb1, 'inner space 1')
  var sdb12 = space(sdb1, 'inner space 2')
  var sdb21 = space(sdb2, 'inner space 1')
  var dbs = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bar0'), 'foo0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bar1'), 'foo1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'bar3'), 'foo3' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'foo3'), 'bar3' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'bar4'), 'foo4' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'foo4'), 'bar4' ],
      [ nsHex([ 'test space 2' ], 'bar2'), 'foo2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'bar5'), 'foo5' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'foo5'), 'bar5' ],
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test gets', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length * 2, t.end)

    dbs.forEach(function (db, i) {
      db.get('foo' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'bar' + i, 'got expected value')
        done()
      })
      db.get('bar' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'foo' + i, 'got expected value')
        done()
      })
    })
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test gets @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1 = space(ldb, 'test space 1')
  var sdb2 = space(ldb, 'test space 2')
  var sdb11 = space(sdb1, 'inner space 1')
  var sdb12 = space(sdb1, 'inner space 2')
  var sdb21 = space(sdb2, 'inner space 1')
  var dbs = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length * 2, t.end)

    dbs.forEach(function (db, i) {
      db.get('foo' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'bar' + i, 'got expected value')
        done()
      })
      db.get('bar' + i, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'foo' + i, 'got expected value')
        done()
      })
    })
  }

  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test dels', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.del('bar' + i, function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test dels @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1 = space(ldb, 'test space 1')
  var sdb2 = space(ldb, 'test space 2')
  var sdb11 = space(sdb1, 'inner space 1')
  var sdb12 = space(sdb1, 'inner space 2')
  var sdb21 = space(sdb2, 'inner space 1')
  var dbs = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
  var done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.del('bar' + i, function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'foo3'), 'bar3' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'foo4'), 'bar4' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'foo5'), 'bar5' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch([
        { type: 'put', key: 'boom' + i, value: 'bang' + i },
        { type: 'del', key: 'bar' + i },
        { type: 'put', key: 'bang' + i, value: 'boom' + i },
      ], function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bang0'), 'boom0' ],
      [ hex('boom0'), 'bang0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bang1'), 'boom1' ],
      [ nsHex([ 'test space 1' ], 'boom1'), 'bang1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 2' ], 'bang2'), 'boom2' ],
      [ nsHex([ 'test space 2' ], 'boom2'), 'bang2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1 = space(ldb, 'test space 1')
  var sdb2 = space(ldb, 'test space 2')
  var sdb11 = space(sdb1, 'inner space 1')
  var sdb12 = space(sdb1, 'inner space 2')
  var sdb21 = space(sdb2, 'inner space 1')
  var dbs = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
  var done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch([
        { type: 'put', key: 'boom' + i, value: 'bang' + i },
        { type: 'del', key: 'bar' + i },
        { type: 'put', key: 'bang' + i, value: 'boom' + i },
      ], function (err) {
        t.ifError(err, 'no error')
        done()
      })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bang0'), 'boom0' ],
      [ hex('boom0'), 'bang0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bang1'), 'boom1' ],
      [ nsHex([ 'test space 1' ], 'boom1'), 'bang1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'bang3'), 'boom3' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'boom3'), 'bang3' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'foo3'), 'bar3' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'bang4'), 'boom4' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'boom4'), 'bang4' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'foo4'), 'bar4' ],
      [ nsHex([ 'test space 2' ], 'bang2'), 'boom2' ],
      [ nsHex([ 'test space 2' ], 'boom2'), 'bang2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'bang5'), 'boom5' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'boom5'), 'bang5' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'foo5'), 'bar5' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test chained batch', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch()
        .put('boom' + i, 'bang' + i)
        .del('bar' + i)
        .put('bang' + i, 'boom' + i)
        .write(function (err) {
          t.ifError(err, 'no error')
          done()
        })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bang0'), 'boom0' ],
      [ hex('boom0'), 'bang0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bang1'), 'boom1' ],
      [ nsHex([ 'test space 1' ], 'boom1'), 'bang1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 2' ], 'bang2'), 'boom2' ],
      [ nsHex([ 'test space 2' ], 'boom2'), 'bang2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('test batch @ multiple levels', dbWrap(function (t, ldb) {
  var sdb1 = space(ldb, 'test space 1')
  var sdb2 = space(ldb, 'test space 2')
  var sdb11 = space(sdb1, 'inner space 1')
  var sdb12 = space(sdb1, 'inner space 2')
  var sdb21 = space(sdb2, 'inner space 1')
  var dbs = [ ldb, sdb1, sdb2, sdb11, sdb12, sdb21 ]
  var done = after(dbs.length * 2, afterPut)

  function afterPut (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length, verify)

    dbs.forEach(function (db, i) {
      db.batch()
        .put('boom' + i, 'bang' + i)
        .del('bar' + i)
        .put('bang' + i, 'boom' + i)
        .write(function (err) {
          t.ifError(err, 'no error')
          done()
        })
    })
  }

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bang0'), 'boom0' ],
      [ hex('boom0'), 'bang0' ],
      [ hex('foo0'), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bang1'), 'boom1' ],
      [ nsHex([ 'test space 1' ], 'boom1'), 'bang1' ],
      [ nsHex([ 'test space 1' ], 'foo1'), 'bar1' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'bang3'), 'boom3' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'boom3'), 'bang3' ],
      [ nsHex([ 'test space 1', 'inner space 1' ], 'foo3'), 'bar3' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'bang4'), 'boom4' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'boom4'), 'bang4' ],
      [ nsHex([ 'test space 1', 'inner space 2' ], 'foo4'), 'bar4' ],
      [ nsHex([ 'test space 2' ], 'bang2'), 'boom2' ],
      [ nsHex([ 'test space 2' ], 'boom2'), 'bang2' ],
      [ nsHex([ 'test space 2' ], 'foo2'), 'bar2' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'bang5'), 'boom5' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'boom5'), 'bang5' ],
      [ nsHex([ 'test space 2', 'inner space 1' ], 'foo5'), 'bar5' ],
    ], t.end)
  }


  dbs.forEach(function (db, i) {
    db.put('foo' + i, 'bar' + i, done)
    db.put('bar' + i, 'foo' + i, done)
  })
}))


test('explicit json valueEncoding', dbWrap(function (t, ldb) {
  var thing = { one: 'two', three: 'four' }
  var opt = { valueEncoding: 'json'}
  var jsonDb = space(ldb, 'json-things', opt)

  jsonDb.put('thing', thing, opt, function (err) {
    t.ifError(err, 'no error')

    jsonDb.get('thing', opt, function (err, got) {
      t.ifError(err, 'no error')
      t.ok(got, 'got something back!')
      t.equal(typeof got, 'object', 'got back an object')
      t.deepEqual(got, thing, 'got back the right thing')
      t.end()
    })
  })
}))


test('explicit json on db valueEncoding raw entry', dbWrap(function (t, ldb) {
  var sdb = space(ldb, 'json-things', { valueEncoding: 'json' })
  var thing = { one: 'two', three: 'four' }

  sdb.put('thing', thing, function (err) {
    t.error(err)

    ldb.get(nsKey([ 'json-things' ], 'thing'), {
      valueEncoding: 'utf8'
    }, function (err, value) {
      t.error(err)
      t.equal(typeof value, 'string')
      t.equal(value, JSON.stringify(thing))
      t.end()
    })
  })
}))


test('explicit json on put valueEncoding raw entry', dbWrap(function (t, ldb) {
  var sdb = space(ldb, 'json-things')
  var thing = { one: 'two', three: 'four' }

  sdb.put('thing', thing, {
    valueEncoding: 'json'
  }, function (err) {
    t.error(err)

    ldb.get(nsKey([ 'json-things' ], 'thing'), {
      valueEncoding: 'utf8'
    }, function (err, value) {
      t.error(err)
      t.equal(typeof value, 'string')
      t.equal(value, JSON.stringify(thing))
      t.end()
    })
  })
}))


test('custom keyEncoding on get', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    var done = after(dbs.length * 3, t.end)

    dbs.forEach(function (db, i) {
      db.get(bytewise.encode([ 'foo', i ]), function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'bar' + i, 'got expected value')
        done()
      })

      db.get([ 'bar', i ], { keyEncoding: bytewise }, function (err, value) {
        t.ifError(err, 'no error')
        t.equal(value, 'foo' + i, 'got expected value')
        done()
      })

      var expected = i > 0 ? [
        [ hex(bytewise.encode([ 'bar', i ])), 'foo' + i ],
        [ hex(bytewise.encode([ 'foo', i ])), 'bar' + i ],
      ] : [
        [ hex(bytewise.encode([ 'bar', 0 ])), 'foo0' ],
        [ hex(bytewise.encode([ 'foo', 0 ])), 'bar0' ],
        [ nsHex([ 'test space 1' ], bytewise.encode([ 'bar', 1 ])), 'foo1' ],
        [ nsHex([ 'test space 1' ], bytewise.encode([ 'foo', 1 ])), 'bar1' ],
        [ nsHex([ 'test space 2' ], bytewise.encode([ 'bar', 2 ])), 'foo2' ],
        [ nsHex([ 'test space 2' ], bytewise.encode([ 'foo', 2 ])), 'bar2' ],
      ]

      dbEquals(db, t)(expected, done)
    })
  }

  dbs.forEach(function (db, i) {
    db.put(bytewise.encode([ 'foo', i ]), 'bar' + i, done)
    db.put(bytewise.encode([ 'bar', i ]), 'foo' + i, { keyEncoding: 'binary' }, done)
  })
}))


// FIXME this one's pretty damn important!
test.skip('custom keyEncoding on put', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2'),
  ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex(bytewise.encode([ 'bar', 0 ])), 'foo0' ],
      [ hex(bytewise.encode([ 'foo', 0 ])), 'bar0' ],
      [ nsHex([ 'test space 1' ], bytewise.encode([ 'bar', 1 ])), 'foo1' ],
      [ nsHex([ 'test space 1' ], bytewise.encode([ 'foo', 1 ])), 'bar1' ],
      [ nsHex([ 'test space 2' ], bytewise.encode([ 'bar', 2 ])), 'foo2' ],
      [ nsHex([ 'test space 2' ], bytewise.encode([ 'foo', 2 ])), 'bar2' ],
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put(bytewise.encode([ 'foo', i ]), 'bar' + i, done)
    db.put([ 'bar', i ], 'foo' + i, { keyEncoding: bytewise }, done)
  })
}))


test('custom keyEncoding on db', dbWrap(function (t, ldb) {
  var dbs = [
    ldb,
    space(ldb, 'test space 1'),
    space(ldb, 'test space 2', { keyEncoding: bytewise }),
  ]
  var done = after(dbs.length * 2, verify)

  function verify (err) {
    t.ifError(err, 'no error')

    t.dbEquals([
      [ hex('bar,0'), 'foo0' ],
      [ hex(bytewise.encode([ 'foo', 0 ])), 'bar0' ],
      [ nsHex([ 'test space 1' ], 'bar,1'), 'foo1' ],
      [ nsHex([ 'test space 1' ], bytewise.encode([ 'foo', 1 ])), 'bar1' ],
      [ nsHex([ 'test space 2' ], bytewise.encode(bytewise.encode([ 'foo', 2 ]))), 'bar2' ],
      [ nsHex([ 'test space 2' ], bytewise.encode([ 'bar', 2 ])), 'foo2' ],
    ], t.end)
  }

  dbs.forEach(function (db, i) {
    db.put(bytewise.encode([ 'foo', i ]), 'bar' + i, done)
    db.put([ 'bar', i ], 'foo' + i, done)
  })
}))


function readStreamTest(options) {
  test('test readStream with ' + inspect(options), function (t) {
    var ref1Db = levelup(testDb + '.ref')
    var ref2Db = levelup(testDb + '.ref2')
    var ldb = levelup(testDb)
    var sdb1 = space(ldb, 'test space')
    var sdb2 = space(sdb1, 'inner space ')
    var ref1List
    var ref2List
    var sdb1List
    var sdb2List
    var done = after(3, prepare)

    ref1Db.on('ready', done)
    ref2Db.on('ready', done)
    ldb.on('ready', done)

    function prepare() {
      var batches = [
        ref1Db.batch(),
        ref2Db.batch(),
        ldb.batch(),
        sdb1.batch(),
        sdb2.batch()
      ]
      var done = after(batches.length, exec)

      for (var i = 0; i < 200; i++) {
        batches.forEach(function (batch) {
          batch.put('key' + i, 'value' + i)
        })
      }

      batches.forEach(function (batch) {
        batch.write(done)
      })
    }

    function exec() {
      var done = after(4, verify)

      readStreamToList(
        ref1Db.createReadStream(options),
        function (err, data) {
          t.ifError(err, 'no error')
          ref1List = data
          done()
        }
      )

      readStreamToList(
        ref2Db.createReadStream(options),
        function (err, data) {
          t.ifError(err, 'no error')
          ref2List = data
          done()
        }
      )

      readStreamToList(
        sdb1.createReadStream(options),
        function (err, data) {
          t.ifError(err, 'no error')
          sdb1List = data
          done()
        }
      )

      readStreamToList(
        sdb2.createReadStream(options),
        function (err, data) {
          t.ifError(err, 'no error')
          sdb2List = data
          done()
        }
      )
    }

    function verify () {
      var done = after(3, function (err) {
        t.ifError(err, 'no error')
        rimraf.sync(testDb)
        rimraf.sync(testDb + '.ref')
        rimraf.sync(testDb + '.ref2')
        t.end()
      })

      t.equal(
        sdb1List.length,
        ref1List.length,
        'test subspace db returned correct number of entries (' + ref1List.length + ')'
      )
      t.deepEqual(
        sdb1List,
        ref1List,
        'test subspace db returned same entries as reference db'
      )     

      t.equal(
        sdb2List.length,
        ref2List.length,
        'inner subspace db returned correct number of entries (' + ref2List.length + ')'
      )
      t.deepEqual(
        sdb2List,
        ref2List,
        'inner subspace db returned same entries as reference db'
      )     

      ref1Db.close(done)
      ref2Db.close(done)
      ldb.close(done)
    }
  })
}


readStreamTest({})
readStreamTest({ start: 'key0', end: 'key50' })
readStreamTest({ start: 'key0', end: 'key150' })
readStreamTest({ gte: 'key0', lte: 'key50' })
readStreamTest({ gt: 'key0', lt: 'key50' })
readStreamTest({ gte: 'key0', lte: 'key150' })
readStreamTest({ gt: 'key0', lt: 'key150' })
readStreamTest({ start: 'key0', end: 'key50' })
readStreamTest({ start: 'key50', end: 'key150' })
readStreamTest({ start: 'key50' })
readStreamTest({ end: 'key50' })
readStreamTest({ gt: 'key50' })
readStreamTest({ gte: 'key50' })
readStreamTest({ lt: 'key50' })
readStreamTest({ lte: 'key50' })
readStreamTest({ reverse: true })
readStreamTest({ start: 'key0', end: 'key50', reverse: true })
readStreamTest({ start: 'key50', end: 'key150', reverse: true })
readStreamTest({ gte: 'key0', lte: 'key50', reverse: true })
readStreamTest({ gt: 'key0', lt: 'key50', reverse: true })
readStreamTest({ gte: 'key0', lte: 'key150', reverse: true })
readStreamTest({ gt: 'key0', lt: 'key150', reverse: true })
readStreamTest({ start: 'key50', reverse: true })
readStreamTest({ end: 'key50', reverse: true })
readStreamTest({ gt: 'key50', reverse: true })
readStreamTest({ gte: 'key50', reverse: true })
readStreamTest({ lt: 'key50', reverse: true })
readStreamTest({ lte: 'key50', reverse: true })
readStreamTest({ limit: 40 })
readStreamTest({ start: 'key0', end: 'key50', limit: 40 })
readStreamTest({ start: 'key50', end: 'key150', limit: 40 })
readStreamTest({ start: 'key50', limit: 40 })
readStreamTest({ reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key50', limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key50', limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key150', limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key150', limit: 40 })
readStreamTest({ start: 'key50', limit: 40 })
readStreamTest({ end: 'key50', limit: 40 })
readStreamTest({ gt: 'key50', limit: 40 })
readStreamTest({ gte: 'key50', limit: 40 })
readStreamTest({ lt: 'key50', limit: 40 })
readStreamTest({ lte: 'key50', limit: 40 })
readStreamTest({ start: 'key0', end: 'key50', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', end: 'key150', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key50', reverse: true, limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key0', lte: 'key150', reverse: true, limit: 40 })
readStreamTest({ gt: 'key0', lt: 'key150', reverse: true, limit: 40 })
readStreamTest({ start: 'key50', reverse: true, limit: 40 })
readStreamTest({ end: 'key50', reverse: true, limit: 40 })
readStreamTest({ gt: 'key50', reverse: true, limit: 40 })
readStreamTest({ gte: 'key50', reverse: true, limit: 40 })
readStreamTest({ lt: 'key50', reverse: true, limit: 40 })
readStreamTest({ lte: 'key50', reverse: true, limit: 40 })
