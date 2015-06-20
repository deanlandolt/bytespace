#!/usr/bin/env node

var after = require('after')
var bytewise = require('bytewise-core')
var encode = bytewise.encode
var levelup = require('levelup')
var list = require('list-stream')
var inspect = require('util').inspect
var rimraf = require('rimraf')
var test = require('tape')
var extend = require('xtend')

var bytespace = require('../')
var testDb  = __dirname + '/__bytespace.db'


test('levelup, buffer namespaces', run.bind(null, levelup, false))
test('levelup, hex namespaces', run.bind(null, levelup, true))
// test('multilevel, buffer namespaces', run.bind(null, ..., false))
// test('multilevel, hex namespaces', run.bind(null, ..., true))

function run(dbFactory, hexNamespace, t) {

  function subspace(db, ns, opts) {
    if (hexNamespace) {
      opts = extend({ hexNamespace: true }, opts)
    }

    return bytespace(db, ns, opts)
  }

  function readStreamToList(readStream, cb) {
    readStream.pipe(list.obj(function (err, data) {
      if (err)
        return cb(err)

      data = data.map(function (entry) {
        if (entry.key || entry.value) {
          return [ entry.key, entry.value ]
        }
        return entry
      })

      cb(null, data)
    }))
  }

  function dbEquals(base, t) {
    return function (expected, cb) {
      readStreamToList(base.createReadStream({
        keyEncoding: 'binary'
      }), function (err, data) {
        t.ifError(err, 'no error')

        var hexed = expected.map(function (kv, i) {
          var d = data[i]
          var dataKey = d[0]
          var expectedKey = kv[0]
          if (typeof expectedKey === 'string') {
            d[0] = String(dataKey)
          }
          else {
            expectedKey = hex(expectedKey)
            d[0] = hexNamespace ? String(dataKey) : hex(dataKey)
          }
          return [ expectedKey, kv[1] ]
        })

        t.deepEqual(data, hexed, 'database contains expected entries')
        cb()
      })
    }
  }

  function dbWrap(dbOpts, testFn) {
    if (typeof dbOpts === 'function') {
      testFn = dbOpts
      dbOpts = undefined
    }

    return function (t) {
      rimraf.sync(testDb)
      levelup(testDb, dbOpts, function (err, base) {
        t.ifError(err, 'no error')

        t.$end = t.end
        t.end = function (err) {
          if (err !== undefined)
            t.ifError(err, 'no error')

          base.close(function (err) {
            t.ifError(err, 'no error')
            rimraf.sync(testDb)
            t.$end()
          })
        }
        t.dbEquals = dbEquals(base, t)

        testFn(t, base)
      })
    }
  }

  function hex(key) {
    return Buffer(key).toString('hex')
  }

  function encodeNs(ns, key) {
    if (typeof key === 'string')
      key = Buffer(key)

    return Buffer.concat([ encode(ns), key ])
  }

  t.test('test puts', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var done = after(dbs.length * 2, verify)

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bar0', 'foo0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bar1'), 'foo1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.bar2'), 'foo2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test puts @ multiple levels', dbWrap(function (t, base) {
    var sdb1 = subspace(base, 'test space 1')
    var sdb2 = subspace(base, 'test space 2')
    var sdb11 = subspace(sdb1, 'inner space 1')
    var sdb12 = subspace(sdb1, 'inner space 2')
    var sdb21 = subspace(sdb2, 'inner space 1')
    var dbs = [ base, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    var done = after(dbs.length * 2, verify)

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bar0', 'foo0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bar1'), 'foo1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.bar3'), 'foo3' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.foo3'), 'bar3' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.bar4'), 'foo4' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.foo4'), 'bar4' ],
        [ encodeNs([ 'test space 2' ], '.bar2'), 'foo2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.bar5'), 'foo5' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.foo5'), 'bar5' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test gets', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
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


  t.test('test gets @ multiple levels', dbWrap(function (t, base) {
    var sdb1 = subspace(base, 'test space 1')
    var sdb2 = subspace(base, 'test space 2')
    var sdb11 = subspace(sdb1, 'inner space 1')
    var sdb12 = subspace(sdb1, 'inner space 2')
    var sdb21 = subspace(sdb2, 'inner space 1')
    var dbs = [ base, sdb1, sdb2, sdb11, sdb12, sdb21 ]
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


  t.test('test dels', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.del('.bar' + i, function (err) {
          t.ifError(err, 'no error')
          done()
        })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test dels @ multiple levels', dbWrap(function (t, base) {
    var sdb1 = subspace(base, 'test space 1')
    var sdb2 = subspace(base, 'test space 2')
    var sdb11 = subspace(sdb1, 'inner space 1')
    var sdb12 = subspace(sdb1, 'inner space 2')
    var sdb21 = subspace(sdb2, 'inner space 1')
    var dbs = [ base, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.del('.bar' + i, function (err) {
          t.ifError(err, 'no error')
          done()
        })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.foo3'), 'bar3' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.foo4'), 'bar4' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.foo5'), 'bar5' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test batch', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch([
          { type: 'put', key: '.boom' + i, value: 'bang' + i },
          { type: 'del', key: '.bar' + i },
          { type: 'put', key: '.bang' + i, value: 'boom' + i },
        ], function (err) {
          t.ifError(err, 'no error')
          done()
        })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bang1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.boom1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test batch @ multiple levels', dbWrap(function (t, base) {
    var sdb1 = subspace(base, 'test space 1')
    var sdb2 = subspace(base, 'test space 2')
    var sdb11 = subspace(sdb1, 'inner space 1')
    var sdb12 = subspace(sdb1, 'inner space 2')
    var sdb21 = subspace(sdb2, 'inner space 1')
    var dbs = [ base, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch([
          { type: 'put', key: '.boom' + i, value: 'bang' + i },
          { type: 'del', key: '.bar' + i },
          { type: 'put', key: '.bang' + i, value: 'boom' + i },
        ], function (err) {
          t.ifError(err, 'no error')
          done()
        })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bang1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.boom1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.bang3'), 'boom3' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.boom3'), 'bang3' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.foo3'), 'bar3' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.bang4'), 'boom4' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.boom4'), 'bang4' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.foo4'), 'bar4' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.bang5'), 'boom5' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.boom5'), 'bang5' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.foo5'), 'bar5' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test chained batch', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch()
          .put('.boom' + i, 'bang' + i)
          .del('.bar' + i)
          .put('.bang' + i, 'boom' + i)
          .write(function (err) {
            t.ifError(err, 'no error')
            done()
          })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bang1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.boom1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('test batch @ multiple levels', dbWrap(function (t, base) {
    var sdb1 = subspace(base, 'test space 1')
    var sdb2 = subspace(base, 'test space 2')
    var sdb11 = subspace(sdb1, 'inner space 1')
    var sdb12 = subspace(sdb1, 'inner space 2')
    var sdb21 = subspace(sdb2, 'inner space 1')
    var dbs = [ base, sdb1, sdb2, sdb11, sdb12, sdb21 ]
    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch()
          .put('.boom' + i, 'bang' + i)
          .del('.bar' + i)
          .put('.bang' + i, 'boom' + i)
          .write(function (err) {
            t.ifError(err, 'no error')
            done()
          })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bang1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.boom1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.foo1'), 'bar1' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.bang3'), 'boom3' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.boom3'), 'bang3' ],
        [ encodeNs([ 'test space 1', 'inner space 1' ], '.foo3'), 'bar3' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.bang4'), 'boom4' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.boom4'), 'bang4' ],
        [ encodeNs([ 'test space 1', 'inner space 2' ], '.foo4'), 'bar4' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.bang5'), 'boom5' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.boom5'), 'bang5' ],
        [ encodeNs([ 'test space 2', 'inner space 1' ], '.foo5'), 'bar5' ],
      ], t.end)
    }


    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })
  }))


  t.test('explicit json valueEncoding', dbWrap(function (t, base) {
    var thing = { one: 'two', three: 'four' }
    var opt = { valueEncoding: 'json'}
    var jsonDb = subspace(base, 'json-things', opt)

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


  t.test('read stream on explicit json valueEncoding', dbWrap(function (t, base) {
    var sdb = subspace(base, 'json-things', { valueEncoding: 'json' })
    var v = { an: 'object' }

    sdb.put('k', v, function (err) {
      t.error(err)

      readStreamToList(sdb.createReadStream(), function(err, data) {
        t.error(err)
        t.deepEqual(data, [ [ 'k', v ] ])
        t.end()
      })
    })
  }))


  t.test('value stream on explicit json valueEncoding', dbWrap(function (t, base) {
    var sdb = subspace(base, 'json-things', { valueEncoding: 'json' })
    var v = { an: 'object' }

    sdb.put('k', v, function (err) {
      t.error(err)

      readStreamToList(sdb.createValueStream(), function(err, data) {
        t.error(err)
        t.deepEqual(data, [ v ])
        t.end()
      })
    })
  }))


  t.test('explicit json on base db valueEncoding', dbWrap({
    valueEncoding: 'json'
  }, function (t, base) {
    var thing = { one: 'two', three: 'four' }
    var opt = {}
    var jsonDb = subspace(base, 'json-things', opt)

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


  t.test('explicit json on base db valueEncoding, iterator', dbWrap({
    valueEncoding: 'json'
  }, function (t, base) {
    var thing = { one: 'two', three: 'four' }
    var opt = {}
    var jsonDb = subspace(base, 'json-things', opt)

    jsonDb.put('thing', thing, opt, function (err) {
      t.ifError(err, 'no error')

      readStreamToList(
        jsonDb.createReadStream(opt),
        function (err, data) {
          t.ifError(err, 'no error')
          t.equal(data.length, 1)

          t.equal(data[0][0], 'thing')
          var got = data[0][1]
          t.ok(got, 'got something back!')
          t.equal(typeof got, 'object', 'got back an object')
          t.deepEqual(got, thing, 'got back the right thing')
          t.end()
        }
      )
    })
  }))


  t.test('explicit json on db valueEncoding raw entry', dbWrap(function (t, base) {
    var sdb = subspace(base, 'json-things', { valueEncoding: 'json' })
    var thing = { one: 'two', three: 'four' }

    sdb.put('thing', thing, function (err) {
      t.error(err)

      var key = encodeNs([ 'json-things' ], 'thing')
      base.get(hexNamespace ? hex(key) : key, {
        valueEncoding: 'utf8'
      }, function (err, value) {
        t.error(err)
        t.equal(typeof value, 'string')
        t.equal(value, JSON.stringify(thing))
        t.end()
      })
    })
  }))


  t.test('explicit json on put valueEncoding raw entry', dbWrap(function (t, base) {
    var sdb = subspace(base, 'json-things')
    var thing = { one: 'two', three: 'four' }

    sdb.put('thing', thing, {
      valueEncoding: 'json'
    }, function (err) {
      t.error(err)

      var key = encodeNs([ 'json-things' ], 'thing')
      base.get(hexNamespace ? hex(key) : key, {
        valueEncoding: 'utf8'
      }, function (err, value) {
        t.error(err)
        t.equal(typeof value, 'string')
        t.equal(value, JSON.stringify(thing))
        t.end()
      })
    })
  }))


  t.test('nested value encodings, utf8 on top', dbWrap({
    valueEncoding: 'json'
  }, function (t, base) {
    var sp1 = subspace(base, 'sp1', { valueEncoding: 'utf8' })
    var sp2 = subspace(sp1, 'sp2', { valueEncoding: 'json' })
    var sp3 = subspace(sp2, 'sp3', { valueEncoding: 'utf8' })
    var v = '{"an":"object"}'
    sp3.put('k', v, function (err) {
      t.error(err)
      sp3.get('k', function (err, value) {
        t.error(err)
        t.equal(typeof value, 'string')
        t.equal(value, v)
        t.end()
      })
    })
  }))


  t.test('nested value encodings, json on top', dbWrap({
    valueEncoding: 'json'
  }, function (t, base) {
    var sp1 = subspace(base, 'sp1', { valueEncoding: 'utf8' })
    var sp2 = subspace(sp1, 'sp2', { valueEncoding: 'json' })
    var sp3 = subspace(sp2, 'sp3', { valueEncoding: 'utf8' })
    var sp4 = subspace(sp3, 'sp4', { valueEncoding: 'json' })
    var v = { an: 'object' }
    sp4.put('k', v, function (err) {
      t.error(err)
      sp4.get('k', function (err, value) {
        t.error(err)
        t.equal(typeof value, 'object')
        t.deepEqual(value, v)
        t.end()
      })
    })
  }))


  t.test('nested value encodings, override', dbWrap({
    valueEncoding: 'json'
  }, function (t, base) {
    var sp1 = subspace(base, 'sp1', { valueEncoding: 'utf8' })
    var sp2 = subspace(sp1, 'sp2', { valueEncoding: 'json' })
    var sp3 = subspace(sp2, 'sp3', { valueEncoding: 'utf8' })
    var v   = { an: 'object' }
    sp3.put('k', v, { valueEncoding: 'json' }, function (err) {
      t.error(err)
      sp3.get('k', { valueEncoding: 'json' }, function (err, value) {
        t.error(err)
        t.equal(typeof value, 'object')
        t.deepEqual(value, v)
        t.end()
      })
    })
  }))


  t.test('custom keyEncoding on get', dbWrap(function (t, base) {
    // skip get tests for hex mode
    if (hexNamespace) {
      return t.end()
    }
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var done = after(dbs.length * 2, verify)

    function verify (err) {
      t.ifError(err, 'no error')

      var done = after(dbs.length * 5, t.end)

      dbs.forEach(function (db, i) {

        db.get(encode([ '.foo', i ]), function (err, value) {
          t.ifError(err, 'no error')
          t.equal(value, 'bar' + i, 'got expected value')
          done()
        })

        db.get([ '.foo', i ], { keyEncoding: bytewise }, function (err, value) {
          t.ifError(err, 'no error')
          t.equal(value, 'bar' + i, 'got expected value')
          done()
        })

        db.get(encode([ '.bar', i ]), function (err, value) {
          t.ifError(err, 'no error')
          t.equal(value, 'foo' + i, 'got expected value')
          done()
        })

        db.get([ '.bar', i ], { keyEncoding: bytewise }, function (err, value) {
          t.ifError(err, 'no error')
          t.equal(value, 'foo' + i, 'got expected value')
          done()
        })

        var possibilities = [ [
          [ encode([ '.bar', 0 ]), 'foo0' ],
          [ encode([ '.foo', 0 ]), 'bar0' ],
          [ encodeNs([ 'test space 1' ], encode([ '.bar', 1 ])), 'foo1' ],
          [ encodeNs([ 'test space 1' ], encode([ '.foo', 1 ])), 'bar1' ],
          [ encodeNs([ 'test space 2' ], encode([ '.bar', 2 ])), 'foo2' ],
          [ encodeNs([ 'test space 2' ], encode([ '.foo', 2 ])), 'bar2' ],
        ], [
          [ encode([ '.bar', i ]), 'foo' + i ],
          [ encode([ '.foo', i ]), 'bar' + i ],
        ], [
          [ encode([ '.bar', i ]), 'foo' + i ],
          [ encode([ '.foo', i ]), 'bar' + i ],
        ] ]

        var expected = possibilities[i]
        dbEquals(db, t)(expected, done)
      })
    }

    dbs.forEach(function (db, i) {
      db.put(encode([ '.foo', i ]), 'bar' + i, done)
      db.put(encode([ '.bar', i ]), 'foo' + i, { keyEncoding: 'binary' }, done)
    })
  }))


  t.test('custom keyEncoding on put', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var done = after(dbs.length * 2, verify)

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ encode([ '.bar', 0 ]), 'foo0'],
        [ encode([ '.foo', 0 ]), 'bar0' ],
        [ encodeNs([ 'test space 1' ], encode([ '.bar', 1 ])), 'foo1' ],
        [ encodeNs([ 'test space 1' ], encode([ '.foo', 1 ])), 'bar1' ],
        [ encodeNs([ 'test space 2' ], encode([ '.bar', 2 ])), 'foo2' ],
        [ encodeNs([ 'test space 2' ], encode([ '.foo', 2 ])), 'bar2' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      var k0 = encode([ '.foo', i ])
      var k1 = [ '.bar', i ]
      var opts1 = { keyEncoding: bytewise }

      // special treatment for base level keys in hex tests
      if (!i && hexNamespace) {
        k0 = hex(k0)
        k1 = hex(encode(k1))
        opts1.keyEncoding = 'utf8'
      }

      db.put(k0, 'bar' + i, done)
      db.put(k1, 'foo' + i, opts1, done)
    })
  }))


  t.test('custom keyEncoding on db', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2', { keyEncoding: bytewise }),
    ]

    var done = after(dbs.length * 2, verify)

    function verify (err) {
      t.ifError(err, 'no error')

      t.dbEquals([
        [ '.bar,0', 'foo0' ],
        [ encode([ '.foo', 0 ]), 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.bar,1'), 'foo1' ],
        [ encodeNs([ 'test space 1' ], encode([ '.foo', 1 ])), 'bar1' ],
        [ encodeNs([ 'test space 2' ], encode(encode([ '.foo', 2 ]))), 'bar2' ],
        [ encodeNs([ 'test space 2' ], encode([ '.bar', 2 ])), 'foo2' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      var opts = {}
      var k0 = encode([ '.foo', i ])
      var k1 = [ '.bar', i ]

      // special treatment for base level keys in hex tests
      if (!i && hexNamespace) {
        opts.keyEncoding = 'utf8'
        k0 = hex(k0)
        k1 = String(k1)
      }

      db.put(k0, 'bar' + i, opts, done)
      db.put(k1, 'foo' + i, opts, done)
    })
  }))


  function readStreamTest(options) {
    t.test('test readStream with ' + inspect(options), function (t) {
      var ref1Db = levelup(testDb + '.ref')
      var ref2Db = levelup(testDb + '.ref2')
      var base = levelup(testDb)
      var sdb1 = subspace(base, 'test space')
      var sdb2 = subspace(sdb1, 'inner space ')
      var ref1List
      var ref2List
      var sdb1List
      var sdb2List
      var done = after(3, prepare)

      ref1Db.on('ready', done)
      ref2Db.on('ready', done)
      base.on('ready', done)

      function prepare() {
        var batches = [
          ref1Db.batch(),
          ref2Db.batch(),
          base.batch(),
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
        base.close(done)
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


  t.test('precommit hooks', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var calls = [ 0, 0, 0 ]

    //
    // add pre hooks
    //
    dbs[1].pre(function (op, add, ops) {
      t.equal(typeof op.key, 'string')
      calls[1]++
      op.key = op.key.toUpperCase()
    })
    dbs[2].pre(function (op, add, ops) {
      t.equal(typeof op.key, 'string')
      calls[2]++
      op = extend(op)
      op.key += ' xxx'
      add(op)
    })

    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')
      t.deepEqual(calls, [ 0, 2, 2 ])

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch([
          { type: 'put', key: '.boom' + i, value: 'bang' + i },
          { type: 'del', key: '.bar' + i },
          { type: 'put', key: '.bang' + i, value: 'boom' + i },
        ], done)
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.deepEqual(calls, [ 0, 5, 5 ])

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.BANG1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.BOOM1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.FOO1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.bang2 xxx'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.boom2 xxx'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2' ], '.foo2 xxx'), 'bar2' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })

  }))


  t.test('precommit hooks, chained batches', dbWrap(function (t, base) {
    var dbs = [
      base,
      subspace(base, 'test space 1'),
      subspace(base, 'test space 2'),
    ]
    var calls = [ 0, 0, 0 ]

    //
    // add pre hooks
    //
    dbs[1].pre(function (op, add, ops) {
      t.equal(typeof op.key, 'string')
      calls[1]++
      op.key = op.key.toUpperCase()
    })
    dbs[2].pre(function (op, add, ops) {
      t.equal(typeof op.key, 'string')
      calls[2]++
      op = extend(op)
      op.key += ' xxx'
      add(op)
    })

    var done = after(dbs.length * 2, afterPut)

    function afterPut (err) {
      t.ifError(err, 'no error')
      t.deepEqual(calls, [ 0, 2, 2 ])

      var done = after(dbs.length, verify)

      dbs.forEach(function (db, i) {
        db.batch()
          .put('.boom' + i, 'bang' + i)
          .del('.bar' + i)
          .put('.bang' + i, 'boom' + i)
          .write(function (err) {
            t.ifError(err, 'no error')
            done()
          })
      })
    }

    function verify (err) {
      t.ifError(err, 'no error')

      t.deepEqual(calls, [ 0, 5, 5 ])

      t.dbEquals([
        [ '.bang0', 'boom0' ],
        [ '.boom0', 'bang0' ],
        [ '.foo0', 'bar0' ],
        [ encodeNs([ 'test space 1' ], '.BANG1'), 'boom1' ],
        [ encodeNs([ 'test space 1' ], '.BOOM1'), 'bang1' ],
        [ encodeNs([ 'test space 1' ], '.FOO1'), 'bar1' ],
        [ encodeNs([ 'test space 2' ], '.bang2'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.bang2 xxx'), 'boom2' ],
        [ encodeNs([ 'test space 2' ], '.boom2'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.boom2 xxx'), 'bang2' ],
        [ encodeNs([ 'test space 2' ], '.foo2'), 'bar2' ],
        [ encodeNs([ 'test space 2' ], '.foo2 xxx'), 'bar2' ],
      ], t.end)
    }

    dbs.forEach(function (db, i) {
      db.put('.foo' + i, 'bar' + i, done)
      db.put('.bar' + i, 'foo' + i, done)
    })

  }))
}
