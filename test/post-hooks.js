var levelup = require('levelup')
var memdown = require('memdown')
var test = require('tape')
var bytespace = require('../')

function factory() {
  return bytespace(levelup(memdown))
}

test('post hooks, multiple namespaces', function(t){
  t.plan(10)

  var db = factory()
  var sub1 = db.sublevel('sub1')
  var sub2 = db.sublevel('sub2')
  var called = []
  var expected = ['root', 'sub1', 'sub2']

  db.post(function(op){
    called.push(op.key)
    t.is(op.key, 'root', 'root hook')
  })

  sub1.post(function(op){
    called.push(op.key)
    t.is(op.key, 'sub1', 'sub1 hook')
  })

  sub2.post(function(op){
    called.push(op.key)
    t.is(op.key, 'sub2', 'sub2 hook')
  })

  db.batch([
    { key: 'root', value: 'a' },
    { key: 'sub1', prefix: sub1, value: 'b' },
    { key: 'sub2', prefix: sub2, value: 'c' }
  ], function(err){
    t.ifError(err, 'no error')
    t.same(called, expected, 'order')

    called = []

    sub2.batch([
      { key: 'root', prefix: db, value: 'a' },
      { key: 'sub1', prefix: sub1, value: 'b' },
      { key: 'sub2', prefix: sub2, value: 'c' }
    ], function(err){
      t.ifError(err, 'no error')
      t.same(called, expected, 'order')
    })
  })
})

test('post hook throwing error', function(t) {
  t.plan(1)

  var db = factory()

  db.post(function(op){
    throw new Error('beep')
  })

  db.put('a', 'a', function(err){
    t.is(err.message, 'beep')
  })
})
