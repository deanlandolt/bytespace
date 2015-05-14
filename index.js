var assert = require('assert')
var bytewise = require('bytewise-core')
var domain = require('domain')
var equal = require('bytewise-core/util').equal
var levelup = require('levelup')
var updown = require('level-updown')
var xtend = require('xtend')

//
// brand prefix instance to keep track of subspace root
//
function Prefix(path) {
  this.path = path
  this.buffer = bytewise.encode(path)
}

Prefix.prototype.append = function (namespace) {
  return new Prefix(this.path.concat(namespace))
}

Prefix.prototype.contains = function (key) {
  //
  // slice to get key prefix
  //
  return equal(this.buffer, key.slice(0, this.buffer.length))
}

Prefix.prototype.decode = function (key) {
  assert(Buffer.isBuffer(key))

  if (!this.contains(key))
    return key

  //
  // slice prefix and return
  //
  return key.slice(this.buffer.length)
}

Prefix.prototype.encode = function (key) {
  if (typeof key === 'string')
    key = new Buffer(key)

  return Buffer.concat([ this.buffer, key ])
}

//
// create a bytespace given a provided levelup instance
//
function bytespace(db, namespace, options) {
  var prefix = namespace
  if (!(prefix instanceof Prefix)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (typeof db.subspace === 'function')
      return db.subspace(namespace, options)
    
    //
    // otherwise this is a top level subspace
    //
    prefix = new Prefix([ namespace ])
  }

  options || (options = {})

  prefix.precommit = options.precommit
  prefix.postcommit = options.postcommit

  function factory() {
    var base = updown(db)
    prefix.batch = base._batch.bind(base)

    base.extendWith({
      prePut: prePut.bind(prefix),
      preDel: preDel.bind(prefix),
      preBatch: preBatch.bind(prefix),
      preGet: preGet.bind(prefix),
      postGet: postGet.bind(prefix),
      preIterator: preIterator.bind(prefix)
    })

    return base
  }

  options.db = factory

  var space = levelup(options)

  space.subspace = function (namespace, options) {
    return bytespace(db, prefix.append(namespace), options)
  }

  space.clone = function (namespace, options) {
    return bytespace(db, prefix, options)
  }

  return space
}


function prePut(key, value, options, cb, next) {
  var ops = [{ type: 'put', key: key, value: value }]

  //
  // only run hooks if we aren't currently within a transaction context
  //
  var tx = domain.active
  var unscoped = !tx
  if (unscoped) {
    tx = domain.create()
    tx.on('error', function (err) {
      tx.abort || (tx.abort = err)
    })
  }

  var prefix = this
  tx.run(function () {
    ops = (unscoped && prefix.precommit) ? prefix.precommit(ops) : ops

    prefix.batch(ops, options, function (err) {
      unscoped && tx.exit()
      cb(err)
    })
  })
}


function preDel(key, options, cb, next) {
  var ops = [{ type: 'del', key: key, value: null }]

  //
  // only run hooks if we aren't currently within a transaction context
  //
  var tx = domain.active
  var unscoped = !tx
  if (unscoped) {
    tx = domain.create()
    tx.on('error', function (err) {
      tx.abort || (tx.abort = err)
    })
  }

  var prefix = this
  tx.run(function () {
    ops = (unscoped && prefix.precommit) ? prefix.precommit(ops) : ops

    prefix.batch(ops, options, function (err) {
      unscoped && tx.exit()
      cb(err)
    })
  })
}


function preBatch(ops, options, cb, next) {
  //
  // only run hooks if we aren't currently within a transaction context
  //
  var tx = domain.active
  var unscoped = !tx
  if (unscoped) {
    tx = domain.create()
    tx.on('error', function (err) {
      tx.abort || (tx.abort = err)
    })
  }

  var prefix = this
  tx.run(function () {
    ops = (unscoped && prefix.precommit) ? prefix.precommit(ops) : ops
    if (tx.abort) {
      tx.exit()
      return cb(tx.abort)
    }

    options.keyEncoding = 'binary'
    var array = ops
    var op

    if (Array.isArray(ops)) {
      array = []
      for (var i = 0, length = ops.length; i < length; i++) {
        op = array[i] = xtend(ops[i])
        op.key = prefix.encode(op.key)
      }
    }

    next(array, options, function (err) {
      unscoped && tx.exit()
      cb(err)
    })
  })
}


function preGet(key, options, cb, next) {
  options.keyEncoding = 'binary'
  next(this.encode(key), options, cb)
}


function postGet(key, options, err, value, cb, next) {
  next(this.decode(key), options, err, value, cb)
}


function postNext(keyAsBuffer, err, key, value, cb, next) {
  //
  // pass through errors and null calls for end-of-iterator
  //
  if (err || key == null)
    return next(err, key, value, cb)

  key = this.decode(key)
  next(err, err ? key : keyAsBuffer ? key : key.toString('utf8'), value, cb)
}


var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])
var rangeKeys = [ 'start', 'end', 'gt', 'lt', 'gte', 'lte' ]

function preIterator(pre) {
  var options = xtend(pre.options)
  var keyAsBuffer = options.keyAsBuffer
  options.keyAsBuffer = true

  var has = {}
  rangeKeys.forEach(function (key) {
    has[key] = key in options
  })

  if (has.start || has.end) {
    if (has.gt || has.lt || has.gte || has.lte) {
      // TODO: updown sending phantom start value -- bug?
    }
    else {
      if (!options.reverse) {
        options.gte = has.start ? options.start : LOWER_BOUND
        options.lte = has.end ? options.end : UPPER_BOUND
      }
      else {
        options.gte = has.end ? options.end : LOWER_BOUND
        options.lte = has.start ? options.start : UPPER_BOUND
      }

      has.gte = has.lte = true
      delete options.start
      delete options.end
    }
  }

  if (has.gt) {
    options.gt = this.encode(options.gt, options)
    delete options.gte
  }
  else if (has.gte) {
    options.gte = this.encode(options.gte, options)
  }
  else {
    options.gt = this.encode(LOWER_BOUND, options)
  }

  if (has.lt) {
    options.lt = this.encode(options.lt, options)
    delete options.lte
  }
  else if (has.lte)
    options.lte = this.encode(options.lte, options)
  else {
    options.lt = this.encode(UPPER_BOUND, options)
  }

  var $postNext = postNext.bind(this, keyAsBuffer)

  function wrappedFactory(options) {
    var iterator = pre.factory(options)

    iterator.extendWith({
      postNext: $postNext
    })

    return iterator
  }

  return {
    options: options,
    factory: wrappedFactory
  }
}

//
// add ref to bytewise as a convenience
//
bytespace.bytewise = bytewise

module.exports = bytespace
