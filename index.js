var assert = require('assert')
var bytewise = require('bytewise-core')
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

  function factory() {
    var base = updown(db)

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

  return space
}


function prePut(key, value, options, cb, next) {
  options.keyEncoding = 'binary'
  next(this.encode(key), value, options, cb)
}


function preDel(key, options, cb, next) {
  options.keyEncoding = 'binary'
  next(this.encode(key), options, cb)
}


function preBatch(array, options, cb, next) {
  options.keyEncoding = 'binary'
  var narray = array

  if (Array.isArray(array)) {
    narray = []
    for (var i = 0, length = array.length; i < length; i++) {
      narray[i] = xtend(array[i])
      narray[i].key = this.encode(narray[i].key)
    }
  }

  next(narray, options, cb)
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
