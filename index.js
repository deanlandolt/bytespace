var bytewise = require('bytewise-core')
var compare = require('bytewise-core/util').compare
var levelup = require('levelup')
var updown = require('level-updown')
var xtend = require('xtend')

var arrayBoundary = bytewise.sorts.array.bound

//
// brand namespace to keep track of subspace root
//
function Namespace(path) {
  this.path = path
  this.prefix = bytewise.encode(path)
}

Namespace.prototype.append = function (sub) {
  return new Namespace(this.path.concat(sub))
}

Namespace.prototype.contains = function (key) {
  return compare(this.prefix, key.slice(0, this.prefix.length)) === 0
}

Namespace.prototype.decode = function (key, options) {
  if (!this.contains(key))
    return key

  //
  // slice off namespace prefix and return
  //
  return key.slice(this.prefix.length)
}

Namespace.prototype.encode = function (key, options) {
  return Buffer.concat([ this.prefix, key ])
}

function space(db, ns, options) {
  if (!(ns instanceof Namespace)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (typeof db.subspace === 'function') {
      return db.subspace(ns, options)
    }
    
    //
    // otherwise this is a top level subspace
    //
    ns = new Namespace([ ns ])
  }

  options || (options = {})

  //
  // encode as binary but preserve original (defaults to "utf8" per levelup API)
  //
  ns.options = xtend(options)
  options.keyEncoding = 'binary'

  function factory() {
    var base = updown(db)

    base.extendWith({
      prePut: prePut.bind(ns),
      preDel: preDel.bind(ns),
      preBatch: preBatch.bind(ns),
      preGet: preGet.bind(ns),
      postGet: postGet.bind(ns),
      preIterator: preIterator.bind(ns)
    })

    return base
  }

  options.db = factory

  var sub = levelup(options)

  sub.subspace = function (subNs, options) {
    return space(db, ns.append(subNs), options)
  }

  return sub
}


function prePut(key, value, options, cb, next) {
  next(this.encode(key, options), value, options, cb)
}


function preDel(key, options, cb, next) {
  next(this.encode(key, options), options, cb)
}

function preBatch(array, options, cb, next) {
  var narray = array

  if (Array.isArray(array)) {
    narray = []
    for (var i = 0, length = array.length; i < length; i++) {
      narray[i] = xtend(array[i])
      narray[i].key = this.encode(narray[i].key, options)
    }
  }

  next(narray, options, cb)
}


function preGet(key, options, cb, next) {
  next(this.encode(key, options), options, cb)
}


function postGet(key, options, err, value, cb, next) {
  next(this.decode(key, options), options, err, value, cb)
}


function postNext(options, err, key, value, cb, next) {
  next(err, (err || key == null) ? key : this.decode(key, options), value, cb)
}


var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])
var rangeKeys = [ 'start', 'end', 'gt', 'lt', 'gte', 'lte' ]

function preIterator(pre) {
  var options = xtend(pre.options)

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

  var $postNext = postNext.bind(this, options)

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
space.bytewise = bytewise

module.exports = space
