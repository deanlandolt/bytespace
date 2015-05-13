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
  this.prefix = bytewise.encode(arrayBoundary.lower([ path ]))
}

Namespace.prototype.append = function (sub) {
  return new Namespace(this.path.concat(sub))
}

Namespace.prototype.encode = function (key, options) {
  // TODO: respect `options.keyEncoding`, optimize on bw keys to avoid double-encode
  return bytewise.encode([ this.path, key ])
}

Namespace.prototype.decode = function (key, options) {
  //
  // slice off namespace portion before decoding
  //
  var buffer = key.slice(this.prefix.length, -2)
  return this.contains(key) ? bytewise.decode(buffer, { nested: true }) : key
}

Namespace.prototype.contains = function (key) {
  return compare(this.prefix, key.slice(0, this.prefix.length)) === 0
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
  ns.keyEncoding = options.keyEncoding || 'utf8'
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


function prePut(key, value, options, callback, next) {
  next(this.encode(key, options), value, options, callback)
}


function preDel(key, options, callback, next) {
  next(this.encode(key, options), options, callback)
}

function preBatch(array, options, callback, next) {
  var narray = array

  if (Array.isArray(array)) {
    narray = []
    for (var i = 0, length = array.length; i < length; i++) {
      narray[i] = xtend(array[i])
      narray[i].key = this.encode(narray[i].key, options)
    }
  }

  next(narray, options, callback)
}


function preGet(key, options, callback, next) {
  next(this.encode(key, options), options, callback)
}


function postGet(key, options, err, value, callback, next) {
  next(this.decode(key, options), options, err, value, callback)
}


var LOWER_BOUND = bytewise.encode(bytewise.bound.lower())
var UPPER_BOUND = bytewise.encode(bytewise.bound.upper())
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

  var $postNext = postNext.bind(this)

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


function postNext(err, key, value, callback, next) {
  next(err, (err || key == null) ? key : this.decode(key), value, callback)
}


//
// add ref to bytewise as a convenience
//
space.bytewise = bytewise

module.exports = space
