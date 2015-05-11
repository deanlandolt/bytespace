var bytewise = require('bytewise-core')
var levelup = require('levelup')
var updown = require('level-updown')
var xtend = require('xtend')

//
// brand namespace to keep track of root
//
function Namespace(data) {
  // TODO: encode as length-prefixed array, cheating with nested arrays for now
  this.data = data
  this.encoded = bytewise.encode(data)
  this.prefix = this.encoded.slice(1, this.encoded.length - 1).toString('hex')
}

Namespace.prototype.append = function (sub) {
  return new Namespace(this.data.concat(sub))
}

Namespace.prototype.encode = function (key, options) {
  // TODO: respect `options.keyEncoding`, optimize bw keys avoiding extra encode
  return bytewise.encode([ this.data, key ])
}

Namespace.prototype.decode = function (key, options) {
  // TODO: slice off namespace portion before decoding
  return bytewise.decode(key)[1]
}

function space(db, namespace, options) {
  if (!(namespace instanceof Namespace)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (typeof db.mountSubspace === 'function') {
      return db.mountSubspace(namespace, options)
    }
    
    //
    // otherwise this is a root subspace
    //
    namespace = new Namespace([ namespace ])
  }

  options || (options = {})

  //
  // encode as binary but preserve original (defaulting to "utf8" like levelup)
  //
  var keyEncoding = options.keyEncoding || 'utf8'
  options.keyEncoding = 'binary'

  function encode(key, options) {
    return namespace.encode(key, options)
  }

  function decode(key, options) {
    // TODO: figure out why updown even sends bounds keys
    if (key.toString('hex').slice(4).indexOf(namespace.prefix) !== 0)
      return key

    return namespace.decode(key, options)
  }

  function factory() {
    var ud = updown(db)

    ud.extendWith({
      prePut: mkPrePut(encode),
      preGet: mkPreGet(encode),
      postGet: mkPostGet(decode),
      preDel: mkPreDel(encode),
      preBatch: mkPreBatch(encode),
      preIterator: mkPreIterator(encode, decode)
    })

    return ud
  }

  options.db = factory

  var subspace = levelup(options)

  subspace.mountSubspace = function (sub, options) {
    return space(db, namespace.append(sub), options)
  }

  return subspace
}


function mkPrePut(encode) {
  return function prePut(key, value, options, callback, next) {
    next(encode(key, options), value, options, callback)
  }
}


function mkPreGet(encode) {
  return function preGet(key, options, callback, next) {
    next(encode(key, options), options, callback)
  }
}


function mkPostGet(decode) {
  return function postGet(key, options, err, value, callback, next) {
    next(decode(key, options), options, err, value, callback)
  }
}


function mkPreDel(encode) {
  return function preDel(key, options, callback, next) {
    next(encode(key, options), options, callback)
  }
}


function mkPreBatch(encode) {
  return function preBatch(array, options, callback, next) {
    var narray = array

    if (Array.isArray(array)) {
      narray = []
      for (var i = 0; i < array.length; i++) {
        narray[i] = xtend(array[i])
        narray[i].key = encode(narray[i].key, options)
      }
    }

    next(narray, options, callback)
  }
}

function mkPreIterator(encode, decode) {
  return function preIterator(pre) {
    var options = xtend(pre.options)

    if ('start' in options || 'end' in options) {
      if ('lte' in options || 'lt' in options || 'gte' in options || 'gt' in options) {
        // TODO: updown sending phantom start value -- bug?
      }
      else {
        if (!options.reverse) {
          options.gte = 'start' in options ? options.start : null
          options.lte = 'end' in options ? options.end : undefined
        }
        else {
          options.gte = 'end' in options ? options.end : null
          options.lte = 'start' in options ? options.start : undefined
        }
        delete options.start
        delete options.end
      }
    }

    if ('gt' in options) {
      options.gt = encode(options.gt, options)
      delete options.gte
    }
    else if ('gte' in options) {
      options.gte = encode(options.gte, options)
    }
    else {
      // TODO .gt = encode(base.bound.lower, options)
      options.gte = encode(null, options)
    }

    if ('lt' in options) {
      options.lt = encode(options.lt, options)
      delete options.lte
    }
    else if ('lte' in options)
      options.lte = encode(options.lte, options)
    else {
      // TODO: .lt = encode(base.bound.upper, options)
      options.lte = encode(undefined, options)
    }

    function wrappedFactory(options) {
      var iterator = pre.factory(options)

      iterator.extendWith({
          postNext: mkPreNext(decode)
      })

      return iterator
    }

    return {
      options: options,
      factory: wrappedFactory
    }
  }
}


function mkPreNext(decode) {
  return function preNext(err, key, value, callback, next) {
    next(err, (err || key == null) ? key : decode(key), value, callback)
  }
}

//
// add ref to bytewise as a convenience
//
space.bytewise = bytewise

module.exports = space
