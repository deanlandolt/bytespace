'use strict'

var Codec = require('level-codec')
var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var NotFoundError = require('level-errors').NotFoundError
var Transform = require('stream').Transform
var xtend = require('xtend')

var Batch = require('./batch')
var Namespace = require('./namespace')
var NOT_FOUND = /notfound/i

module.exports = Bytespace

function getCallback (opts, cb) {
  return typeof opts == 'function' ? opts : cb
}

// from https://github.com/Level/levelup/blob/master/lib/util.js
function getOptions (options) {
  if (typeof options == 'string')
    return { valueEncoding: options }
  if (typeof options != 'object')
    return {}
  return xtend(options)
}

// create a bytespace within a remote levelup instance
// TODO: remove ns from signature to align w/ sublevel
function Bytespace(db, ns, opts) {
  if (!(this instanceof Bytespace))
    return new Bytespace(db, ns, opts)

  if (!(ns instanceof Namespace)) {

    // if db is a subspace mount as a nested subspace
    if (db.namespace instanceof Namespace)
      return db.sublevel(ns, opts)

    // otherwise it's a root subspace
    ns = new Namespace([ ns ], opts && opts.hexNamespace)
  }

  var space = this

  space.namespace = ns
  opts = space.options = xtend(Bytespace.options, db.options, opts)
  ns.codec = new Codec(opts)

  // use provided methods manifest in options or get from db
  // TODO: can we remove this?
  space.methods = xtend(opts.methods || db.methods)

  // sublevel@6-compatible-ish
  Object.defineProperty(db, 'version', {
    value: 6,
    configurable: true
  })

  // forward open and close events from base db w/o affecting listener count
  function forwardOpen() {
    db.once('open', function () {
      space.emit('open', space)
      forwardOpen()
    })
  }

  // TODO: level emits closed, multilevel emits close...damn...
  function forwardClose() {
    var closeEvent = db.createRpcStream ? 'close' : 'closed'
    db.once(closeEvent, function () {
      // only emit 'close' for sanity's sake
      space.emit('close')
      forwardClose()
    })
  }

  // proxy `isOpen` to underlying db
  space.isOpen = function () {
    return db.isOpen()
  }

  // set multilevel `isClient` boolean
  space.isClient = !!db.isClient

  // for sublevel api-compatibility
  space.sublevel = function (ns_, opts_) {
    var index = space.sublevels || (space.sublevels = {})

    // memoize the sublevels we create
    // TODO: memoize with bytewise-encoded hex string instead
    if (index[ns_]) return index[ns_]

    return index[ns_] = new Bytespace(db, ns.append(ns_), xtend(opts, opts_))
  }

  space.clone = function () {
    return new Bytespace(db, ns, opts)
  }

  function kOpts(initial) {
    return xtend(initial, { keyEncoding: ns.keyEncoding, keyAsBuffer: !ns.hex })
  }

  function vOpts(initial) {
    return xtend({ valueEncoding: opts.valueEncoding }, initial)
  }

  function kvOpts(initial) {
    return vOpts(kOpts(initial))
  }

  function addEncodings(op, db) {
    if (db && db.options) {
      op.keyEncoding || (op.keyEncoding = db.options.keyEncoding)
      op.valueEncoding || (op.valueEncoding = db.options.valueEncoding)
    }
    return op
  }

  // method proxy implementations
  if (typeof db.get === 'function') {
    space.get = function (k, opts, cb) {
      cb = getCallback(opts, cb)
      opts = getOptions(opts)

      try {
        db.get(ns.encode(k, opts), kvOpts(opts), handler)
      }
      catch (err) {
        process.nextTick(cb.bind(null, err))
      }

      function handler(err, v) {
        // sanitize full keypath for notFound errors
        if (err && err.notFound || NOT_FOUND.test(err)) {
          try {
            err = new NotFoundError('Key not found in database [' + k + ']')
          }
          catch (_) {}
        }

        cb(err, v)
      }
    }
  }

  // helper to register pre and post commit hooks
  function addHook(hooks, hook) {
    hooks.push(hook)
    return function () {
      var i = hooks.indexOf(hook)
      if (i >= 0) return hooks.splice(i, 1)
    }
  }

  if (typeof db.batch === 'function') {

    space.del = function (k, opts, cb) {
      // redirect to batch
      space.batch([{ type: 'del', key: k }], opts, cb)
    }

    space.put = function (k, v, opts, cb) {
      // redirect to batch
      space.batch([{ type: 'put', key: k, value: v }], opts, cb)
    }

    space.batch = function (ops, opts, cb) {
      if (!arguments.length) return new Batch(space)

      cb = getCallback(opts, cb)
      opts = getOptions(opts)

      function add(op) {    
        if (op === false) {    
          return delete ops[i]   
        }    
        ops.push(op)   
      }

      try {
        // encode batch ops and apply precommit hooks
        for (var i = 0, len = ops.length; i < len; i++) {
          var op = ops[i]

          addEncodings(op, op.prefix)

          op.prefix || (op.prefix = space)

          var ns = op.prefix.namespace
          if (!(ns instanceof Namespace))
            return cb('Unknown prefix in batch commit')

          if (ns.prehooks.length) {
            ns.trigger(ns.prehooks, op.prefix, [ op, add, ops ])
          }
        }

        if (!ops.length) return cb()

        var encodedOps = ops.map(function (op) {
          return {
            type: op.type,
            key: op.prefix.namespace.encode(op.key, opts, op),
            keyEncoding: ns.keyEncoding,
            value: op.value,
            // TODO: multilevel json serialization issue?
            valueEncoding: op.valueEncoding,
            sync: op.sync
          }
        })

        db.batch(encodedOps, kvOpts(opts), function (err) {
          if (err) return cb(err)

          // apply postcommit hooks for ops, setting encoded keys to initial state
          try {
            if (ns.posthooks.length) {
              ops.forEach(function (op) {
                ns.trigger(ns.posthooks, op.prefix, [ op ])
              })
            }
          }
          catch (err) {
            cb(err)
          }

          cb()
        })
      }
      catch (err) {
        process.nextTick(cb.bind(null, err))
      }
    }

    space.pre = function (hook) {
      return addHook(ns.prehooks, hook)
    }

    space.post = function (hook) {
      return addHook(ns.posthooks, hook)
    }
  }

  // if no batch available on db, replace write methods individually
  else {
    if (typeof db.del === 'function') {
      space.del = function (k, opts, cb) {
        cb = getCallback(opts, cb)
        opts = getOptions(opts)

        try {
          db.del(ns.encode(k, opts), kOpts(opts), cb)
        }
        catch (err) {
          process.nextTick(cb.bind(null, err))
        }
      }
    }

    if (typeof db.put === 'function') {
      space.put = function (k, v, opts, cb) {
        cb = getCallback(opts, cb)
        opts = getOptions(opts)

        try {
          db.put(ns.encode(k, opts), v, kvOpts(opts), cb)
        }
        catch (err) {
          process.nextTick(cb.bind(null, err))
        }
      }
    }
  }

  // transform stream to decode data keys
  function decodeStream(opts) {
    opts || (opts = {})
    var stream = Transform({ objectMode: true })

    stream._transform = function (data, _, cb) {
      try {
        if (opts.keys && opts.values) {
          data.key = ns.decode(data.key, opts)
        }
        else if (opts.keys) {
          data = ns.decode(data, opts)
        }
      }
      catch (err) {
        return cb(err)
      }
      cb(null, data)
    }

    return stream
  }

  // add read stream proxy methods if createReadStream is available
  // TODO: clean all this duplication up
  function readStream(opts) {
    return db.createReadStream(ns.encodeRange(opts)).pipe(decodeStream(opts))
  }

  function liveStream(opts) {
    return db.createLiveStream(ns.encodeRange(opts)).pipe(decodeStream(opts))
  }

  if (typeof db.createReadStream === 'function') {
    space.createReadStream = space.readStream = function (opts) {
      return readStream(xtend({ keys: true, values: true }, vOpts(opts)))
    }
    if (db.readStream) space.readStream = space.createReadStream
  }

  if (typeof db.createKeyStream === 'function') {
    space.createKeyStream = function (opts) {
      return readStream(xtend(vOpts(opts), { keys: true, values: false }))
    }
    if (db.keyStream) space.keyStream = space.createKeyStream
  }

  if (typeof db.createValueStream === 'function') {
    space.createValueStream = function (opts) {
      return readStream(xtend(vOpts(opts), { keys: false, values: true }))
    }
    if (db.valueStream) space.valueStream = space.createValueStream
  }

  // add createLiveStream proxy if available
  if (typeof db.createLiveStream === 'function') {
    space.createLiveStream = function (opts) {
      var o = xtend(vOpts(opts), ns.encodeRange(opts))
      return db.createLiveStream(o).pipe(decodeStream(opts))
    }
    if (db.liveStream) space.liveStream = space.createLiveStream
  }
}

inherits(Bytespace, EventEmitter)

// default options for root subspace db (from levelup/lib/util.js)
Bytespace.options = {
  keyEncoding: 'utf8',
  valueEncoding: 'utf8'
}
