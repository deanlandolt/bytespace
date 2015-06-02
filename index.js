var Codec = require('level-codec')
var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var merge = require('xtend')
var through = require('through2')
var util = require('levelup/lib/util')

var Batch = require('./batch')
var Namespace = require('./namespace')

module.exports = Bytespace

function getCallback (opts, cb) {
  return typeof opts == 'function' ? opts : cb
}

function getOptions(opts) {
  return merge(util.getOptions(opts))
}
//
// create a bytespace within a remote levelup instance
//
function Bytespace(db, ns, opts) {
  if (!(this instanceof Bytespace))
    return new Bytespace(db, ns, opts)

  if (!(ns instanceof Namespace)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (db.namespace instanceof Namespace)
      return db.sublevel(ns, opts)

    //
    // otherwise it's a root subspace
    //
    ns = new Namespace([ ns ])
  }

  opts = this.options = merge(Bytespace.options, db.options, opts)
  var codec = this._codec = new Codec(opts)
  this.namespace = ns

  function encode(k, opts) {
    return ns.encode(codec.encodeKey(k, opts))
  }

  function decode(k, opts) {
    var coerce = codec.keyAsBuffer(opts) ? Buffer : String
    return codec.decodeKey(coerce(ns.decode(k, opts)))
  }

  //
  // use provided methods manifest in options or get from db
  //
  this.methods = merge(opts.methods || db.methods)

  //
  // sublevel api-compatibility
  //
  this.sublevel = function (ns_, opts_) {
    var subs = this.sublevels || (this.sublevels = {})
    //
    // memoize the sublevels we create
    //
    if (subs[ns_])
      return subs[ns_]
    return subs[ns_] = new Bytespace(db, ns.append(ns_), merge(opts, opts_))
  }

  this.clone = function () {
    return new Bytespace(db, ns, opts)
  }

  //
  // helper to register pre and post commit hooks
  //
  function addHook(hooks, hook) {
    hooks.push(hook)
    return function () {
      var i = hooks.indexOf(hook)
      if (~i)
        return hooks.splice(i, 1)
    }
  }

  this.pre = function (hook) {
    return addHook(ns.prehooks, hook)
  }

  this.post = function (hook) {
    return addHook(ns.posthooks, hook)
  }

  function kOpts(initial) {
    return merge(initial, { keyEncoding: 'binary' })
  }

  function vOpts(initial) {
    return merge({ valueEncoding: opts.valueEncoding }, initial)
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

  //
  // method proxy implementations
  //
  if (typeof db.get === 'function') {
    this.get = function (k, opts, cb) {
      cb = getCallback(opts, cb)
      opts = getOptions(opts)

      db.get(encode(k, opts), kvOpts(opts), cb)
    }
  }

  if (typeof db.del === 'function') {
    this.del = function (k, opts, cb) {
      //
      // redirect to batch if we have hooks
      //
      if (ns.hasHooks()) {
        this.batch([{ type: 'del', key: k }], opts, cb)
      }
      else {
        cb = getCallback(opts, cb)
        opts = getOptions(opts)

        db.del(encode(k, opts), kOpts(opts), cb)
      }
    }
  }

  if (typeof db.put === 'function') {
    this.put = function (k, v, opts, cb) {
      //
      // redirect to batch if we have hooks
      //
      if (ns.hasHooks()) {
        this.batch([{ type: 'put', key: k, value: v }], opts, cb)
      }
      else {
        cb = getCallback(opts, cb)
        opts = getOptions(opts)

        db.put(encode(k, opts), v, kvOpts(opts), cb)
      }
    }
  }

  if (typeof db.batch === 'function') {
    this.batch = function (ops, opts, cb) {
      if (!arguments.length)
        return new Batch(this)

      cb = getCallback(opts, cb)
      opts = getOptions(opts)

      //
      // encode batch ops and apply precommit hooks
      //
      for (var i = 0, len = ops.length; i < len; i++) {
        var op = ops[i]

        function add(op) {
          if (op === false)
            return delete ops[i]
          ops.push(op)
        }

        addEncodings(op, op.prefix)

        op.prefix || (op.prefix = this)

        var ns = op.prefix.namespace
        if (!(ns instanceof Namespace))
          return cb('Unknown prefix in batch commit')

        if (ns.prehooks.length)
          ns.trigger(ns.prehooks, op.prefix, [ op, add, ops ])

        //
        // encode op key, but keep a ref to initial value around for postcommit
        //
        op.initialKey = op.key
        op.initialKeyEncoding = op.keyEncoding
      }

      ops.forEach(function (op) {
        var pre = op.prefix
        op.key = pre.namespace.encode(pre._codec.encodeKey(op.key, opts, op))
        op.keyEncoding = 'binary'
      })

      if (!ops.length)
        return cb()

      db.batch(ops, kvOpts(opts), function (err) {
        if (err)
          return cb(err)

        //
        // apply postcommit hooks for ops, setting encoded keys to initial state
        //
        ops.forEach(function (op) {
          op.key = op.initialKey
          op.keyEncoding = op.initialKeyEncoding
          var ns = op.prefix.namespace
          if (ns.posthooks.length)
            ns.trigger(ns.posthooks, op.prefix, [ op ])
        })

        cb()
      })
    }
  }

  //
  // transform stream to decode data keys
  //
  function streamDecoder(opts) {
    return through.obj(function (data, enc, cb) {
      try {
        if (opts.keys && opts.values) {
          data.key = decode(data.key, opts)
        }
        else if (opts.keys) {
          data = decode(data, opts)
        }
      }
      catch (err) {
        return cb(err)
      }

      cb(null, data)
    })
  }

  function readStream(opts) {
    return db.createReadStream(ns.encodeRange(opts)).pipe(streamDecoder(opts))
  }

  //
  // add read stream proxy methods if createReadStream is avaialble
  //
  if (typeof db.createReadStream === 'function') {
    this.createReadStream = this.readStream = function (opts) {
      return readStream(merge({ keys: true, values: true }, vOpts(opts)))
    }

    this.createKeyStream = this.keyStream =  function (opts) {
      return readStream(merge(vOpts(opts), { keys: true, values: false }))
    }

    this.createValueStream = this.valueStream = function (opts) {
      return readStream(merge(vOpts(opts), { keys: false, values: true }))
    }
  }

  //
  // add createLiveStream proxy if available
  //
  if (typeof db.createLiveStream === 'function') {
    this.createLiveStream = this.liveStream =  function (opts) {
      var o = merge(vOpts(opts), ns.encodeRange(opts))
      return db.createLiveStream(o).pipe(streamDecoder(opts))
    }
  }
}

inherits(Bytespace, EventEmitter)

//
// used to define default options for root subspaces
//
Bytespace.options = {
  keyEncoding: util.defaultOptions.keyEncoding,
  valueEncoding: util.defaultOptions.valueEncoding
}
