var Batch = require('./batch')
var levelup = require('levelup')
var merge = require('xtend')
var Namespace = require('./namespace')
var updown = require('level-updown')

module.exports = Bytespace

//
// create a bytespace within a provided levelup instance
//
function Bytespace(db, ns, opts) {
  if (!(ns instanceof Namespace)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (db.namespace instanceof Namespace)
      return db.sublevel(ns, opts)
    
    //
    // otherwise it's a top level subspace
    //
    ns = new Namespace([ ns ])
  }

  function factory() {
    var base = updown(db)

    base.extendWith({
      preBatch: preBatch.bind(ns),
      preGet: preGet.bind(ns),
      postGet: postGet.bind(ns),
      preIterator: preIterator.bind(ns)
    })

    return base
  }

  opts = merge(db.options, opts, { db: factory })
  var space = levelup(opts)
  space.namespace = ns
  ns.codec = db._codec

  //
  // api-compatible with sublevel, extended to allow overloading db options
  //
  space.sublevel = function (ns_, opts_) {
    space.sublevels || (space.sublevels = {})
    if (space.sublevels[ns_])
      return space.sublevels[ns_]
    return space.sublevels[ns_] = Bytespace(db, ns.append(ns_), merge(opts, opts_))
  }

  space.clone = function () {
    return Bytespace(db, ns, opts)
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

  space.pre = function (hook) {
    return addHook(ns.prehooks, hook)
  }

  space.post = function (hook) {
    return addHook(ns.posthooks, hook)
  }

  //
  // override single-record write operations, redirecting to batch
  //
  space.put = function (k, v, opts, cb) {
    if (!cb && typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    space.batch([{ type: 'put', key: k, value: v, options: opts }], opts, cb)
  }

  space.del = function (k, opts, cb) {
    if (!cb && typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    space.batch([{ type: 'del', key: k, options: opts }], opts, cb)
  }

  function addEncodings(op, subspace) {
    if (subspace && subspace.options) {
      op.keyEncoding || (op.keyEncoding = subspace.options.keyEncoding)
      op.valueEncoding || (op.valueEncoding = subspace.options.valueEncoding)
    }
    return op
  }

  //
  // hook batch method to invoke commit hooks on original keys
  //
  var _batch = space.batch
  space.batch = function (ops, opts, cb) {
    if (!arguments.length)
      return new Batch(space)

    if (!cb && typeof opts === 'function') {
      cb = opts
      opts = {}
    }

    //
    // apply precommit hooks
    //
    for (var i = 0, len = ops.length; i < len; i++) {
      var op = ops[i]

      function add(op) {
        if (op === false)
          return delete ops[i]
        ops.push(op)
      }

      addEncodings(op, op.prefix)

      op.prefix || (op.prefix = space)

      var ns = op.prefix.namespace
      if (!(ns instanceof Namespace))
        return cb('Unknown prefix in commit')

      ns.trigger(ns.prehooks, op.prefix, [ op, add, ops ])

      //
      // convert value before we pass to leveldown
      //
      op.__value = op.value
      var asBuffer = ns.codec.valueAsBuffer(opts)
      op.value = ns.codec.encodeValue(op.value, opts, op)
      op.__valueEncoding = op.valueEncoding
      delete op.valueEncoding
      op.valueEncoding = asBuffer ? 'binary' : 'utf8'
    }

    _batch.call(space, ops, opts, function (err) {
      if (err)
        return cb(err)

      ops.forEach(function (op) {
        var ns = op.prefix.namespace
        ns.trigger(ns.posthooks, op.prefix, [ op ])
      })

      cb()
    })
  }

  return space
}

//
// leveldown pre-batch hook
//
function preBatch(array, opts, cb, next) {
  var encoded = []
  var op
  for (var i = 0, length = array.length; i < length; i++) {
    op = encoded[i] = merge(array[i])

    var ns = op.prefix.namespace
    op.key = ns.encode(op.key)
    op.keyAsBuffer = true
  }

  next(encoded, opts, cb)
}

//
// leveldown pre-get hook
//
function preGet(k, opts, cb, next) {
  opts.keyEncoding = 'binary'
  next(this.encode(k), opts, cb)
}

//
// leveldown post-get hook
//
function postGet(k, opts, err, v, cb, next) {
  next(this.decode(k), opts, err, this.codec.decodeValue(v, opts), cb)
}


function preIterator(pre) {
  var ns = this
  var opts = ns.encodeRange(pre.options)

  function wrappedFactory(opts) {
    var iterator = pre.factory(opts)

    iterator.extendWith({
      postNext: postNext.bind(ns, opts, pre.options)
    })

    return iterator
  }

  return {
    options: opts,
    factory: wrappedFactory
  }
}


function postNext(opts, preOpts, err, k, v, cb, next) {
  //
  // pass through errors and null end-of-iterator values
  //
  if (err || k == null)
    return next(err, k, v, cb)

  k = this.decode(k)
  v = this.codec.decodeValue(v, opts)
  next(err, err ? k : preOpts.keyAsBuffer ? k : k.toString('utf8'), v, cb)
}
