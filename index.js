var Batch = require('./batch')
var bytewise = require('bytewise-core')
var levelup = require('levelup')
// var ltgt = require('ltgt')
var merge = require('xtend')
var Namespace = require('./namespace')
var updown = require('level-updown')

//
// create a bytespace given a provided levelup instance
//
function bytespace(db, ns, opts) {

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
  // api-compatible with sublevel, extended to allow overloading db options
  //
  space.sublevel = function (ns_, opts_) {
    return bytespace(db, ns.append(ns_), merge(opts, opts_))
  }

  space.clone = function () {
    return bytespace(db, ns, opts)
  }

  //
  // override single-record write operations, redirecting to batch
  //
  space.put = function (k, v, opts, cb) {
    space.batch([{ type: 'put', key: k, value: v, options: opts }], opts, cb)
  }

  space.del = function (k, opts, cb) {
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
    op.keyEncoding = 'binary'
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
  next(this.decode(k), opts, err, v, cb)
}


var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])

var rangeKeys = [ 'start', 'end', 'gt', 'lt', 'gte', 'lte' ]

function preIterator(pre) {
  var opts = merge(pre.options)
  var keyAsBuffer = opts.keyAsBuffer
  opts.keyAsBuffer = true


  // TODO: use ltgt rather than this hand-rolled crap
  var has = {}
  rangeKeys.forEach(function (k) {
    has[k] = k in opts
  })

  // getting a phantom start value -- but from where?
  if (has.start && (has.gt || has.lt || has.gte || has.lte)) {
    delete opts.start
    has.start = false
  }

  // var lower = this.encode(LOWER_BOUND)
  // var upper = this.encode(UPPER_BOUND)
  // ltgt.toLtgt(opts, opts, this.encode.bind(this), lower, upper)

  if (has.start || has.end) {
    if (!opts.reverse) {
      opts.gte = has.start ? opts.start : LOWER_BOUND
      opts.lte = has.end ? opts.end : UPPER_BOUND
    }
    else {
      opts.gte = has.end ? opts.end : LOWER_BOUND
      opts.lte = has.start ? opts.start : UPPER_BOUND
    }

    has.gte = has.lte = true
    delete opts.start
    delete opts.end
  }

  if (has.gt) {
    opts.gt = this.encode(opts.gt)
    delete opts.gte
  }
  else if (has.gte) {
    opts.gte = this.encode(opts.gte)
  }
  else {
    opts.gt = this.encode(LOWER_BOUND)
  }

  if (has.lt) {
    opts.lt = this.encode(opts.lt)
    delete opts.lte
  }
  else if (has.lte)
    opts.lte = this.encode(opts.lte)
  else {
    opts.lt = this.encode(UPPER_BOUND)
  }

  var $postNext = postNext.bind(this, keyAsBuffer)

  function wrappedFactory(opts) {
    var iterator = pre.factory(opts)

    iterator.extendWith({
      postNext: $postNext
    })

    return iterator
  }

  return {
    options: opts,
    factory: wrappedFactory
  }
}


function postNext(keyAsBuffer, err, k, v, cb, next) {
  //
  // pass through errors and null end-of-iterator values
  //
  if (err || k == null)
    return next(err, k, v, cb)

  k = this.decode(k)
  next(err, err ? k : keyAsBuffer ? k : k.toString('utf8'), v, cb)
}

//
// add ref to bytewise encoding as a convenience
//
bytespace.bytewise = bytewise

module.exports = bytespace
