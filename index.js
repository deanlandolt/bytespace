var Batch = require('./batch')
var bytewise = require('bytewise-core')
var levelup = require('levelup')
// var ltgt = require('ltgt')
var Prefix = require('./prefix')
var updown = require('level-updown')
var xtend = require('xtend')

var prefixes = new WeakMap()

//
// create a bytespace given a provided levelup instance
//
function bytespace(db, ns, opts) {
  opts || (opts = {})

  var prefix = ns
  if (!(prefix instanceof Prefix)) {
    //
    // if db is a subspace mount as a nested subspace
    //
    if (prefixes.get(db))
      return db.sublevel(ns, opts)
    
    //
    // otherwise it's a top level subspace, inherit keyEncoding from config
    //
    opts.keyEncoding || (opts.keyEncoding = bytespace.keyEncoding)
    prefix = new Prefix([ ns ])
  }

  function factory() {
    var base = updown(db)

    base.extendWith({
      preBatch: preBatch.bind(prefix),
      preGet: preGet.bind(prefix),
      postGet: postGet.bind(prefix),
      preIterator: preIterator.bind(prefix)
    })

    return base
  }

  opts.db = factory

  var space = levelup(opts)

  //
  // associate prefix with space weakly, and add ref to space to prefx
  //
  prefixes.set(space, prefix)
  prefix.db = space

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
    return addHook(prefix.prehooks, hook)
  }

  space.post = function (hook) {
    return addHook(prefix.posthooks, hook)
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

  function addEncodings(op, prefix) {
    if (prefix && prefix.options) {
      op.keyEncoding || (op.keyEncoding = prefix.options.keyEncoding)
      op.valueEncoding || (op.valueEncoding = prefix.options.valueEncoding)
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

      //
      // resolve prefix if an alternative space is referenced
      //
      op.prefix || (op.prefix = this)
      var prefix = prefixes.get(op.prefix)
      prefix.trigger(prefix.prehooks, [ op, add, ops ])
    }

    _batch.call(space, ops, opts, function (err) {
      if (err)
        return cb(err)

      ops.forEach(function (op) {
        prefix.trigger(prefix.posthooks, [ op ])
      })
      cb()
    })
  }

  //
  // api-compatible with sublevel
  //
  space.sublevel = function (ns, opts_) {
    //
    // subspace inherits options from parent space
    //
    return bytespace(db, prefix.append(ns), xtend(opts, opts_))
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
    op = encoded[i] = xtend(array[i])

    //
    // resolve prefix from db reference
    //
    var prefix = prefixes.get(op.prefix)
    if (!prefix)
      return cb(new Error('Unknown prefix in batch'))

    op.key = prefix.encode(op.key || this)
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
// leveldown pre-get hook
//
function postGet(k, opts, err, v, cb, next) {
  next(this.decode(k), opts, err, v, cb)
}


var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])

var rangeKeys = [ 'start', 'end', 'gt', 'lt', 'gte', 'lte' ]

function preIterator(pre) {
  var opts = xtend(pre.options)
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
// default key encoding to ut8 per levelup API
//
bytespace.keyEncoding = 'utf8'

//
// add ref to bytewise encoding as a convenience
//
bytespace.bytewise = bytewise

module.exports = bytespace
