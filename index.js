var Batch = require('./batch')
var bytewise = require('bytewise-core')
var levelup = require('levelup')
// var ltgt = require('ltgt')
var Prefix = require('./prefix')
var updown = require('level-updown')
var xtend = require('xtend')

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
    if (typeof db.subspace === 'function')
      return db.subspace(ns, opts)
    
    //
    // otherwise this is a top level subspace, inherit keyEncoding from config
    //
    opts.keyEncoding || (opts.keyEncoding = bytespace.keyEncoding)
    prefix = new Prefix([ ns ])
  }

  prefix.precommit = opts.precommit
  prefix.postcommit = opts.postcommit

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
  // hook write methods and redirect to batch
  //
  space.put = function (k, v, opts, cb) {
    space.batch([{ type: 'put', key: k, value: v, options: opts }], opts, cb)
  }

  space.del = function (k, opts, cb) {
    space.batch([{ type: 'del', key: k, options: opts }], opts, cb)
  }

  //
  // hook batch method to invoke commit hooks with original keys
  //
  var _batch = space.batch
  space.batch = function (array, opts, cb) {
    if (!arguments.length)
      return new Batch(space)

    if (prefix.precommit)
      array = prefix.precommit(array)

    _batch.call(space, array, opts, function (err) {
      if (prefix.postcommit)
        err = prefix.postcommit(err, array)

      cb(err)
    })
  }

  //
  // allow subspace to be created without leaking ref to root db
  //
  space.subspace = function (ns, opts_) {
    //
    // subspace inherits options from parent
    //
    return bytespace(db, prefix.append(ns), xtend(opts, opts_))
  }

  return space
}


function preBatch(array, opts, cb, next) {
  var encoded = []
  var op
  for (var i = 0, length = array.length; i < length; i++) {
    op = encoded[i] = xtend(array[i])
    op.key = this.encode(op.key)
    op.keyEncoding = 'binary'
  }

  next(encoded, opts, cb)
}


function preGet(k, opts, cb, next) {
  opts.keyEncoding = 'binary'
  next(this.encode(k), opts, cb)
}


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
