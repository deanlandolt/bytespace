var bytewise = require('bytewise-core')
var equal = require('bytewise-core/util').equal
var merge = require('xtend')

//
// brand namespace instance to keep track of subspace root
//
function Namespace(path) {
  this.path = path
  this.buffer = bytewise.encode(path)
  this.prehooks = []
  this.posthooks = []
}

Namespace.prototype.append = function (ns) {
  return new Namespace(this.path.concat(ns))
}

Namespace.prototype.contains = function (k) {
  //
  // slice to get key prefix
  //
  return equal(this.buffer, k.slice(0, this.buffer.length))
}

Namespace.prototype.decode = function (k) {
  if (!Buffer.isBuffer(k))
    throw new TypeError('Key must be encoded as a buffer')

  if (!this.contains(k))
    return k

  //
  // slice off prefix and return the rest
  //
  return k.slice(this.buffer.length)
}

Namespace.prototype.encode = function (k) {
  return Buffer.concat([ this.buffer, Buffer(k) ])
}

var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])
var RANGE_KEYS = [ 'gt', 'lt', 'gte', 'lte', 'min', 'max', 'start', 'end' ]

Namespace.prototype.encodeRange = function (range) {
  var opts = merge(range, {
    keyAsBuffer: true,
    keyEncoding: 'binary'
  })

  // TODO: use ltgt rather than this hand-rolled crap?
  var has = {}
  RANGE_KEYS.forEach(function (k) {
    has[k] = k in opts
  })

  if (has.gt || has.lt || has.gte || has.lte) {
    // TODO: getting a phantom start value -- but from where?
    delete opts.start
  }

  else if (has.min || has.max) {
    if (opts.min) {
      opts.gte = opts.min
      delete opts.min
    }
    if (opts.max) {
      opts.lte = opts.max
      delete opts.max
    }
    has.gte = has.lte = true
  }

  else if (has.start || has.end) {
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

  return opts
}

Namespace.prototype.hasHooks = function (ns) {
  return !!(this.prehooks.length || this.posthooks.length)
}

//
// loop over hooks and trigger in the context of subspace
//
Namespace.prototype.trigger = function(hooks, space, args) {
  for (var i = 0, len = hooks.length; i < len; i++) {
    hooks[i].apply(space, args)
  }
}

module.exports = Namespace
