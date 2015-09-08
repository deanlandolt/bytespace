var bytewise = require('bytewise-core')
var Codec = require('level-codec')
var equal = require('bytewise-core/util').equal
var merge = require('xtend')


var LOWER_BOUND = new Buffer([])
var UPPER_BOUND = new Buffer([ 0xff ])
var RANGE_KEYS = [ 'gt', 'lt', 'gte', 'lte', 'min', 'max', 'start', 'end' ]

//
// brand namespace instance to keep track of subspace root
//
function Namespace(path, hex) {
  this.hex = !!hex
  this.keyEncoding = hex ? 'utf8' : 'binary'

  this.path = path
  this.buffer = bytewise.encode(path)
  this.prehooks = []
  this.posthooks = []
}

Namespace.prototype.append = function (ns) {
  return new Namespace(this.path.concat(ns), this.hex)
}

Namespace.prototype.createCodec = function (opts) {
  return this.codec = new Codec(opts)
}

Namespace.prototype.contains = function (k) {
  // slice full key to get prefix to compare against buffer
  return equal(this.buffer, k.slice(0, this.buffer.length))
}

Namespace.prototype.decode = function (k, opts) {
  if (this.hex) {
    if (typeof k !== 'string') {
      throw new TypeError('Key must be encoded as a hex string')
    }

    k = new Buffer(k, 'hex')
  }

  else if (!Buffer.isBuffer(k))
    throw new TypeError('Key must be encoded as a buffer')

  // TODO: throw?
  if (!this.contains(k))
    return k

  // slice off prefix and run through codec
  var encoded = k.slice(this.buffer.length)
  var coerce = this.codec.keyAsBuffer(opts) ? Buffer : String
  return this.codec.decodeKey(coerce(encoded))
}

Namespace.prototype.encode = function (k, opts, batchOpts) {
  var buffer = this.buffer

  // TODO: this could be a lot more efficient
  if (k === LOWER_BOUND) {
    // noop
  }
  else if (k === UPPER_BOUND) {
    buffer = Buffer.concat([ buffer, k ])
  }
  else {
    var encoded = this.codec.encodeKey(k, opts, batchOpts)
    buffer = Buffer.concat([ buffer, new Buffer(encoded) ])
  }

  return this.hex ? buffer.toString('hex') : buffer
}

Namespace.prototype.encodeRange = function (range) {
  var opts = merge(range, {
    keyAsBuffer: !this.hex,
    keyEncoding: this.keyEncoding
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
