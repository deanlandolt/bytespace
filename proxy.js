var Codec = require('level-codec')
var merge = require('xtend')
var through = require('through2')
var util = require('levelup/lib/util')

var Batch = require('./batch')
var Namespace = require('./namespace')

module.exports = Bytespace

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

  opts = this.options = merge(Bytespace.options, opts)
  var codec = this._codec = new Codec(opts)
  this.namespace = ns

  function encode(k, opts, batchOpts) {
    return ns.encode(codec.encodeKey(k, opts, batchOpts))
  }

  function decode(k, opts) {
    var coerce = codec.keyAsBuffer(opts) ? Buffer : String
    return codec.decodeKey(coerce(ns.decode(k, opts)))
  }

  //
  // use provided methods manifest in options or get from db
  //
  this.methods = opts.methods || merge(db.methods)

  //
  // enumerate provided methods
  //
  for (var name in this.methods) {
    if (typeof db[name] === 'function') {
      this[name] = db[name].bind(db)
    }
  }

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
  // method proxy implementations
  //

  if (typeof db.get === 'function') {
    this.get = function (k, opts, cb) {
      if (!cb && typeof opts === 'function') {
        cb = opts
        opts = {}
      }
      opts = util.getOptions(opts)
      k = encode(k, opts)
      opts.keyEncoding = 'binary'
      db.get(k, opts, cb)
    }
  }

  if (typeof db.del === 'function') {
    this.del = function (k, opts, cb) {
      if (!cb && typeof opts === 'function') {
        cb = opts
        opts = {}
      }
      opts = util.getOptions(opts)
      k = encode(k, opts)
      opts.keyEncoding = 'binary'
      db.del(k, opts, cb)
    }
  }

  if (typeof db.put === 'function') {
    this.put = function (k, v, opts, cb) {
      if (!cb && typeof opts === 'function') {
        cb = opts
        opts = {}
      }
      opts = util.getOptions(opts)
      k = encode(k, opts)
      opts.keyEncoding = 'binary'
      db.put(k, v, opts, cb)
    }
  }

  if (typeof db.batch === 'function') {
    this.batch = function (array, opts, cb) {
      if (!arguments.length) {
        //
        // wrap del and put methods for chained batches
        //
        var batch = db.batch()

        var _del = batch.del
        batch.del = function (k, opts) {
          return _del.call(batch, encode(k, opts), { keyEncoding: 'binary' })
        }

        var _put = batch.put
        batch.put = function (k, v, opts) {
          return _put.call(batch, encode(k, opts), v, { keyEncoding: 'binary' })
        }

        return batch
      }

      if (!cb && typeof opts === 'function') {
        cb = opts
        opts = {}
      }

      opts = util.getOptions(opts)
      array.map(function (item) {
        item.key = encode(item.key, opts, item.options)
        return item
      })
      opts.keyEncoding = 'binary'
      db.batch(array, opts, cb)
    }
  }

  //
  // transform stream to decode data keys
  //
  function streamDecoder(opts) {
    return through.obj(function (data, enc, cb) {
      if (opts.keys) {
        try {
          data.key = decode(data.key, opts)
        }
        catch (err) {
          return cb(err)
        }
      }
      cb(null, data)
    })
  }

  if (typeof db.createReadStream === 'function') {
    this.createReadStream = this.readStream = function (opts) {
      var opts_ = merge({ keys: true }, this.options, opts)
      return db.createReadStream(ns.encodeRange(opts)).pipe(streamDecoder(opts_))
    }
  }

  if (typeof db.createKeyStream === 'function') {
    this.createKeyStream = this.keyStream =  function (opts) {
      var opts_ = merge(opts, { keys: true })
      return db.createKeyStream(ns.encodeRange(opts)).pipe(streamDecoder(opts_))
    }
  }

  if (typeof db.createValueStream === 'function') {
    this.createValueStream = this.valueStream =  function (opts) {
      return db.createValueStream(ns.encodeRange(opts))
    }
  }

  if (typeof db.createLiveStream === 'function') {
    this.createLiveStream = this.liveStream =  function (opts) {
      var opts_ = merge(opts, ns.encodeRange(opts))
      return db.createLiveStream(opts_).pipe(streamDecoder(opts))
    }
  }

}

//
// used to define default options for root subspaces
//
Bytespace.options = {
  keyEncoding: util.defaultOptions.keyEncoding,
  valueEncoding: util.defaultOptions.valueEncoding
}
