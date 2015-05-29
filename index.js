var Codec = require('level-codec')
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

  function keyOpts(initial) {
    return merge(initial, { keyEncoding: 'binary' })
  }

  function valueOpts(initial) {
    return merge({ valueEncoding: opts.valueEncoding }, initial)
  }

  function allOpts(initial) {
    return valueOpts(keyOpts(initial))
  }

  if (typeof db.get === 'function') {
    this.get = function (k, opts, cb) {
      cb = getCallback(opts, cb)
      opts = getOptions(opts)
      k = encode(k, opts)
      db.get(k, allOpts(opts), cb)
    }
  }

  if (typeof db.del === 'function') {
    this.del = function (k, opts, cb) {
      cb = getCallback(opts, cb)
      opts = getOptions(opts)
      k = encode(k, opts)
      db.del(k, keyOpts(opts), cb)
    }
  }

  if (typeof db.put === 'function') {
    this.put = function (k, v, opts, cb) {
      cb = getCallback(opts, cb)
      opts = getOptions(opts)
      k = encode(k, opts)
      db.put(k, v, allOpts(opts), cb)
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
          return _del.call(batch, encode(k, opts), keyOpts(opts))
        }

        var _put = batch.put
        batch.put = function (k, v, opts) {
          return _put.call(batch, encode(k, opts), v, allOpts(opts))
        }

        return batch
      }

      cb = getCallback(opts, cb)
      opts = getOptions(opts)
      array.map(function (item) {
        item.key = encode(item.key, opts)
        return item
      })
      opts.keyEncoding = 'binary'
      db.batch(array, allOpts(opts), cb)
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
      return readStream(merge({ keys: true, values: true }, valueOpts(opts)))
    }

    this.createKeyStream = this.keyStream =  function (opts) {
      var o = merge(valueOpts(opts), { keys: true, values: false })
      return readStream(merge(valueOpts(opts), { keys: true, values: false }))
    }

    this.createValueStream = this.valueStream = function (opts) {
      return readStream(merge(valueOpts(opts), { keys: false, values: true }))
    }
  }

  //
  // add createLiveStream proxy if available
  //
  if (typeof db.createLiveStream === 'function') {
    this.createLiveStream = this.liveStream =  function (opts) {
      var o = merge(valueOpts(opts), ns.encodeRange(opts))
      return db.createLiveStream(o).pipe(streamDecoder(opts))
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
