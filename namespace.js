var assert = require('assert')
var bytewise = require('bytewise-core')
var equal = require('bytewise-core/util').equal

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
  assert(Buffer.isBuffer(k))

  if (!this.contains(k))
    return k

  //
  // slice off prefix and return the rest
  //
  return k.slice(this.buffer.length)
}

Namespace.prototype.encode = function (k, space) {
  if (typeof k === 'string')
    k = new Buffer(k)

  return Buffer.concat([ this.buffer, k ])
}

//
// loop over hooks and trigger in the context of subspace
//
Namespace.prototype.trigger = function(hooks, args) {
  for (var i = 0, len = hooks.length; i < len; i++) {
    hooks[i].apply(this.db, args)
  }
}

module.exports = Namespace
