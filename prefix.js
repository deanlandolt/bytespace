var assert = require('assert')
var bytewise = require('bytewise-core')
var equal = require('bytewise-core/util').equal

//
// brand prefix instance to keep track of subspace root
//
function Prefix(path) {
  this.path = path
  this.buffer = bytewise.encode(path)
}

Prefix.prototype.append = function (ns) {
  return new Prefix(this.path.concat(ns))
}

Prefix.prototype.contains = function (k) {
  //
  // slice to get key prefix
  //
  return equal(this.buffer, k.slice(0, this.buffer.length))
}

Prefix.prototype.decode = function (k) {
  assert(Buffer.isBuffer(k))

  if (!this.contains(k))
    return k

  //
  // slice off prefix and return the rest
  //
  return k.slice(this.buffer.length)
}

Prefix.prototype.encode = function (k, space) {
  if (typeof k === 'string')
    k = new Buffer(k)

  return Buffer.concat([ this.buffer, k ])
}

module.exports = Prefix
