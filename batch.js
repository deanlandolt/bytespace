function Batch(space) {
  this.ops = []
  this._space = space
}

Batch.prototype.put = function (key, value, options) {
  this.ops.push({ type: 'put', key: key, value: value, options: options })
  return this
}

Batch.prototype.del = function (key, options) {
  this.ops.push({ type: 'del', key: key, options: options })
  return this
}

Batch.prototype.clear = function () {
  this.ops = []
  return this
}

Batch.prototype.write = function (cb) {
  this._space.batch(this.ops, null, cb)
}

module.exports = Batch
