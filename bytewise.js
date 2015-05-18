var bytespace = require('./')
var bytewise = require('bytewise-core')
var merge = require('xtend')

module.exports = function (db, ns, opts) {
  return bytespace(db, ns, merge({ keyEncoding: bytewise }, opts))
}
