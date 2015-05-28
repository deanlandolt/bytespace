var bytespace = require('./')
var merge = require('xtend')

//
// adapt keys in method arguments to respect prefix
//
function adapt(method, description) {
  // TODO
  return method
}

module.exports = function (db, ns, opts) {
  var space = bytespace(db, ns, merge({ factory: module.exports }, opts))
  var methods = db.methods || {}
  for (var key in methods)
    space[key] = adapt(db[key], methods[key])

  //
  // underlying db methods not exposed by default
  //
  space.methods = {}

  return space
}
