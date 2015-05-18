# bytespace

Efficient keypath subspaces prefixed with bytewise tuples.

[![build status](https://travis-ci.org/deanlandolt/bytespace.svg?branch=master)](https://travis-ci.org/deanlandolt/bytespace)

A lot like `level-sublevel` but simpler. Built on `level-updown`, `bytespace` allows you to use arbirarily complex keys for subspace prefixes, and any `keyEncoding` you prefer for your subspace suffix keyspace.


```js
var bytespace = require('bytespace')
var levelup = require('levelup')
var db = levelup('./mydb')

// all the standard levelup options
var options = {
  // keyEncoding defaults to "utf8" just like levelup
  keyEncoding: require('bytewise')
}

// same API as levelup
var appDb = bytespace(db, 'myapp', options)

// or you can require a bytewise-encoded subspaces per sublevel's API
var bwKeyDb = require('bytespace/bytewise')(db)

// you can mount subspaces within subspaces
var nestedDb = bytespace(myapp, 'nested')

// namespace can be any bytewise-serializable value
var testDb = bytespace(appDb, new Date())

// subspaces may also be mounted sublevel-style
var subDb = testDb.sublevel('another')
```

## Rooted keypaths

The subspace db instance itself is essentially a keyspace `chroot` -- a jail you cannot escape with just a reference to the subspace. While the subspace must be provided a reference to a backing db to initialize, this capability should not be surfaced on any properties or methods of the subspace. The capabilities of a subspace are restricted to the subset of of keyspace allocated to it.


## Nested subspaces

When instantiating a subspace, it will test the provided `db` reference to determine if it's a `bytespace` instance. If so, it will call the `sublevel` method with the provided options to create the new subspace. Rather than running through the encode/decode process mulitple times, the responsibility of encoding and decoding keys is delegated to the root subspace. All keys will be correctly prefixed to the appropriate subspace.

The `sublevel` method is API-compatible with [level-sublevel](https://github.com/dominictarr/level-sublevel), though we take an extra `options` argument to allow additional `levelup` db options to be provided.


### Hooks

Precommit and postcommit hooks are implemented using the `pre` and `post` methods from [level-sublevel](https://github.com/dominictarr/level-sublevel)'s API. The optional `range` argument is not yet implemented but it is otherwise API-compatible, allowing a `bytespace` instance with libraries expected a `sublevel` instance.


## Encoding

Subspace keys are encoded as bytewise-prefixed arrays. This allows subspace keys to be appended as the last element of a namespace without overhead. Encoded subspace keys can be appended to the precomputed namespace buffer with a single `Buffer.concat` operation. Mounting a subspace adds another element to the prefix tuple. This serialization ensures that keys of different subspaces cannot interleave.

When encoding keys, the encoded namespace buffer can be efficiently concatenated with a subspace key. When decoding, the namespace portion can be sliced off and ignored. Testing for subspace inclusion is also just a single buffer slice.
