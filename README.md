# bytewise-space

Efficient keypath subspaces with bytewise tuples. A lot like `level-sublevel` but simpler. More like `level-spaces`, but allows you to use arbitrarily complex keys for your subspace rather than just strings.


```js
var space = require('bytewise-space')
var levelup = require('levelup')
var db = levelup('./mydb')

// all the standard levelup options
var options = {
  // keyEncoding defaults to "utf8" just like levelup
  keyEncoding: require('bytewise') // or `space.bytewise` as a convenience
}

// same API as levelup
var appDb = space(db, 'myapp', options);

// you can mount subspaces within subspaces
var nestedDb = space(myapp, 'nested')

// namespace can be any bytewise-serializable value
var testDb = space(appDb, new Date())
```

## Rooted keypaths

The subspace db instance itself is essentially a keyspace `chroot` -- a jail you cannot escape with a reference to the subspace alone. The subspace must be provided a reference to soem backing db, but it does not surface this reference and none of its methods will allow access outside of its namespace.


## Nested subspaces

When instantiating a subspace, it will check the provided `db` for a `mountSubspace` method. If available, it will call this method with its namespace and return the resulting subspace instance. Rather than running through the encode/decode process mulitple times, the responsibility of encoding and decoding keys is delegated to the root subspace, but all keys will be correctly prefixed within the subspace.


## Encoding

Subspace keys are encoded as length-prefixed arrays. This allows subspace keys to be appended as the last element of a namespace without the overhead required to escape certain bytes in regular arrays. Encoded subspace keys can be appended to the precomputed namespace buffer with a single `Buffer.concat` operation. Keys of a top level subspace would be two element tuple, the first element being the namespace key. Mounting a subspace adds another element to this tuple. The last element is always the subspace-specific key of each record. This serialization ensures that keys of different subspaces cannot possibly interleave, while allowing for inexpensive key manipulation. When encoding keys, the encoded namespace buffer can be efficiently concatenated with a subspace key. When decoding, the namespace portion can be slice off and ignored.


## Subspace keys

The last element of the keypath tuple -- the subspace key -- is serialized as a buffer and concatenated to the namespace buffer. If you're using bytewise keys in your subspace you will end up with a complete keypath that's correctly bytewise-encoded. If using another encoding, the final keypath cannot be decoded directly in bytewise, but the fact that keys are length-prefixed should be enough of a clue for tooling (e.g. levelui) to treat the last element as an opaque buffer, just as if it were a top level key.
