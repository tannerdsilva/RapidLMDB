# RapidLMDB

RapidLMDB is a Swift LMDB framework designed to provide the developer full access to the LMDB API while also simultaneously making the syntax as efficient as possible. Designed for rapid development and performance, RapidLMDB is an uncompromising Swift wrapper to the LMDB core.

LMDB version: 0.9.28

# Getting Started

Open an LMDB environment with the following declaration:

`let env = try Environment(path:"/home/somebody/lmdb_helloworld", mapSize:5000000)`

With the newly initialized Environment class, a transaction can now be opened where further actions can take place...

```
try env.transact(readOnly:false) { txHandle in
	//create a new metadata database
	let newMetadataDatabase = try env.openDatabase(named:"metadata", flags:[.create], tx:txHandle)
	try newMetadataDatabase.set(value:"1", forKey:"version", tx:txHandle)
} 
```

Transaction blocks (such as the one shown above) are automatically committed when complete. Transactions may be aborted by throwing an error within the transaction block. Furthermore, transaction handles may be directly reset and renewed with the `Transaction` instance functions `renew()` and `reset()`.

Transaction blocks will transparently return the values of their containing code blocks, including tuples. This makes it very easy to batch-initialize databases within transactions, like such:

```
let (timelineDatabase, metadataDatabase) = try env.transact(readOnly:false) { txHandle in
	let a = try env.openDatabase(named:"timeline", tx:txHandle)
	let b = try env.openDatabase(named:"metadata", tx:txHandle)
	
	return (a, b)
	//transaction is automatically comitted
}
```

LMDB does not allow for concurrent opening or creation of databases. In situations where databases must be created or opened in a concurrent environment, `nil` may be passed as the transaction argument. In such a case, RapidLMDB will internally serialize these database initializations.

`let someConcurrentInitialization = try env.openDatabse(named:"timeline", tx:nil)`

## Data handling within transactions

Key / value retrieval happens one of two says with RapidLMDB:

1. Simple serialization to-and-from a specified type via `Database`

	The database structure allows easy and quick (JSON) serialization of Arrays, Dictionaries, and Sets of `Codable` compliant objects. Furthermore, `Database` will serialize objects directly to bytestreams using the `DataConvertible` protocol. When retrieving a value from the database in this way, one must specify a value type when retrieving a key.
	
	`let verisonNumber try metadataDatabase.get(type:String.self, forKey:"version", tx:txHandle)`
	
	In this case, the `get` command will throw if the key is not found, or if there is an internal error with LMDB. `get` will return nil if a key is found, but the key could not be serialized to the specified data type.
	
	For simple assignments or retreivals, `nil` may be passed as the transaction argument. In this case, `Database` will create and handle the transaction lifecycle internally.

	`try metadataDatabase.set(value:"512", forKey:"maxConnections", tx:nil)`
	
2. Fast and direct bytestream access via `Cursor`

	Open a database cursor with the following declaration:
	
	`let timelineCursor = try timelineDatabase.cursor(tx:txHandle)`
	
	For key retrieval, `Cursor` does not concern itself with parsing data. Unlike `Database`, it will simply return a key/value tuple with data directly from the database.
	
	Cursor integrates with Swift enumeration, allowing for convenient traversal of database contents.
	
	```
	for (i, keyValue) in timelineCursor.enumerated() {
		print("\(i) - \(keyValue.key) : \(keyValue.value)")
	}
	```
	
	Since cursors are always used within transactions, Cursor always returns data that is initialized directly from the database memorymap.
	
## DataConvertible: Supported Types

 - Dictionary where Key:Codable, Value:Coadble -> [Encodes JSON]
 
 - Array where Element:Codable -> [Encodes JSON]
 
 - Set where Element:Codable -> [Encodes to JSON]
 
 - Data
 
 - String
 
 - Bool
 
 - Int
 
 - Int8
 
 - Int16
 
 - Int32
 
 - Int64
 
 - UInt
 
 - UInt8
 
 - UInt16
 
 - UInt32
 
 - UInt64
 
 - Float
 
 - Double
 
 - Date
	
## Roadmap

 - `MDB_RESERVE` support