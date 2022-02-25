import CLMDB
import Foundation

public class Environment {
	/*
	Configuration flags of the environment
	*/
	public struct Flags: OptionSet {
		public let rawValue: UInt32
		public init(rawValue: UInt32) { self.rawValue = rawValue }

		public static let fixedMap = Flags(rawValue: UInt32(MDB_FIXEDMAP))
		public static let noSubDir = Flags(rawValue: UInt32(MDB_NOSUBDIR))
		public static let noSync = Flags(rawValue: UInt32(MDB_NOSYNC))
		public static let readOnly = Flags(rawValue: UInt32(MDB_RDONLY))
		public static let noMetaSync = Flags(rawValue: UInt32(MDB_NOMETASYNC))
		public static let writeMap = Flags(rawValue: UInt32(MDB_WRITEMAP))
		public static let mapAsync = Flags(rawValue: UInt32(MDB_MAPASYNC))
		public static let noTLS = Flags(rawValue: UInt32(MDB_NOTLS))
		public static let noLock = Flags(rawValue: UInt32(MDB_NOLOCK))
		public static let noReadahead = Flags(rawValue: UInt32(MDB_NORDAHEAD))
		public static let noMemoryInit = Flags(rawValue: UInt32(MDB_NOMEMINIT))
	}

	/*
	Opened Database Cache
	*/
//    var databases = [String:Database]()
//    let databaseAccess = DispatchQueue(label:"com.tannersilva.database.sync")

	/*
	Primary environment handle
	*/
	public var handle:OpaquePointer? = nil
		
	/*
	Configuration flags getters and setters
	Note: environments that are initialized as readonly cannot be configured at a later time for readwrite access
	*/
	public var flags:Flags {
		get {
			var captureFlags = UInt32()
			mdb_env_get_flags(handle, &captureFlags)
			return Flags(rawValue:captureFlags)
		}
		set {
			mdb_env_set_flags(handle, newValue.rawValue, 1)
		}
	}
	public var max_keysize:Int32 {
		get {
			return mdb_env_get_maxkeysize(handle)
		}
	}
	
	/*
	Primary initializer
	*/
	public init(path:String, flags:Flags = [], mapSize:size_t? = nil, maxReaders:MDB_dbi? = 256, maxDBs:MDB_dbi? = 128) throws {
		var environmentHandle:OpaquePointer? = nil;
		let envStatus = mdb_env_create(&environmentHandle)
		guard envStatus == 0 else {
			throw LMDBError(returnCode:envStatus)
		}
						
		//set maximum db count
		if maxDBs != nil {
			let mdbSetResult = mdb_env_set_maxdbs(environmentHandle, maxDBs!)
			guard mdbSetResult == 0 else {
				print("[LMDB] Unable to set maximum database count.")
				throw LMDBError(returnCode:mdbSetResult)
			}
		}
		
		//set the map size
		if mapSize != nil {
			let mdbSetResult = mdb_env_set_mapsize(environmentHandle, mapSize!)
			guard mdbSetResult == 0 else {
				print("[LMDB] Unable to set mapsize.")
				throw LMDBError(returnCode:mdbSetResult)
			}
		}
				
		try path.withCString { stringBuffer in
			let fileMode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH
			let openStatus = mdb_env_open(environmentHandle, stringBuffer, UInt32(flags.rawValue), fileMode)
			guard openStatus == 0 else {
				throw LMDBError(returnCode:openStatus)
			}
		}
		self.handle = environmentHandle
	}
	
	
	public func transact<R>(readOnly:Bool = true, _ txFunc:(Transaction) throws -> R) rethrows -> R {
		//create the new transaction
		let newTransaction = try! Transaction(environment:self.handle, readOnly:readOnly, parent:nil)
		
		let captureValue:R
		//run the transaction handler
		do {
			captureValue = try txFunc(newTransaction)
		} catch let error {
			//if the transaction handler throws, abort the transaction and return 
			if newTransaction.isOpen == true {
				newTransaction.abort()
			}
			throw error
		}
		if newTransaction.isOpen == true {
			try! newTransaction.commit()
		}
		return captureValue
	}

	public func setMapSize(_ newMapSize:size_t) throws {
		let mdbSetResult = mdb_env_set_mapsize(handle, newMapSize)
		guard mdbSetResult == 0 else {
			throw LMDBError(returnCode:mdbSetResult)
		}
	}
	
	public func openDatabase(named databaseName:String, flags:Database.Flags = Database.Flags(rawValue:0), tx:Transaction) throws -> Database {
		return try Database(environment:self.handle, name:databaseName, flags:flags, tx:tx)
	}
	
	public func deleteDatabase(_ database:Database, tx:Transaction?) throws {
		try database.delete(tx:tx)
	}

	public func copyTo(path:URL, performCompaction:Bool) throws {
		try path.path.withCString { pathCString in
			let copyFlags:UInt32 = (performCompaction == true) ? UInt32(MDB_CP_COMPACT) : 0
			let copyResult = mdb_env_copy2(handle, pathCString, copyFlags)
			guard copyResult == 0 else {
				throw LMDBError(returnCode:copyResult)
			}
		}
	}
	
	/// Flush the data buffers to the disk. This function is primarily useful in situations where an Environment was opened with ``Flags.noSync`` or ``Flags.noMetaSync``. This call is invalid if the environment was opened with ``Flags.readOnly``.
	/// - Parameter force: Force a synchronous flush when true. If the Environment was opened with ``Environment.Flags.noSync``, flushes will be omitted when this parameter is false. If the Environment was opened with ``Environment.Flags.mapAsync``, flushes will be asynchronous when this parameter is false.
	public func sync(force:Bool = true) throws {
		let syncStatus = mdb_env_sync(self.handle, (force == true ? 1 : 0))
		guard syncStatus == 0 else {
			throw LMDBError(returnCode:syncStatus)
		}
	}
	
	deinit {
		mdb_env_close(handle)
	}
}
