import Foundation
import CLMDB

//fileprivate let openDatabaseGuard = DispatchQueue(label:"com.tannerdsilva.lmdb.dbi.open-serial")

public struct Database {
	public struct Flags:OptionSet {
        public let rawValue:UInt32
        public init(rawValue:UInt32) { self.rawValue = rawValue }
    
        public static let reverseKey = Flags(rawValue: UInt32(MDB_REVERSEKEY))
        public static let dupSort = Flags(rawValue: UInt32(MDB_DUPSORT))
        public static let integerKey = Flags(rawValue: UInt32(MDB_INTEGERKEY))
        public static let dupFixed = Flags(rawValue: UInt32(MDB_DUPFIXED))
        public static let integerDup = Flags(rawValue: UInt32(MDB_INTEGERDUP))
        public static let reverseDup = Flags(rawValue: UInt32(MDB_REVERSEDUP))
        public static let create = Flags(rawValue: UInt32(MDB_CREATE))
    }
    public struct Statistics {
    	public let pageSize:UInt32
    	public let depth:UInt32
    	public let branch_pages:size_t
    	public let leaf_pages:size_t
    	public let overflow_pages:size_t
    	public let entries:size_t
    }
    
    public let name:String
    public let env_handle:OpaquePointer?
    public let db_handle:MDB_dbi
    
    //primary initializer
	public init(environment:OpaquePointer?, name:String, flags:Flags, tx:Transaction) throws {
		var captureHandle = MDB_dbi()
		try name.withCString { namePointer in
			let openDatabaseResult = mdb_dbi_open(tx.handle, namePointer, UInt32(flags.rawValue), &captureHandle)
			guard openDatabaseResult == 0 else {
				throw LMDBError(returnCode:openDatabaseResult)
			}
		}
		self.db_handle = captureHandle
		self.env_handle = environment
		self.name = name
	}
	
	/*
	Metadata related to the database
	*/
	public func getStatistics(tx:Transaction?) throws -> Statistics {
		var statObj = MDB_stat()
		if tx != nil { 
			let getStatTry = mdb_stat(tx!.handle, db_handle, &statObj)
			return Statistics(pageSize:statObj.ms_psize, depth:statObj.ms_depth, branch_pages:statObj.ms_branch_pages, leaf_pages:statObj.ms_leaf_pages, overflow_pages:statObj.ms_overflow_pages, entries:statObj.ms_entries)	
		} else {
			return try Transaction.instantTransaction(environment:env_handle, readOnly:true, parent:nil) { someTransaction in
				var statObj = MDB_stat()
				let getStatTry = mdb_stat(someTransaction.handle, db_handle, &statObj)
				return Statistics(pageSize:statObj.ms_psize, depth:statObj.ms_depth, branch_pages:statObj.ms_branch_pages, leaf_pages:statObj.ms_leaf_pages, overflow_pages:statObj.ms_overflow_pages, entries:statObj.ms_entries)	
			}
		}
	}
	
	public func getFlags(tx:Transaction?) throws -> Flags {
		var captureFlags = UInt32()
		if tx != nil {
			mdb_dbi_flags(tx!.handle, db_handle, &captureFlags)
			return Flags(rawValue:captureFlags)
		} else {
			return try Transaction.instantTransaction(environment:env_handle, readOnly:true, parent:nil) { someTransaction in
				mdb_dbi_flags(someTransaction.handle, db_handle, &captureFlags)
				return Flags(rawValue:captureFlags)
			}
		}
	}
	
	public func getCount(tx:Transaction? = nil) throws -> size_t {
		return try self.getStatistics(tx:tx).entries
	}
	
	/*
	Assigning and retrieving keys
	*/
	
	//basic getter
	public func get<K:DataConvertible, V:DataConvertible>(type:V.Type, forKey key:K, tx:Transaction?) throws -> V? {
		var keyData = key.exportData()
		return try V(data:keyData.withUnsafeMutableBytes { kBuffPointer -> Data in
			var keyVal = MDB_val(mv_size:kBuffPointer.count, mv_data:kBuffPointer.baseAddress)
			var dataVal = MDB_val()
		
			if tx != nil {
				let valueResult = mdb_get(tx!.handle, db_handle, &keyVal, &dataVal)
				guard valueResult == 0 else {
					throw LMDBError(returnCode:valueResult)
				}
			} else {
				let valueResult = try Transaction.instantTransaction(environment:env_handle, readOnly:true, parent:nil) { someTransaction in
					return mdb_get(someTransaction.handle, db_handle, &keyVal, &dataVal)
				}
				guard valueResult == 0 else {
					throw LMDBError(returnCode:valueResult)
				}
			}
		
			return Data(bytes:dataVal.mv_data, count:dataVal.mv_size)
		})
	}
	
	//basic setter
	public func set<K:DataConvertible, V:DataConvertible>(value:V, forKey key:K, flags:Transaction.Flags? = nil, tx:Transaction?) throws {
		var keyData = key.exportData()
		var valueData = value.exportData()
		try keyData.withUnsafeMutableBytes { keyBufferPointer in
			var keyStruct = MDB_val(mv_size:keyBufferPointer.count, mv_data:keyBufferPointer.baseAddress)
			try valueData.withUnsafeMutableBytes { valueBufferPointer in
				var valueStruct = MDB_val(mv_size:valueBufferPointer.count, mv_data:valueBufferPointer.baseAddress)
				if tx != nil {
					let insertResult = mdb_put(tx!.handle, db_handle, &keyStruct, &valueStruct, ((flags != nil) ? flags!.rawValue : 0))
					guard insertResult == 0 else {
						throw LMDBError(returnCode:insertResult)
					}
				} else {
					try Transaction.instantTransaction(environment:env_handle, readOnly:false, parent:nil) { someTransaction in 
						let insertResult = mdb_put(someTransaction.handle, db_handle, &keyStruct, &valueStruct, ((flags != nil) ? flags!.rawValue : 0))
						guard insertResult == 0 else {
							throw LMDBError(returnCode:insertResult)
						}
					}
				}				
			}
		}
	}
	
	//basic delete
	public func delete<K:DataConvertible>(key:K, tx:Transaction?) throws {
		var keyData = key.exportData()
		try keyData.withUnsafeMutableBytes { kBuffPointer in
			var keyStruct = MDB_val(mv_size:kBuffPointer.count, mv_data:kBuffPointer.baseAddress)
			
			if tx == nil {
				try Transaction.instantTransaction(environment:env_handle, readOnly:false, parent:nil) { someTransaction in
					let captureResult = mdb_del(someTransaction.handle, db_handle, &keyStruct, nil)
					guard captureResult == 0 else {
						throw LMDBError(returnCode:captureResult)
					}
				}
			} else {
				let captureResult = mdb_del(tx!.handle, db_handle, &keyStruct, nil)
				guard captureResult == 0 else {
					throw LMDBError(returnCode:captureResult)
				}
			}
		}
	}
	
	//basic function for checking if a key exists
	public func contains<K:DataConvertible>(key:K, tx:Transaction?) throws -> Bool {
		var keyData = key.exportData()
		return try keyData.withUnsafeMutableBytes { kBuffPointer -> Bool in
			var keyVal = MDB_val(mv_size:kBuffPointer.count, mv_data:kBuffPointer.baseAddress)
			var dataVal = MDB_val()
		
			if tx != nil {
				let valueResult = mdb_get(tx!.handle, db_handle, &keyVal, &dataVal)
				if valueResult == 0 {
					return true
				} else if valueResult == MDB_NOTFOUND {
					return false
				} else {
					throw LMDBError(returnCode:valueResult)
				}
			} else {
				let valueResult = try Transaction.instantTransaction(environment:env_handle, readOnly:true, parent:nil) { someTransaction in
					return mdb_get(someTransaction.handle, db_handle, &keyVal, &dataVal)
				}
				if valueResult == 0 {
					return true
				} else if valueResult == MDB_NOTFOUND {
					return false
				} else {
					throw LMDBError(returnCode:valueResult)
				}
			}
		}
	}
	
	//empty a database without removing it from the environment
	public func empty(tx:Transaction?) throws {
		if (tx == nil) {
			try Transaction.instantTransaction(environment:env_handle, readOnly:false, parent:nil) { someTrans in
				let captureResult = mdb_drop(someTrans.handle, db_handle, 0)
				guard captureResult == 0 else {
					throw LMDBError(returnCode:captureResult)
				}
			}
		} else {
			let captureResult = mdb_drop(tx!.handle, db_handle, 0)
			guard captureResult == 0 else {
				throw LMDBError(returnCode:captureResult)
			}
		}
	}
	
	//remove the database from the environment
	internal func delete(tx:Transaction?) throws {
		if (tx == nil) {
			try Transaction.instantTransaction(environment:env_handle, readOnly:false, parent:nil) { someTrans in
				let captureResult = mdb_drop(someTrans.handle, db_handle, 1)
				guard captureResult == 0 else {
					throw LMDBError(returnCode:captureResult)
				}
			}
		} else {
			let captureResult = mdb_drop(tx!.handle, db_handle, 1)
			guard captureResult == 0 else {
				throw LMDBError(returnCode:captureResult)
			}
		}
	}
	
	/*
	Cursors
	*/
	public func cursor(tx:Transaction) throws -> Cursor {
		return try Cursor(transaction:tx.handle, db:self.db_handle)
	}
}