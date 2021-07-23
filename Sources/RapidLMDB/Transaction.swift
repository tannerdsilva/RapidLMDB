import CLMDB
import Foundation

public class Transaction {
	public struct Flags:OptionSet {
        public let rawValue:UInt32
        public init(rawValue:UInt32) { self.rawValue = rawValue }
    
		public static let noOverwrite = Flags(rawValue: UInt32(MDB_NOOVERWRITE))
        public static let noDupData = Flags(rawValue: UInt32(MDB_REVERSEKEY))
		
		public static let current = Flags(rawValue: UInt32(MDB_CURRENT))
		public static let reserve = Flags(rawValue: UInt32(MDB_RESERVE))
		public static let append = Flags(rawValue: UInt32(MDB_APPEND))
		public static let appendDup = Flags(rawValue: UInt32(MDB_APPENDDUP))
		public static let multiple = Flags(rawValue: UInt32(MDB_MULTIPLE))
    }
	
	public typealias Handler = (Transaction) throws -> Void
	
	public var env_handle:OpaquePointer?
	public var handle:OpaquePointer?
	
	internal var isOpen = true
	
	//FOR INTERNAL USE
	//quickly spawn a transaction for one-time use. This function is typically used to create single-use transactions when a user has not specified a transaction 
	internal static func instantTransaction<R>(environment:OpaquePointer?, readOnly:Bool, parent:OpaquePointer?, _ handler:(Transaction) throws -> R) rethrows -> R {
		let newTransaction = try! Transaction(environment:environment, readOnly:readOnly, parent:parent)
		let captureReturn:R
		do {
			captureReturn = try handler(newTransaction)
		} catch let error {
			if newTransaction.isOpen == true {
				newTransaction.abort()
			}
			throw error
		}
		if newTransaction.isOpen == true {
			try! newTransaction.commit()
		}
		return captureReturn
	}
	
	//user cant directly make transactions sorry. trying to simplify usage by internalizing the explicit initialization of these things
	internal init(environment:OpaquePointer?, readOnly:Bool, parent:OpaquePointer? = nil) throws {
		self.env_handle = environment
		var start_handle:OpaquePointer? = nil
		var flags:UInt32  = 0
		if (readOnly == true) {
			flags = UInt32(MDB_RDONLY)
		}
		let createResult = mdb_txn_begin(environment, parent, flags, &start_handle)
		guard createResult == 0 else {
			throw LMDBError(returnCode:createResult)
		}
		self.handle = start_handle
	}
	
	public func subTransact(readOnly:Bool = true) throws -> Transaction {
		return try Transaction(environment:self.env_handle, readOnly:readOnly, parent:self.handle)
	}
	
	/*
	Actions that can be taken on a transaction object.
	*/
	public func commit() throws {
		let commitResult = mdb_txn_commit(handle)
		guard commitResult == 0 else {
			throw LMDBError(returnCode:commitResult)
		}
		self.isOpen = false
	}
	public func reset() {
		mdb_txn_reset(handle)
	}
	public func renew() throws {
		let renewResult = mdb_txn_renew(handle)
		guard renewResult == 0 else {
			throw LMDBError(returnCode:renewResult)
		}
	}
	public func abort() {
		mdb_txn_abort(handle)
		self.isOpen = false
	}
}