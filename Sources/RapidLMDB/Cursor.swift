import CLMDB
import Foundation

//Cursor - provides a mechanism to iterate through the contents of an LMDB key/value database.
//This Cursor class does not copy the data that is read from the database. Data objects that are returned are initialized from pointers directly returned from the database memory map.
//A cursor can be initialized with the existence of two instances: a database and a transaction
public class Cursor:Sequence {
	
	//typealias classes
	public typealias Element = (key:Data, value:Data)
	public typealias Iterator = CursorIterator
	
	//CursorIterator helps the `Cursor` class conform to the `Sequence` protocol that allows for easily iterating over a databases contents
	public struct CursorIterator:IteratorProtocol {
		internal let handle:OpaquePointer?
		var first:Bool = true
		public var count:Int
		
		fileprivate init(count:Int, handle:OpaquePointer?) {
			self.handle = handle
			self.count = count
		}
		
		public mutating func next() -> (key:Data, value:Data)? {
			let cursorOp:MDB_cursor_op
			if first == true {
				cursorOp = MDB_FIRST
				first = false
			} else {
				cursorOp = MDB_NEXT
			}
			var captureKey = MDB_val()
			var captureVal = MDB_val()
			let cursorResult = mdb_cursor_get(handle, &captureKey, &captureVal, cursorOp)
			guard cursorResult == 0 else {
				return nil
			}
			let keyData = Data(bytesNoCopy:captureKey.mv_data, count:captureKey.mv_size, deallocator:.none)
			let valueData = Data(bytesNoCopy:captureVal.mv_data, count:captureVal.mv_size, deallocator:.none)
			return (key:keyData, value:valueData)
		}
	}
	
	//cursor get operations
	public enum Operation {
		case first
		case firstDup
		case getBoth
		case getBothRange
		case getCurrent
		case getMultiple
		case last
		case lastDup
		case next
		case nextDup
		case nextMultiple
		case nextNoDup
		case previous
		case previousDup
		case previousNoDup
		case set
		case setKey
		case setRange
		
		public init(mdbValue:MDB_cursor_op) {
			switch mdbValue {
				case MDB_FIRST:
					self = .first
				case MDB_FIRST_DUP:
					self = .firstDup
				case MDB_GET_BOTH:
					self = .getBoth
				case MDB_GET_BOTH_RANGE:
					self = .getBothRange
				case MDB_GET_CURRENT:
					self = .getCurrent
				case MDB_GET_MULTIPLE:
					self = .getMultiple
				case MDB_LAST:
					self = .last
				case MDB_LAST_DUP:
					self = .lastDup
				case MDB_NEXT:
					self = .next
				case MDB_NEXT_DUP:
					self = .nextDup
				case MDB_NEXT_MULTIPLE:
					self = .nextMultiple
				case MDB_NEXT_NODUP:
					self = .nextNoDup
				case MDB_PREV:
					self = .previous
				case MDB_PREV_DUP:
					self = .previousDup
				case MDB_PREV_NODUP:
					self = .previousNoDup
				case MDB_SET:
					self = .set
				case MDB_SET_KEY:
					self = .setKey
				case MDB_SET_RANGE:
					self = .setRange
				default:
					self = .next
			}
		}
		
		public var mdbValue:MDB_cursor_op {
			get {
				switch self {
					case .first:
						return MDB_FIRST
					case .firstDup:
						return MDB_FIRST_DUP
					case .getBoth:
						return MDB_GET_BOTH
					case .getBothRange:
						return MDB_GET_BOTH_RANGE
					case .getCurrent:
						return MDB_GET_CURRENT
					case .getMultiple:
						return MDB_GET_MULTIPLE
					case .last:
						return MDB_LAST
					case .lastDup:
						return MDB_LAST_DUP
					case .next:
						return MDB_NEXT
					case .nextDup:
						return MDB_NEXT_DUP
					case .nextMultiple:
						return MDB_NEXT_MULTIPLE
					case .nextNoDup:
						return MDB_NEXT_NODUP
					case .previous:
						return MDB_PREV
					case .previousDup:
						return MDB_PREV_DUP
					case .previousNoDup:
						return MDB_PREV_NODUP
					case .set:
						return MDB_SET
					case .setKey:
						return MDB_SET_KEY
					case .setRange:
						return MDB_SET_RANGE
				}
			}
		}
	}
	
	//a cursor consists of the cursor handle: `handle`, the database handle that created the cursor: `db_handle`, and the transaction it was created in: `tx_handle`
	public let handle:OpaquePointer?
	public let db_handle:MDB_dbi
	public let tx_handle:OpaquePointer?
	public let readOnly:Bool
	
	//initializer
	internal init(transaction:OpaquePointer?, db:MDB_dbi, readOnly:Bool) throws {
		var buildCursor:OpaquePointer? = nil
		let openCursorResult = mdb_cursor_open(transaction, db, &buildCursor)
		guard openCursorResult == 0 else {
			throw LMDBError(returnCode:openCursorResult)
		}
		self.handle = buildCursor
		self.db_handle = db
		self.tx_handle = transaction
		self.readOnly = readOnly
	}
	
	//returns the amount of values that are stored in the database for the particular
	public func currentKeyCount() throws -> size_t {
		var countVar = size_t()
		let countResult = mdb_cursor_count(handle, &countVar)
		guard countResult == 0 else {
			throw LMDBError(returnCode:countResult)
		}
		return countVar
	}

	//get command where key and value are provided as input
	public func get<K:DataConvertible, V:DataConvertible>(_ operation:Operation, key inputKey:K, value inputValue:V) throws -> (key:Data, value:Data)  {
		func executeOperation(key:UnsafeMutablePointer<MDB_val>?, value:UnsafeMutablePointer<MDB_val>?) throws -> (key:Data, value:Data) {
			let cursorResult = mdb_cursor_get(handle, key, value, operation.mdbValue)
			guard cursorResult == 0 else {
				throw LMDBError(returnCode:cursorResult)
			}
			let keyData = Data(bytesNoCopy:key!.pointee.mv_data, count:key!.pointee.mv_size, deallocator:.none)
			let valueData = Data(bytesNoCopy:value!.pointee.mv_data, count:value!.pointee.mv_size, deallocator:.none)
			return (key:keyData, value:valueData)
		}
		
		var keyData = inputKey.exportData()
		var valueData = inputValue.exportData()
		return try keyData.withUnsafeMutableBytes { kDataBuff in
			return try valueData.withUnsafeMutableBytes { vDataBuff in
				var keyStruct = MDB_val(mv_size:kDataBuff.count, mv_data:kDataBuff.baseAddress)
				var valStruct = MDB_val(mv_size:vDataBuff.count, mv_data:vDataBuff.baseAddress)
				return try executeOperation(key:&keyStruct, value:&valStruct)
			}
		}
	}
	
	//get command where a value is provided, but no key
	public func get<V:DataConvertible>(_ operation:Operation, value inputValue:V) throws -> (key:Data, value:Data) {
		var valueData = inputValue.exportData()
		return try valueData.withUnsafeMutableBytes { vDataBuff in
			var keyStruct = MDB_val(mv_size:0, mv_data:nil)
			var valStruct = MDB_val(mv_size:vDataBuff.count, mv_data:vDataBuff.baseAddress)
			let cursorResult = mdb_cursor_get(handle, &keyStruct, &valStruct, operation.mdbValue)
			guard cursorResult == 0 else {
				throw LMDBError(returnCode:cursorResult)
			}
			let keyData = Data(bytesNoCopy:keyStruct.mv_data, count:keyStruct.mv_size, deallocator:.none)
			let valueData = Data(bytesNoCopy:valStruct.mv_data, count:valStruct.mv_size, deallocator:.none)
			return (keyData, valueData)
		}
	}
	
	//get command where a key is provided, but no value
	public func get<K:DataConvertible>(_ operation:Operation, key inputKey:K) throws -> (key:Data, value:Data) {
		var keyData = inputKey.exportData()
		return try keyData.withUnsafeMutableBytes { kDataBuff in
			var keyStruct = MDB_val(mv_size:kDataBuff.count, mv_data:kDataBuff.baseAddress)
			var valStruct = MDB_val(mv_size:0, mv_data:nil)
			let cursorResult = mdb_cursor_get(handle, &keyStruct, &valStruct, operation.mdbValue)
			guard cursorResult == 0 else {
				throw LMDBError(returnCode:cursorResult)
			}
			let keyData = Data(bytesNoCopy:keyStruct.mv_data, count:keyStruct.mv_size, deallocator:.none)
			let valueData = Data(bytesNoCopy:valStruct.mv_data, count:valStruct.mv_size, deallocator:.none)
			return (key:keyData, value:valueData)
		}
	}
	
	//get command where no key or value are provided as input
	public func get(_ operation:Operation) throws -> (key:Data, value:Data) {
		var captureKey = MDB_val()
		captureKey.mv_size = 0
		var captureVal = MDB_val()
		captureVal.mv_size = 0
		let cursorResult = mdb_cursor_get(handle, &captureKey, &captureVal, operation.mdbValue)
		guard cursorResult == 0 else {
			throw LMDBError(returnCode:cursorResult)
		}
		let keyData = Data(bytesNoCopy:captureKey.mv_data, count:captureKey.mv_size, deallocator:.none)
		let valueData = Data(bytesNoCopy:captureVal.mv_data, count:captureVal.mv_size, deallocator:.none)
		return (key:keyData, value:valueData)
	}
	
	//assign a key/value to the database with optional flags
	public func set<K:DataConvertible, V:DataConvertible>(value:V, forKey key:K, flags:Transaction.Flags? = nil) throws {
		var keyData = key.exportData()
		var valueData = value.exportData()
		try keyData.withUnsafeMutableBytes { kDataBuff in
			try valueData.withUnsafeMutableBytes { vDataBuff in
				var keyStruct = MDB_val(mv_size:kDataBuff.count, mv_data:kDataBuff.baseAddress)
				var valueStruct = MDB_val(mv_size:vDataBuff.count, mv_data:vDataBuff.baseAddress)
				let putResult = mdb_cursor_put(handle, &keyStruct, &valueStruct, ((flags != nil) ? flags!.rawValue : 0))
				guard putResult == 0 else {
					throw LMDBError(returnCode:putResult)
				}
			}
		}
	}
	
	//optimized convenience function that checks if a key exists in the database
	public func contains<K:DataConvertible>(key:K) throws -> Bool {
		var keyData = key.exportData()
		return try keyData.withUnsafeMutableBytes { kBuffPointer -> Bool in
			var keyVal = MDB_val(mv_size:kBuffPointer.count, mv_data:kBuffPointer.baseAddress)
			var dataVal = MDB_val()
			let valueResult = mdb_cursor_get(handle, &keyVal, &dataVal, MDB_SET_KEY)
			if valueResult == 0 {
				return true
			} else if valueResult == MDB_NOTFOUND {
				return false
			} else {
				throw LMDBError(returnCode:valueResult)
			}
		}
	}
	
	//deletes the currently selected key/value from the database
	public func deleteCurrent(flags:Transaction.Flags? = nil) throws {
		let deleteResult = mdb_cursor_del(handle, ((flags != nil) ? flags!.rawValue : 0))
		guard deleteResult == 0 else {
			throw LMDBError(returnCode:deleteResult)
		}
	}
	
	//allows for easy swift enumeration over the contents that this cursor can navigate
	public func makeIterator() -> CursorIterator {
		var statObject = MDB_stat()
		guard mdb_stat(self.tx_handle, self.db_handle, &statObject) == 0 else {
			return CursorIterator(count:0, handle:self.handle)
		}
		return CursorIterator(count:Int(statObject.ms_entries), handle:self.handle)
	}
	
	public func comareKeys(_ data1:inout MDB_val, _ data2:inout MDB_val) -> Int32 {
		return mdb_cmp(self.tx_handle, self.db_handle, &data1, &data2)
	}
	
	public func compareValues(_ data1:inout MDB_val, _ data2:inout MDB_val) -> Int32 {
		return mdb_dcmp(self.tx_handle, self.db_handle, &data1, &data2)
	}

	
	//compare two values according to the key comparison function of the database
	public func compareKeys<D:DataConvertible>(_ data1:D, _ data2:D) -> Int32 {
		var data1Export = data1.exportData()
		var data2Export = data2.exportData()
		return data1Export.withUnsafeMutableBytes { data1Buffer -> Int32 in
			var data1Val = MDB_val(mv_size:data1Buffer.count, mv_data:data1Buffer.baseAddress)
			return data2Export.withUnsafeMutableBytes { data2Buffer -> Int32 in
				var data2Val = MDB_val(mv_size:data2Buffer.count, mv_data:data2Buffer.baseAddress)
				return mdb_cmp(self.tx_handle, self.db_handle, &data1Val, &data2Val)
			}
		}
	}
	
	
	//compare two values according to the value comparison function of the database
	public func compareValues<D:DataConvertible>(_ data1:D, _ data2:D) -> Int32 {
		var data1Export = data1.exportData()
		var data2Export = data2.exportData()
		return data1Export.withUnsafeMutableBytes { data1Buffer -> Int32 in
			var data1Val = MDB_val(mv_size:data1Buffer.count, mv_data:data1Buffer.baseAddress)
			return data2Export.withUnsafeMutableBytes { data2Buffer -> Int32 in
				var data2Val = MDB_val(mv_size:data2Buffer.count, mv_data:data2Buffer.baseAddress)
				return mdb_dcmp(self.tx_handle, self.db_handle, &data1Val, &data2Val)
			}
		}
	}
	
	
	//lmdb documentation suggests that read-only cursors always be closed. Therefore, Cursor is implemented as a class with this deinit block to automatically close the cursor on the users behalf
	deinit {
		if (self.readOnly) {
			mdb_cursor_close(self.handle)
		}
	}
}