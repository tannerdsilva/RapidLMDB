import CLMDB
import Foundation

public enum LMDBError: Error {

    // LMDB defined errors.
    case keyExists
    case notFound
    case pageNotFound
    case corrupted
    case panic
    case versionMismatch
    case invalid
    case mapFull
    case dbsFull
    case readersFull
    case tlsFull
    case txnFull
    case cursorFull
    case pageFull
    case mapResized
    case incompatible
    case badReaderSlot
    case badTransaction
    case badValueSize
    case badDBI
    case problem

    // OS errors
    case invalidParameter
    case outOfDiskSpace
    case outOfMemory
    case ioError
    case accessViolation
    
    case other(returnCode: Int32)
    
    init(returnCode: Int32) {
        switch returnCode {
			case MDB_KEYEXIST: self = .keyExists
			case MDB_NOTFOUND: self = .notFound
			case MDB_PAGE_NOTFOUND: self = .pageNotFound
			case MDB_CORRUPTED: self = .corrupted
			case MDB_PANIC: self = .panic
			case MDB_VERSION_MISMATCH: self = .versionMismatch
			case MDB_INVALID: self = .invalid
			case MDB_MAP_FULL: self = .mapFull
			case MDB_DBS_FULL: self = .dbsFull
			case MDB_READERS_FULL: self = .readersFull
			case MDB_TLS_FULL: self = .tlsFull
			case MDB_TXN_FULL: self = .txnFull
			case MDB_CURSOR_FULL: self = .cursorFull
			case MDB_PAGE_FULL:  self = .pageFull
			case MDB_MAP_RESIZED: self = .mapResized
			case MDB_INCOMPATIBLE: self = .incompatible
			case MDB_BAD_RSLOT: self = .badReaderSlot
			case MDB_BAD_TXN: self = .badTransaction
			case MDB_BAD_VALSIZE: self = .badValueSize
			case MDB_BAD_DBI: self = .badDBI
		
			case EINVAL: self = .invalidParameter
			case ENOSPC: self = .outOfDiskSpace
			case ENOMEM: self = .outOfMemory
			case EIO: self = .ioError
			case EACCES: self = .accessViolation
			
			default: self = .other(returnCode: returnCode)
        }
    }
}