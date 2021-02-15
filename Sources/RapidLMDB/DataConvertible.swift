import Foundation

fileprivate let sharedDecoder = JSONDecoder()
fileprivate let sharedEncoder = JSONEncoder()

public protocol DataConvertible {
	init?(data:Data)
	func exportData() -> Data
}

extension Dictionary:DataConvertible where Key:Codable, Value:Codable {
	public init?(data:Data) {
		do {
			self = try sharedDecoder.decode(Self.self, from:data)
		} catch _ {
			return nil
		}
	}
	public func exportData() -> Data {
		return try! sharedEncoder.encode(self)
	}
}

extension Array:DataConvertible where Element:Codable {
	public init?(data:Data) {
		do {
			self = try sharedDecoder.decode(Self.self, from:data)
		} catch _ {
			return nil
		}
	}
	public func exportData() -> Data {
		return try! sharedEncoder.encode(self)
	}
}

extension Set:DataConvertible where Element:Codable {
	public init?(data:Data) {
		do {
			self = Set(try sharedDecoder.decode(Array<Element>.self, from:data))
		} catch _ {
			return nil
		}
	}
	public func exportData() -> Data {
		return try! sharedEncoder.encode(Array(self))
	}
}

extension Data:DataConvertible {
	public init?(data:Data) {
		self = data
	}
	public func exportData() -> Data {
		return self
	}
}

extension String:DataConvertible {
	public init?(data:Data) {
		self.init(data:data, encoding:.utf8)
	}
	public func exportData() -> Data {
		return self.data(using:.utf8)!
	}
}

extension Bool:DataConvertible {
	public init?(data:Data) {
		guard let intVal = UInt8(data:data) else { return nil }
		self = (intVal != 0)
	}
	public func exportData() -> Data {
		let value:UInt8 = self ? 1 : 0
		return value.exportData()
	}
}

extension FixedWidthInteger where Self:DataConvertible {
	public init?(data:Data) {
		guard data.count == MemoryLayout<Self>.size else { return nil }
		let littleEndian = data.withUnsafeBytes { $0.load(as:Self.self) }
		self = .init(littleEndian:littleEndian)
	}
	public func exportData() -> Data {
		var littleEndian = self.littleEndian
		return Data(bytes:&littleEndian, count:MemoryLayout<Self>.size)
	}
}

extension Int:DataConvertible {}
extension Int8:DataConvertible {}
extension Int16:DataConvertible {}
extension Int32:DataConvertible {}
extension Int64:DataConvertible {}

extension UInt:DataConvertible {}
extension UInt8:DataConvertible {}
extension UInt16:DataConvertible {}
extension UInt32:DataConvertible {}
extension UInt64:DataConvertible {}

extension Float:DataConvertible {
	public init?(data:Data) {
		guard data.count == MemoryLayout<UInt32>.size else { return nil }
		let littleEndian = data.withUnsafeBytes { $0.load(as:UInt32.self) }
		let bitPattern = UInt32(littleEndian:littleEndian)
		self = .init(bitPattern:bitPattern)
	}
	public func exportData() -> Data {
		return bitPattern.littleEndian.exportData()
	}
}

extension Double:DataConvertible {
	public init?(data:Data) {
		guard data.count == MemoryLayout<UInt64>.size else { return nil }
		let littleEndian = data.withUnsafeBytes { $0.load(as:UInt64.self) }
		let bitPattern = UInt64(littleEndian:littleEndian)
		self = .init(bitPattern:bitPattern)
	}
	public func exportData() -> Data {
		return bitPattern.littleEndian.exportData()
	}
}

extension Date:DataConvertible {
	public init?(data:Data) {
		guard let timeInterval = TimeInterval(data:data) else { return nil }
		self = Date(timeIntervalSinceReferenceDate:timeInterval)
	}
	public func exportData() -> Data {
		return timeIntervalSinceReferenceDate.exportData()
	}
}