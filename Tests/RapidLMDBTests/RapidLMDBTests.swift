import XCTest
@testable import RapidLMDB

final class RapidLMDBTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(RapidLMDB().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
