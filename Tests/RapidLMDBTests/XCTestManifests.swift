import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(RapidLMDBTests.allTests),
    ]
}
#endif
