package httpadapter

// testDeps returns a Deps with nil stores — sufficient for health-endpoint
// tests that exercise /healthz and /readyz (no store required).
func testDeps() Deps { return Deps{} }
