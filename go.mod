module github.com/slidebolt/plugin-framework

go 1.24.0

// replace plugin-sdk => ../plugin-sdk

require (
	github.com/nats-io/nats.go v1.48.0
	github.com/slidebolt/plugin-sdk v0.0.0-20260222172329-b5df52b61282
	github.com/yuin/gopher-lua v1.1.1
// github.com/slidebolt/plugin-sdk v0.0.0
)

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
)
