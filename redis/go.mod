module github.com/ninepeach/go-ratelimit/redis

go 1.25.1

require (
	github.com/ninepeach/go-ratelimit v0.0.0
	github.com/redis/go-redis/v9 v9.17.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

// Until the root module is tagged, build against the checkout. Replace this
// with the released version before tagging redis/vX.
replace github.com/ninepeach/go-ratelimit => ../
