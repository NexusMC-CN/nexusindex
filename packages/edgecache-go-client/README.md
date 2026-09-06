# edgecache-go-client

Small HTTP client for the EdgeCache service, scoped to `nexusindex`.

This package does not embed or import the EdgeCache runtime. It only talks to a running EdgeCache instance through HTTP, so `nexusindex` stays process-independent.

```go
client := edgecache.New(edgecache.Options{
	BaseURL: "http://127.0.0.1:4410",
})

result, err := client.Get(ctx, "some:key")
if err == nil && result.Hit {
	// result.Value is json.RawMessage
}
```
