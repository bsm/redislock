# redislock

[![Test](https://github.com/bsm/redislock/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/redislock/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/bsm/redislock?status.png)](http://godoc.org/github.com/bsm/redislock)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/redislock)](https://goreportcard.com/report/github.com/bsm/redislock)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Simplified distributed locking implementation using [Redis](http://redis.io/topics/distlock).
For more information, please see examples.

## Examples

```go
import (
  "fmt"
  "time"

  "github.com/bsm/redislock"
  "github.com/go-redis/redis/v8"
)

func main() {{ "Example" | code }}
```

## Documentation

Full documentation is available on [GoDoc](http://godoc.org/github.com/bsm/redislock)
