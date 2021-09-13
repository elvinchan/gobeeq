English | [简体中文](./README_zh-CN.md)

# Gobeeq
Golang implementation of [Bee-Queue](https://github.com/bee-queue/bee-queue). A simple, fast, robust job/task queue backed by Redis.

[![Ci](https://github.com/elvinchan/gobeeq/actions/workflows/ci.yml/badge.svg)](https://github.com/elvinchan/gobeeq/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/elvinchan/gobeeq/branch/master/graph/badge.svg)](https://codecov.io/gh/elvinchan/gobeeq)
[![Go Report Card](https://goreportcard.com/badge/github.com/elvinchan/gobeeq)](https://goreportcard.com/report/github.com/elvinchan/gobeeq)
[![Go Reference](https://pkg.go.dev/badge/github.com/elvinchan/gobeeq.svg)](https://pkg.go.dev/github.com/elvinchan/gobeeq)
[![MPLv2 License](https://img.shields.io/badge/license-MPLv2-blue.svg)](https://www.mozilla.org/MPL/2.0/)

## Prerequisites
- **[Go](https://golang.org/)**: 1.13 and above.

# Todo
- Benchmark test

# Notice
- For compatible with the original **Bee-Queue**, all integer type of time/duration is millisecond format.
- For more robust and efficiency scripts execution, there's no ensure scripts process, but use `Run()` of `github.com/go-redis/redis`, which optimistically uses `EVALSHA` to run the script, if the script does not exist it is retried using `EVAL`.
- Since events is not associated with specific `Job` instances, this implementation do not provide `Job` store like the original **Bee-Queue**.

## License

[MIT](https://github.com/elvinchan/gobeeq/blob/master/LICENSE)