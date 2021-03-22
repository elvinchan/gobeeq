# Gobeeq
Golang implementation of [Bee-Queue](https://github.com/bee-queue/bee-queue). A simple, fast, robust job/task queue backed by Redis.

# Todo
- Job store
- Benchmark test

# Notice
- For compatible with the original Bee-Queue, all integer type of time/duration is millisecond format.
- For more robust and efficiency scripts execution, there's no ensure scripts process, but use `Run()` of github.com/go-redis/redis, which optimistically uses EVALSHA to run the script, if script does not exist it is retried using EVAL.