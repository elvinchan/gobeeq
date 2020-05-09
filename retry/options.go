package retry

import "time"

type Strategy func(attempt uint) bool

func Limit(attemptLimit uint) Strategy {
	return func(attempt uint) bool {
		return (attempt <= attemptLimit)
	}
}

func Backoff(algorithm Algorithm) Strategy {
	return func(attempt uint) bool {
		if attempt > 0 {
			time.Sleep(algorithm(attempt))
		}
		return true
	}
}

type Algorithm func(attempt uint) time.Duration

func Incremental(initial, increment time.Duration) Algorithm {
	return func(attempt uint) time.Duration {
		return initial + (increment * time.Duration(attempt))
	}
}

func Linear(factor time.Duration) Algorithm {
	return func(attempt uint) time.Duration {
		return (factor * time.Duration(attempt))
	}
}

func Fibonacci(factor time.Duration) Algorithm {
	return func(attempt uint) time.Duration {
		return (factor * time.Duration(fibonacciNumber(attempt)))
	}
}

func fibonacciNumber(n uint) uint {
	if 0 == n {
		return 0
	} else if 1 == n {
		return 1
	} else {
		return fibonacciNumber(n-1) + fibonacciNumber(n-2)
	}
}
