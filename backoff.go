package gobeeq

type (
	BackoffStrategy interface {
		Delay() int64
	}

	BackoffImmediate struct{}
	BackoffFixed     struct {
		Fixed int64
	}
	BackoffExponential struct {
		Next int64
	}
)

func (BackoffImmediate) Delay() int64 {
	return 0
}

func (b BackoffFixed) Delay() int64 {
	return b.Fixed
}

func (b BackoffExponential) Delay() int64 {
	v := b.Next
	b.Next *= 2
	return v
}
