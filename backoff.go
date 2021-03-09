package gobeeq

type BackoffStrategy string

const (
	BackoffImmediate   BackoffStrategy = "immediate"
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

type Backoff struct {
	Strategy BackoffStrategy `json:"strategy"`
	Delay    int64           `json:"delay"`
}

func (b *Backoff) cal() int64 {
	switch b.Strategy {
	case BackoffImmediate:
		return 0
	case BackoffFixed:
		return b.Delay
	case BackoffExponential:
		v := b.Delay
		b.Delay *= 2
		return v
	}
	return 0
}
