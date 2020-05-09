package retry

type Action func(attempt uint) error

func Do(action Action, strategies ...Strategy) error {
	var err error
	for attempt := uint(0); (attempt == 0 || err != nil) && beforeAttempt(attempt, strategies...); attempt++ {
		err = action(attempt)
	}
	return err
}

func beforeAttempt(attempt uint, strategies ...Strategy) bool {
	for i := range strategies {
		if !strategies[i](attempt) {
			return false
		}
	}
	return true
}
