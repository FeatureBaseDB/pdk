package fake

// Source is a pdk.Source which generates fake Event data.
type Source struct {
	g *EventGenerator
}

// NewSource creates a new Source with the given random seed. Using the same
// seed should give the same series of events on a given version of Go.
func NewSource(seed int64) *Source {
	return &Source{
		g: NewEventGenerator(seed),
	}
}

// Record implements pdk.Source and returns a randomly generated fake.Event.
func (s *Source) Record() (interface{}, error) {
	return s.g.Event(), nil
}
