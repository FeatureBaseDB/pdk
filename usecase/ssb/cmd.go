package ssb

type Main struct {
	Dir   string
	Hosts []string
}

func NewMain() *Main {
	return &Main{}
}

func (m *Main) Run() error {
	return nil
}
