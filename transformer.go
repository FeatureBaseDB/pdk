package pdk

// TransformerFunc can be wrapped around a function to make it implement the
// Transformer interface. Similar to http.HandlerFunc.
type TransformerFunc func(*Entity) error

// Transform implements Transformer for TransformerFunc
func (t TransformerFunc) Transform(e *Entity) error {
	return t(e)
}
