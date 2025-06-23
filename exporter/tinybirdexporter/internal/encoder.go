package internal

type Encoder interface {
	Encode(v any) error
}
