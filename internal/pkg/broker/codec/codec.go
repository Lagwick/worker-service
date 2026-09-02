package codec

type Codec[T any] interface {
	Encode(v *T) ([]byte, error)
	Decode(data []byte) (*T, error)
}
