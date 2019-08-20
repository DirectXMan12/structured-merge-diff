package bin

import (
	"encoding/binary"
	"io"
)

type NumWriter struct {
	io.Writer
	lenBuff [binary.MaxVarintLen64]byte
}

func (n *NumWriter) WriteNum(num int) error {
	outLen := binary.PutUvarint(n.lenBuff[:], uint64(num))
	_, err := n.Write(n.lenBuff[:outLen])
	return err
}

func ReadNum(in io.ByteReader) (int, error) {
	out, err := binary.ReadUvarint(in)
	return int(out), err
}
