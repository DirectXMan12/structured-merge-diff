/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fieldpath

import (
	"bytes"
	"io"
	"unsafe"
	"errors"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"sigs.k8s.io/structured-merge-diff/fieldpath/bin"
	"sigs.k8s.io/structured-merge-diff/value"
)

func (s *Set) ToJSON() ([]byte, error) {
	buf := bytes.Buffer{}
	err := s.ToJSONStream(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Set) ToJSONStream(w io.Writer) error {
	stream := writePool.BorrowStream(w)
	defer writePool.ReturnStream(stream)

	var r reusableBuilder

	stream.WriteObjectStart()
	err := s.emitContents_v1(false, stream, &r)
	if err != nil {
		return err
	}
	stream.WriteObjectEnd()
	return stream.Flush()
}

func manageMemory(stream *jsoniter.Stream) error {
	// Help jsoniter manage its buffers--without this, it does a bunch of
	// alloctaions that are not necessary. They were probably optimizing
	// for folks using the buffer directly.
	b := stream.Buffer()
	if cap(b) < 4*1024 {
		b2 := make([]byte, len(b), 5*1024)
		copy(b2, b)
		stream.SetBuffer(b2)
		b = b2
	}
	if len(b) > 4*1024 || cap(b)-len(b) < 1024 {
		if err := stream.Flush(); err != nil {
			return err
		}
		stream.SetBuffer(b[:0])
	}
	return nil
}

type reusableBuilder struct {
	bytes.Buffer
}

func (r *reusableBuilder) unsafeString() string {
	b := r.Bytes()
	return *(*string)(unsafe.Pointer(&b))
}

func (r *reusableBuilder) reset() *bytes.Buffer {
	r.Reset()
	return &r.Buffer
}

func (s *Set) emitContents_v1(includeSelf bool, stream *jsoniter.Stream, r *reusableBuilder) error {
	mi, ci := 0, 0
	first := true
	preWrite := func() {
		if first {
			first = false
			return
		}
		stream.WriteRaw(",")
		// WriteMore flushes, which we don't want since we manage our own flushing.
		// stream.WriteMore()
	}

	for mi < len(s.Members.members) && ci < len(s.Children.members) {
		mpe := s.Members.members[mi]
		cpe := s.Children.members[ci].pathElement

		if mpe.Less(cpe) {
			preWrite()
			if err := serializePathElementToWriter(r.reset(), mpe); err != nil {
				return err
			}
			stream.WriteObjectField(r.unsafeString())
			stream.WriteEmptyObject()
			mi++
		} else if cpe.Less(mpe) {
			preWrite()
			if err := serializePathElementToWriter(r.reset(), cpe); err != nil {
				return err
			}
			stream.WriteObjectField(r.unsafeString())
			stream.WriteObjectStart()
			if err := s.Children.members[ci].set.emitContents_v1(false, stream, r); err != nil {
				return err
			}
			stream.WriteObjectEnd()
			ci++
		} else {
			preWrite()
			if err := serializePathElementToWriter(r.reset(), cpe); err != nil {
				return err
			}
			stream.WriteObjectField(r.unsafeString())
			stream.WriteObjectStart()
			if err := s.Children.members[ci].set.emitContents_v1(true, stream, r); err != nil {
				return err
			}
			stream.WriteObjectEnd()
			mi++
			ci++
		}
	}

	for mi < len(s.Members.members) {
		mpe := s.Members.members[mi]

		preWrite()
		if err := serializePathElementToWriter(r.reset(), mpe); err != nil {
			return err
		}
		stream.WriteObjectField(r.unsafeString())
		stream.WriteEmptyObject()
		mi++
	}

	for ci < len(s.Children.members) {
		cpe := s.Children.members[ci].pathElement

		preWrite()
		if err := serializePathElementToWriter(r.reset(), cpe); err != nil {
			return err
		}
		stream.WriteObjectField(r.unsafeString())
		stream.WriteObjectStart()
		if err := s.Children.members[ci].set.emitContents_v1(false, stream, r); err != nil {
			return err
		}
		stream.WriteObjectEnd()
		ci++
	}

	if includeSelf && !first {
		preWrite()
		stream.WriteObjectField(".")
		stream.WriteEmptyObject()
	}
	return manageMemory(stream)
}

type stringTable struct {
	table map[string]int
	next int
	total int

	buff strings.Builder
}
func (s *stringTable) intern(str string) int {
	if index, ok := s.table[str]; ok {
		return index
	}

	val := s.next
	s.table[str] = val
	s.total+=len(str)
	s.next++
	return val
}

func serializePathElem_v2(includeSelf bool, totalChildren int, selfElem *PathElement, table *stringTable, out *bin.NumWriter) error {
	ownsBit := 0
	if includeSelf {
		ownsBit = 1
	}

	formatBits := 0
	if selfElem != nil {
		switch {
		case selfElem.FieldName != nil:
			formatBits = 0
		case selfElem.Key != nil:
			formatBits = 1
		case selfElem.Value != nil:
			formatBits = 2
		case selfElem.Index != nil:
			formatBits = 3
		default:
			return errors.New("invalid PathElement")
		}
	}

	headerKey := (totalChildren << 3) | (formatBits << 1) | ownsBit
	if err := out.WriteNum(headerKey); err != nil {
		return err
	}


	if selfElem != nil {
		switch formatBits {
		case 0:
			if err := out.WriteNum(table.intern(*selfElem.FieldName)); err != nil {
				return err
			}
		case 1:
			if err := out.WriteNum(len(selfElem.Key.Items)); err != nil {
				return err
			}
			for _, item := range selfElem.Key.Items {
				if err := out.WriteNum(table.intern(item.Name)); err != nil {
					return err
				}

				switch {
				case item.Value.Null:
					if err := out.WriteNum(1 << 1); err != nil {
						return err
					}
				case item.Value.IntValue != nil:
					if err := out.WriteNum(2 << 1); err != nil {
						return err
					}
					if err := out.WriteNum(int(*item.Value.IntValue)); err != nil {
						return err
					}
				case item.Value.BooleanValue != nil:
					if *item.Value.BooleanValue {
						if err := out.WriteNum(3 << 1); err != nil {
							return err
						}
					} else {
						if err := out.WriteNum(4 << 1); err != nil {
							return err
						}
					}
				case item.Value.StringValue != nil:
					num := table.intern(string(*item.Value.StringValue))
					num = (num << 1) | 1
					if err := out.WriteNum(num); err != nil {
						return nil
					}
				default:
					return errors.New("unsupported key value format")
				}
			}
		case 2:
			// just give up for now :-/
			builderBuf := &table.buff
			builderBuf.Reset()
			var err error
			func() {
				stream := writePool.BorrowStream(builderBuf)
				defer writePool.ReturnStream(stream)
				selfElem.Value.WriteJSONStream(stream)

				b := stream.Buffer()
				err = stream.Flush()
				stream.SetBuffer(b[:0])
			}()
			if err != nil {
				return err
			}

			if err := out.WriteNum(table.intern(builderBuf.String())); err != nil {
				return err
			}
		case 3:
			if err := out.WriteNum(*selfElem.Index); err != nil {
				return err
			}
		default:
			return errors.New("invalid PathElement")
		}
	} else {
		if err := out.WriteNum(table.intern("")); err != nil {
			return nil
		}
	}
	return nil
}

func (s *Set) ToBinary_V2Experimental() ([]byte, []byte, error) {
	buf := bytes.Buffer{}
	table := &stringTable{table: make(map[string]int)}
	err := s.emitContents_v2(false, nil, table, &bin.NumWriter{Writer: &buf})
	if err != nil {
		return nil, nil, err
	}
	tableSorted := make([]string, len(table.table))
	for str, i := range table.table {
		tableSorted[i] = str
	}

	tableOut := bytes.NewBuffer(make([]byte, table.total+(len(table.table)+1)*3))
	tableOut.Reset()
	writer := bin.NumWriter{Writer: tableOut}

	if err := writer.WriteNum(len(table.table)); err != nil {
		return nil, nil, err
	}
	
	for _, str := range tableSorted {
		if err := writer.WriteNum(len(str)); err != nil {
			return nil, nil, err
		}
		if _, err := writer.Write(*(*[]byte)(unsafe.Pointer(&str))); err != nil {
			return nil, nil, err
		}
	}

	return buf.Bytes(), tableOut.Bytes(), nil
}

func (s *Set) emitContents_v2(includeSelf bool, selfElem *PathElement, table *stringTable, out *bin.NumWriter) error {
	totalChildren := len(s.Members.members) + len(s.Children.members)
	if err := serializePathElem_v2(includeSelf, totalChildren, selfElem, table, out); err != nil {
		return err
	}

	memberInd, childInd := 0, 0

	for memberInd < len(s.Members.members) && childInd < len(s.Children.members) {
		memberElem := s.Members.members[memberInd]
		childElem := &s.Children.members[childInd].pathElement

		switch {
		case memberElem.Less(*childElem):
			if err := serializePathElem_v2(false, 0, &memberElem, table, out); err != nil {
				return err
			}
			memberInd++
		case childElem.Less(memberElem):
			if err := s.Children.members[childInd].set.emitContents_v2(false, childElem, table, out); err != nil {
				return err
			}
			childInd++
		default:
			if err := s.Children.members[childInd].set.emitContents_v2(true, childElem, table, out); err != nil {
				return err
			}
			memberInd++
			childInd++
		}
	}

	for memberInd < len(s.Members.members) {
		memberElem := s.Members.members[memberInd]
		if err := serializePathElem_v2(false, 0, &memberElem, table, out); err != nil {
			return err
		}
		memberInd++
	}

	for childInd < len(s.Children.members) {
		childElem := &s.Children.members[childInd].pathElement
		if err := s.Children.members[childInd].set.emitContents_v2(false, childElem, table, out); err != nil {
			return err
		}
		childInd++
	}

	return nil
}

func (s *Set) FromBinary_V2Experimental(r io.Reader, tableRaw []byte) error {
	tableRawIn := bytes.NewBuffer(tableRaw)
	tableLen, err := bin.ReadNum(tableRawIn)
	if err != nil {
		return err
	}

	table := make([]string, tableLen)
	for i := range table {
		strLen, err := bin.ReadNum(tableRawIn)
		if err != nil {
			return err
		}
		table[i] = string(tableRawIn.Next(strLen)) // go copies on string casts
	}

	_, contents, _, err := readBin_v2(r.(io.ByteReader), table)
	if err != nil {
		return err
	}
	*s = *contents
	return nil
}

func readBin_v2(r io.ByteReader, table []string) (key PathElement, children *Set, isMember bool, err error) {
	headerKey, err := bin.ReadNum(r)
	if err != nil { return PathElement{}, nil, false, err }

	numChildren := headerKey >> 3
	form := (headerKey >> 1) & 3
	includeSelf := headerKey & 1 != 0

	switch form {
	case 0:
		name, err := readStr(r, table)
		if err != nil { return PathElement{}, nil, false, err }
		key.FieldName = name
	case 1:
		numVals, err := bin.ReadNum(r)
		if err != nil { return PathElement{}, nil, false, err }

		vals := make([]value.Field, numVals)
		for i := range vals {
			name, err := readStr(r, table)
			if err != nil { return PathElement{}, nil, false, err }
			valRaw, err := bin.ReadNum(r)
			if err != nil { return PathElement{}, nil, false, err }

			var outVal value.Value
			if valRaw & 1 != 0 {
				strNum := valRaw >> 1
				valStr, err := indToStr(strNum, table)
				if err != nil { return PathElement{}, nil, false, err }
				outVal = value.StringValue(*valStr)
			} else {
				switch valRaw {
				case 1 << 1:
					outVal.Null = true
				case 2 << 1:
					num, err := bin.ReadNum(r)
					if err != nil { return PathElement{}, nil, false, err }
					outVal = value.IntValue(num)
				case 3 << 1:
					outVal = value.BooleanValue(true)
				case 4 << 1:
					outVal = value.BooleanValue(false)
				default:
					return PathElement{}, nil, false, errors.New("read unsupported key value format")
				}
			}

			vals[i] = value.Field{Name: *name, Value: outVal}
		}
		key.Key = &value.Map{Items: vals}
	case 2:
		contents, err := readStr(r, table)
		if err != nil { return PathElement{}, nil, false, err }
		if err := func() error {
			iter := readPool.BorrowIterator([]byte(*contents))
			defer readPool.ReturnIterator(iter)
			v, err := value.ReadJSONIter(iter)
			if err != nil {
				return err
			}
			key.Value = &v
			return nil
		}(); err != nil { return PathElement{}, nil, false, err }
	case 3:
		ind, err := bin.ReadNum(r)
		if err != nil { return PathElement{}, nil, false, err }
		key.Index = &ind
	}

	if numChildren == 0 {
		return key, children, includeSelf, nil
	}

	for i := 0; i < numChildren; i++ {
		childKey, grandChildren, isMember, err := readBin_v2(r, table)
		if err != nil { return PathElement{}, nil, false, err }

		if isMember || grandChildren == nil {
			if children == nil {
				children = &Set{}
			}
			m := &children.Members.members
			appendOk := len(*m) == 0 || (*m)[len(*m)-1].Less(childKey)
			if appendOk {
				*m = append(*m, childKey)
			} else {
				children.Members.Insert(childKey)
			}

			if isMember {
				numChildren--
			}
		}

		if grandChildren != nil {
			if children == nil {
				children = &Set{}
			}
			m := &children.Children.members
			appendOK := len(*m) == 0 || (*m)[len(*m)-1].pathElement.Less(childKey)
			if appendOK {
				*m = append(*m, setNode{childKey, grandChildren})
			} else {
				*children.Children.Descend(childKey) = *grandChildren
			}
		}
	}

	return key, children, includeSelf, nil
}

func indToStr(strKey int, table []string) (*string, error) {
	if int(strKey) >= len(table) {
		return nil, errors.New("invalid table key")
	}
	return &table[strKey], nil
}

func readStr(r io.ByteReader, table []string) (*string, error) {
	strKey, err := bin.ReadNum(r)
	if err != nil { return nil, err }

	return indToStr(strKey, table)
}

// FromJSON clears s and reads a JSON formatted set structure.
func (s *Set) FromJSON(r io.Reader) error {
	// The iterator pool is completely useless for memory management, grrr.
	iter := jsoniter.Parse(jsoniter.ConfigCompatibleWithStandardLibrary, r, 4096)

	found, _ := readIter_v1(iter)
	if found == nil {
		*s = Set{}
	} else {
		*s = *found
	}
	return iter.Error
}

// returns true if this subtree is also (or only) a member of parent; s is nil
// if there are no further children.
func readIter_v1(iter *jsoniter.Iterator) (children *Set, isMember bool) {
	iter.ReadMapCB(func(iter *jsoniter.Iterator, key string) bool {
		if key == "." {
			isMember = true
			iter.Skip()
			return true
		}
		pe, err := DeserializePathElement(key)
		if err == ErrUnknownPathElementType {
			// Ignore these-- a future version maybe knows what
			// they are. We drop these completely rather than try
			// to preserve things we don't understand.
			iter.Skip()
			return true
		} else if err != nil {
			iter.ReportError("parsing key as path element", err.Error())
			iter.Skip()
			return true
		}
		grandchildren, childIsMember := readIter_v1(iter)
		if childIsMember {
			if children == nil {
				children = &Set{}
			}
			m := &children.Members.members
			// Since we expect that most of the time these will have been
			// serialized in the right order, we just verify that and append.
			appendOK := len(*m) == 0 || (*m)[len(*m)-1].Less(pe)
			if appendOK {
				*m = append(*m, pe)
			} else {
				children.Members.Insert(pe)
			}
		}
		if grandchildren != nil {
			if children == nil {
				children = &Set{}
			}
			// Since we expect that most of the time these will have been
			// serialized in the right order, we just verify that and append.
			m := &children.Children.members
			appendOK := len(*m) == 0 || (*m)[len(*m)-1].pathElement.Less(pe)
			if appendOK {
				*m = append(*m, setNode{pe, grandchildren})
			} else {
				*children.Children.Descend(pe) = *grandchildren
			}
		}
		return true
	})
	if children == nil {
		isMember = true
	}

	return children, isMember
}
