/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package binascii


import "encoding/ascii85"

func a85DecodedLength(data []byte) int {
	n := 0
	for _,b := range data {
		switch {
		case b=='z': n+=5
		case '!'<=b && b <= 'u': n++
		}
	}
	l := ((n+4)/5)*4
	return l
}

func EncodeAscii85(data []byte, buffer []byte) []byte {
	el := ascii85.MaxEncodedLen(len(data))
	bl := len(buffer)
	bc := bl+el
	if cap(buffer)<bc { // Reallocate the buffer if needed.
		nb := make([]byte,bl,bc)
		copy(nb,buffer)
		buffer = nb
	}
	buffer = buffer[:bc] // Expand the used range.
	rel := ascii85.Encode(buffer[bl:],data)
	return buffer[:bl+rel]
}
func DecodeAscii85(encoded []byte, buffer []byte) ([]byte,error) {
	dl := a85DecodedLength(encoded)
	bl := len(buffer)
	bc := bl+dl
	if cap(buffer)<bc { // Reallocate the buffer if needed.
		nb := make([]byte,bl,bc)
		copy(nb,buffer)
		buffer = nb
	}
	buffer = buffer[:bc] // Expand the used range.
	rdl,_,err := ascii85.Decode(buffer[bl:],encoded,true)
	if err!=nil { return nil,err }
	return buffer[:bl+rdl],nil
}


