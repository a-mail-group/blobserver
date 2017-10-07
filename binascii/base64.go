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

import "encoding/base64"
import "encoding/base32"

type codec interface{
	Decode(dst, src []byte) (n int,err error)
	DecodedLen(n int) int
	Encode(dst, src []byte)
	EncodedLen(n int) int
}

func encode(i codec, data []byte, buffer []byte) []byte {
	el := i.EncodedLen(len(data))
	bl := len(buffer)
	bc := bl+el
	if cap(buffer)<bc { // Reallocate the buffer if needed.
		nb := make([]byte,bl,bc)
		copy(nb,buffer)
		buffer = nb
	}
	buffer = buffer[:bc] // Expand the used range.
	i.Encode(buffer[bl:],data)
	return buffer
}
func decode(i codec, encoded []byte, buffer []byte) ([]byte,error) {
	dl := i.DecodedLen(len(encoded))
	bl := len(buffer)
	bc := bl+dl
	if cap(buffer)<bc { // Reallocate the buffer if needed.
		nb := make([]byte,bl,bc)
		copy(nb,buffer)
		buffer = nb
	}
	buffer = buffer[:bc] // Expand the used range.
	rdl,err := i.Decode(buffer[bl:],encoded)
	if err!=nil { return nil,err }
	return buffer[:bl+rdl],nil
}

// URLEncoding
func EncodeBase64(data []byte, buffer []byte) []byte {
	return encode(base64.URLEncoding,data,buffer)
}
// URLEncoding
func DecodeBase64(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base64.URLEncoding,encoded,buffer)
}

// RawURLEncoding
func EncodeBase64Raw(data []byte, buffer []byte) []byte {
	return encode(base64.RawURLEncoding,data,buffer)
}
// RawURLEncoding
func DecodeBase64Raw(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base64.RawURLEncoding,encoded,buffer)
}

// StdEncoding
func EncodeBase64Std(data []byte, buffer []byte) []byte {
	return encode(base64.StdEncoding,data,buffer)
}
// StdEncoding
func DecodeBase64Std(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base64.StdEncoding,encoded,buffer)
}

// RawStdEncoding
func EncodeBase64RawStd(data []byte, buffer []byte) []byte {
	return encode(base64.RawStdEncoding,data,buffer)
}
// RawStdEncoding
func DecodeBase64RawStd(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base64.RawStdEncoding,encoded,buffer)
}

func EncodeBase32(data []byte, buffer []byte) []byte {
	return encode(base32.StdEncoding,data,buffer)
}
func DecodeBase32(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base32.StdEncoding,encoded,buffer)
}

// Extended Hex Alphabet
func EncodeBase32Hex(data []byte, buffer []byte) []byte {
	return encode(base32.HexEncoding,data,buffer)
}
// Extended Hex Alphabet
func DecodeBase32Hex(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(base32.HexEncoding,encoded,buffer)
}



var rawStd=base32.StdEncoding.WithPadding(base32.NoPadding)
var rawHex=base32.HexEncoding.WithPadding(base32.NoPadding)

func EncodeBase32Raw(data []byte, buffer []byte) []byte {
	return encode(rawStd,data,buffer)
}
func DecodeBase32Raw(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(rawStd,encoded,buffer)
}

// Extended Hex Alphabet
func EncodeBase32RawHex(data []byte, buffer []byte) []byte {
	return encode(rawHex,data,buffer)
}
// Extended Hex Alphabet
func DecodeBase32RawHex(encoded []byte, buffer []byte) ([]byte,error) {
	return decode(rawHex,encoded,buffer)
}

