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


package storage

import "github.com/lytics/confl"
import "path/filepath"
import "github.com/tideland/golib/identifier"
import "fmt"
import "os"

func parseUUID(suuid string) (uuid identifier.UUID,err error) {
	var A uint32
	var B,C,D uint16
	var E uint64
	_,err = fmt.Sscanf(suuid,"%8x-%4x-%4x-%4x-%12x", &A, &B, &C, &D, &E)
	uuid[0] = byte(A>>24)
	uuid[1] = byte(A>>16)
	uuid[2] = byte(A>>8)
	uuid[3] = byte(A)
	//-
	uuid[4] = byte(B>>8)
	uuid[5] = byte(B)
	//-
	uuid[6] = byte(C>>8)
	uuid[7] = byte(C)
	//-
	uuid[8] = byte(D>>8)
	uuid[9] = byte(D)
	//-
	uuid[10] = byte(E>>40)
	uuid[11] = byte(E>>32)
	uuid[12] = byte(E>>24)
	uuid[13] = byte(E>>16)
	uuid[14] = byte(E>>8)
	uuid[15] = byte(E)
	return
}

type backendIdentifier struct{
	Uuid string `confl:"uuid"`
}

func GetOrCreateUUID(path string) (identifier.UUID,error){
	bi := new(backendIdentifier)
	cf := filepath.Join(path,"id.conf")
	{
		f,e := os.Open(cf)
		if e!=nil { goto otherwise } // No file.
		defer f.Close()
		e = confl.NewDecoder(f).Decode(bi)
		if e!=nil { goto otherwise } // unreadable.
		id,e := parseUUID(bi.Uuid)
		if e!=nil { goto otherwise }
		return id,nil
	}
	otherwise:
	{
		id,e := identifier.NewUUIDv4()
		if e!=nil { return id,e }
		f,e := os.Create(cf)
		if e!=nil { return id,e }
		defer f.Close()
		bi.Uuid = id.String()
		e = confl.NewEncoder(f).Encode(bi)
		return id,e
	}
}
