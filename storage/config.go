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

import "github.com/maxymania/blobserver/istorage"
import "github.com/lytics/confl"
import "io/ioutil"
import "path/filepath"
import "fmt"


type Size struct{
	K,M,G,T,P uint16
}
func (s *Size) Int64() int64 {
	if s==nil { return 0 }
	i := int64(0)
	i += int64(s.K)<<10
	i += int64(s.M)<<20
	i += int64(s.G)<<30
	i += int64(s.T)<<40
	i += int64(s.P)<<50
	return i
}

type StorageConfig struct {
	Method    string   `confl:"method"`
	Capacity  *Size    `confl:"capacity"`
	
	Options   []string `confl:"options"`
	
	// File-Based special
	MaxOpenFiles int   `confl:"max_open"`
}

type BackendLoader func(path string, cfg *StorageConfig) (string,istorage.Storage,error)

var  Backends = make(map[string]BackendLoader)

func LoadStorage(file string) (map[string]istorage.Storage,error) {
	cfg := make(map[string]*StorageConfig)
	store,err := ioutil.ReadFile(filepath.Join(file,"storage.conf"))
	if err!=nil { return nil,err }
	err = confl.Unmarshal(store, cfg)
	if err!=nil { return nil,err }
	
	nm := make(map[string]istorage.Storage)
	for _,v := range cfg {
		_,ok := Backends[v.Method]
		if !ok { return nil,fmt.Errorf("No such method: %q",v.Method) }
	}
	for k,v := range cfg {
		key,iss,err := Backends[v.Method](k,v)
		if err!=nil { return nil,err }
		nm[key] = iss
	}
	return nm,nil
}


