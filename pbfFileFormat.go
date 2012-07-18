/*
   go-osmpbf-filter; filtering software for OpenStreetMap PBF files.
   Copyright (C) 2012  Mathieu Fenniak

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"OSMPBF"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var cacheUncompressedBlobs map[int64][]byte

type blockData struct {
	blobHeader   *OSMPBF.BlobHeader
	blobData     []byte
	filePosition int64
}

func readBlock(file io.Reader, size int32) ([]byte, error) {
	buffer := make([]byte, size)
	var idx int32 = 0
	for {
		cnt, err := file.Read(buffer[idx:])
		if err != nil {
			return nil, err
		}
		idx += int32(cnt)
		if idx == size {
			break
		}
	}
	return buffer, nil
}

func ReadNextBlobHeader(file *os.File) (*OSMPBF.BlobHeader, error) {
	var blobHeaderSize int32

	err := binary.Read(file, binary.BigEndian, &blobHeaderSize)
	if err != nil {
		return nil, err
	}

	if blobHeaderSize < 0 || blobHeaderSize > (64*1024*1024) {
		return nil, err
	}

	blobHeaderBytes, err := readBlock(file, blobHeaderSize)
	if err != nil {
		return nil, err
	}

	blobHeader := &OSMPBF.BlobHeader{}
	err = proto.Unmarshal(blobHeaderBytes, blobHeader)
	if err != nil {
		return nil, err
	}

	return blobHeader, nil
}

func DecodeBlob(data blockData) ([]byte, error) {
	var blobContent []byte

	if cacheUncompressedBlobs != nil {
		blobContent = cacheUncompressedBlobs[data.filePosition]
		if blobContent != nil {
			return blobContent, nil
		}
	}

	blob := &OSMPBF.Blob{}
	err := proto.Unmarshal(data.blobData, blob)
	if err != nil {
		return nil, err
	}

	if blob.Raw != nil {
		blobContent = blob.Raw
	} else if blob.ZlibData != nil {
		if blob.RawSize == nil {
			return nil, errors.New("decompressed size is required but not provided")
		}
		zlibBuffer := bytes.NewBuffer(blob.ZlibData)
		zlibReader, err := zlib.NewReader(zlibBuffer)
		if err != nil {
			return nil, err
		}
		blobContent, err = readBlock(zlibReader, *blob.RawSize)
		if err != nil {
			return nil, err
		}
		zlibReader.Close()
	} else {
		return nil, errors.New("Unsupported blob storage")
	}

	if cacheUncompressedBlobs != nil {
		cacheUncompressedBlobs[data.filePosition] = blobContent
	}

	return blobContent, nil
}

func MakePrimitiveBlockReader(file *os.File) <-chan blockData {
	retval := make(chan blockData)

	go func() {
		file.Seek(0, os.SEEK_SET)
		for {
			filePosition, err := file.Seek(0, os.SEEK_CUR)

			blobHeader, err := ReadNextBlobHeader(file)
			if err == io.EOF {
				break
			} else if err != nil {
				println("Blob header read error:", err.Error())
				os.Exit(2)
			}

			blobBytes, err := readBlock(file, *blobHeader.Datasize)
			if err != nil {
				println("Blob read error:", err.Error())
				os.Exit(3)
			}

			retval <- blockData{blobHeader, blobBytes, filePosition}
		}
		close(retval)
	}()

	return retval
}

func WriteBlock(file *os.File, block proto.Message, blockType string) error {
	blobContent, err := proto.Marshal(block)
	if err != nil {
		return err
	}

	var blobContentLength int32 = int32(len(blobContent))

	var compressedBlob bytes.Buffer
	zlibWriter := zlib.NewWriter(&compressedBlob)
	zlibWriter.Write(blobContent)
	zlibWriter.Close()

	blob := OSMPBF.Blob{}
	blob.ZlibData = compressedBlob.Bytes()
	blob.RawSize = &blobContentLength
	blobBytes, err := proto.Marshal(&blob)
	if err != nil {
		return err
	}

	var blobBytesLength int32 = int32(len(blobBytes))

	blobHeader := OSMPBF.BlobHeader{}
	blobHeader.Type = &blockType
	blobHeader.Datasize = &blobBytesLength
	blobHeaderBytes, err := proto.Marshal(&blobHeader)
	if err != nil {
		return err
	}

	var blobHeaderLength int32 = int32(len(blobHeaderBytes))

	err = binary.Write(file, binary.BigEndian, blobHeaderLength)
	if err != nil {
		return err
	}
	_, err = file.Write(blobHeaderBytes)
	if err != nil {
		return err
	}
	_, err = file.Write(blobBytes)
	if err != nil {
		return err
	}

	return nil
}

func WriteHeader(file *os.File) error {
	writingProgram := "go-osmpbf-filter"
	header := OSMPBF.HeaderBlock{}
	header.Writingprogram = &writingProgram
	header.RequiredFeatures = []string{"OsmSchema-V0.6"}
	return WriteBlock(file, &header, "OSMHeader")
}
