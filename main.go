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
	"flag"
	"github.com/golang/protobuf/proto"
	"github.com/mfenniak/go-osmpbf-filter/src/OSMPBF"
	"io"
	"math"
	"os"
	"runtime"
)

type boundingBoxUpdate struct {
	wayIndex int
	lon      float64
	lat      float64
}

type node struct {
	id     int64
	lon    float64
	lat    float64
	keys   []string
	values []string
}

type way struct {
	id      int64
	nodeIds []int64
	keys    []string
	values  []string
}

func supportedFilePass(file *os.File) {
	for data := range MakePrimitiveBlockReader(file) {
		if *data.blobHeader.Type == "OSMHeader" {
			blockBytes, err := DecodeBlob(data)
			if err != nil {
				println("OSMHeader blob read error:", err.Error())
				os.Exit(5)
			}
			header := &OSMPBF.HeaderBlock{}
			err = proto.Unmarshal(blockBytes, header)
			if err != nil {
				println("OSMHeader decode error:", err.Error())
				os.Exit(5)
			}

			for _, feat := range header.RequiredFeatures {
				if feat != "OsmSchema-V0.6" && feat != "DenseNodes" {
					println("Unsupported feature required in OSM header:", feat)
					os.Exit(5)
				}
			}
		}
	}
}

func findMatchingWaysPass(file *os.File, filterTag string, filterValue string, totalBlobCount int) [][]int64 {
	wayNodeRefs := make([][]int64, 0, 100)
	pending := make(chan bool)

	appendNodeRefs := make(chan []int64)
	appendNodeRefsComplete := make(chan bool)

	go func() {
		for nodeRefs := range appendNodeRefs {
			wayNodeRefs = append(wayNodeRefs, nodeRefs)
		}
		appendNodeRefsComplete <- true
	}()

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for _, primitiveGroup := range primitiveBlock.Primitivegroup {
						for _, way := range primitiveGroup.Ways {
							for i, keyIndex := range way.Keys {
								valueIndex := way.Vals[i]
								key := string(primitiveBlock.Stringtable.S[keyIndex])
								value := string(primitiveBlock.Stringtable.S[valueIndex])
								if key == filterTag && value == filterValue {
									var nodeRefs = make([]int64, len(way.Refs))
									var prevNodeId int64 = 0
									for index, deltaNodeId := range way.Refs {
										nodeId := prevNodeId + deltaNodeId
										prevNodeId = nodeId
										nodeRefs[index] = nodeId
									}
									appendNodeRefs <- nodeRefs
								}
							}
						}
					}
				}

				pending <- true
			}
		}()
	}

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendNodeRefs)
			<-appendNodeRefsComplete
			close(appendNodeRefsComplete)
		}
	}

	return wayNodeRefs
}

func isInBoundingBoxes(boundingBoxes [][]float64, lon float64, lat float64) bool {
	for _, boundingBox := range boundingBoxes {
		if boundingBox == nil {
			continue
		}
		if lon >= boundingBox[0] && lat >= boundingBox[1] && lon <= boundingBox[2] && lat <= boundingBox[3] {
			return true
		}
	}
	return false
}

func calculateBoundingBoxesPass(file *os.File, wayNodeRefs [][]int64, totalBlobCount int) [][]float64 {
	// maps node ids to wayNodeRef indexes
	nodeOwners := make(map[int64][]int, len(wayNodeRefs)*4)
	for wayIndex, way := range wayNodeRefs {
		for _, nodeId := range way {
			if nodeOwners[nodeId] == nil {
				nodeOwners[nodeId] = make([]int, 0, 1)
			}
			nodeOwners[nodeId] = append(nodeOwners[nodeId], wayIndex)
		}
	}

	pending := make(chan bool)
	updateWayBoundingBoxes := make(chan boundingBoxUpdate)
	updateWayBoundingBoxesComplete := make(chan bool)

	wayBoundingBoxes := make([][]float64, len(wayNodeRefs))

	go func() {
		for update := range updateWayBoundingBoxes {
			boundingBox := wayBoundingBoxes[update.wayIndex]
			if boundingBox == nil {
				boundingBox = make([]float64, 4)
				boundingBox[0] = update.lon
				boundingBox[1] = update.lat
				boundingBox[2] = update.lon
				boundingBox[3] = update.lat
				wayBoundingBoxes[update.wayIndex] = boundingBox
			} else {
				boundingBox[0] = math.Min(boundingBox[0], update.lon)
				boundingBox[1] = math.Min(boundingBox[1], update.lat)
				boundingBox[2] = math.Max(boundingBox[2], update.lon)
				boundingBox[3] = math.Max(boundingBox[3], update.lat)
			}
		}
		updateWayBoundingBoxesComplete <- true
	}()

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for node := range MakeNodeReader(primitiveBlock) {
						owners := nodeOwners[node.GetNodeId()]
						if owners == nil {
							continue
						}
						lon, lat := node.GetLonLat()
						for _, wayIndex := range owners {
							updateWayBoundingBoxes <- boundingBoxUpdate{wayIndex, lon, lat}
						}

					}
				}

				pending <- true
			}
		}()
	}

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(updateWayBoundingBoxes)
			<-updateWayBoundingBoxesComplete
			close(updateWayBoundingBoxesComplete)
		}
	}

	return wayBoundingBoxes
}

func findNodesWithinBoundingBoxesPass(file *os.File, boundingBoxes [][]float64, totalBlobCount int) []node {
	retvalNodes := make([]node, 0, 100000)
	pending := make(chan bool)

	appendNode := make(chan node)
	appendNodeComplete := make(chan bool)

	go func() {
		for node := range appendNode {
			retvalNodes = append(retvalNodes, node)
		}
		appendNodeComplete <- true
	}()

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for nodeAbs := range MakeNodeReader(primitiveBlock) {
						lon, lat := nodeAbs.GetLonLat()
						if isInBoundingBoxes(boundingBoxes, lon, lat) {
							keys, vals := nodeAbs.GetKeyValues()
							node := node{
								nodeAbs.GetNodeId(),
								lon,
								lat,
								keys,
								vals,
							}
							appendNode <- node
						}
					}
				}

				pending <- true
			}
		}()
	}

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendNode)
			<-appendNodeComplete
			close(appendNodeComplete)
		}
	}

	return retvalNodes
}

func findWaysUsingNodesPass(file *os.File, nodes []node, totalBlobCount int) []way {
	ways := make([]way, 0, 1000)
	pending := make(chan bool)

	nodeSet := make(map[int64]bool, len(nodes))
	for _, node := range nodes {
		nodeSet[node.id] = true
	}

	appendWay := make(chan way)
	appendWayComplete := make(chan bool)

	go func() {
		for way := range appendWay {
			ways = append(ways, way)
		}
		appendWayComplete <- true
	}()

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for _, primitiveGroup := range primitiveBlock.Primitivegroup {
						for _, osmWay := range primitiveGroup.Ways {

							match := false

							var prevNodeId int64 = 0
							for _, deltaNodeId := range osmWay.Refs {
								nodeId := prevNodeId + deltaNodeId
								prevNodeId = nodeId

								if nodeSet[nodeId] {
									match = true
									break
								}
							}

							if match {
								nodeRefs := make([]int64, len(osmWay.Refs))
								prevNodeId = 0
								for index, deltaNodeId := range osmWay.Refs {
									nodeId := prevNodeId + deltaNodeId
									prevNodeId = nodeId
									nodeRefs[index] = nodeId
								}

								keys := make([]string, len(osmWay.Keys))
								vals := make([]string, len(osmWay.Keys))
								for i, keyIndex := range osmWay.Keys {
									valueIndex := osmWay.Vals[i]
									keys[i] = string(primitiveBlock.Stringtable.S[keyIndex])
									vals[i] = string(primitiveBlock.Stringtable.S[valueIndex])
								}

								appendWay <- way{
									*osmWay.Id,
									nodeRefs,
									keys,
									vals,
								}
							}
						}
					}
				}

				pending <- true
			}
		}()
	}

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendWay)
			<-appendWayComplete
			close(appendWayComplete)
		}
	}

	return ways
}

func findNodesReferencedByWaysPass(file *os.File, ways []way, nodes []node, totalBlobCount int) []node {
	pending := make(chan bool)

	const NODE_REFERENCED = 1
	const NODE_STORED = 2

	nodeSet := make(map[int64]int, len(nodes))

	for _, way := range ways {
		for _, nodeId := range way.nodeIds {
			nodeSet[nodeId] = NODE_REFERENCED
		}
	}

	for _, node := range nodes {
		nodeSet[node.id] = NODE_STORED
	}

	appendNode := make(chan node)
	appendNodeComplete := make(chan bool)

	go func() {
		for node := range appendNode {
			nodeSet[node.id] = NODE_STORED
			nodes = append(nodes, node)
		}
		appendNodeComplete <- true
	}()

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for nodeAbs := range MakeNodeReader(primitiveBlock) {
						if nodeSet[nodeAbs.GetNodeId()] == NODE_REFERENCED {
							lon, lat := nodeAbs.GetLonLat()
							keys, vals := nodeAbs.GetKeyValues()
							node := node{
								nodeAbs.GetNodeId(),
								lon,
								lat,
								keys,
								vals,
							}
							appendNode <- node
						}
					}
				}

				pending <- true
			}
		}()
	}

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendNode)
			<-appendNodeComplete
			close(appendNodeComplete)
		}
	}

	return nodes
}

func writeNodes(file *os.File, nodes []node) error {
	if len(nodes) == 0 {
		return nil
	}

	for nodeGroupIndex := 0; nodeGroupIndex < (len(nodes)/8000)+1; nodeGroupIndex++ {
		beg := (nodeGroupIndex + 0) * 8000
		end := (nodeGroupIndex + 1) * 8000
		if len(nodes) < end {
			end = len(nodes)
		}
		nodeGroup := nodes[beg:end]

		stringTable := make([][]byte, 1, 1000)
		stringTableIndexes := make(map[string]uint32, 0)

		for _, node := range nodeGroup {
			for _, s := range node.keys {
				idx := stringTableIndexes[s]
				if idx == 0 {
					stringTableIndexes[s] = uint32(len(stringTable))
					stringTable = append(stringTable, []byte(s))
				}
			}
			for _, s := range node.values {
				idx := stringTableIndexes[s]
				if idx == 0 {
					stringTableIndexes[s] = uint32(len(stringTable))
					stringTable = append(stringTable, []byte(s))
				}
			}
		}

		osmNodes := make([]*OSMPBF.Node, len(nodeGroup))

		for idx, node := range nodeGroup {
			osmNode := &OSMPBF.Node{}

			var nodeId int64 = node.id
			osmNode.Id = &nodeId

			var rawlon int64 = int64(node.lon/.000000001) / 100
			var rawlat int64 = int64(node.lat/.000000001) / 100
			osmNode.Lon = &rawlon
			osmNode.Lat = &rawlat

			osmNode.Keys = make([]uint32, len(node.keys))
			for i, s := range node.keys {
				osmNode.Keys[i] = stringTableIndexes[s]
			}
			osmNode.Vals = make([]uint32, len(node.values))
			for i, s := range node.values {
				osmNode.Vals[i] = stringTableIndexes[s]
			}
			osmNodes[idx] = osmNode
		}

		group := OSMPBF.PrimitiveGroup{}
		group.Nodes = osmNodes

		block := OSMPBF.PrimitiveBlock{}
		block.Stringtable = &OSMPBF.StringTable{stringTable, nil}
		block.Primitivegroup = []*OSMPBF.PrimitiveGroup{&group}
		err := WriteBlock(file, &block, "OSMData")
		if err != nil {
			return err
		}
	}

	return nil
}

func writeWays(file *os.File, ways []way) error {
	if len(ways) == 0 {
		return nil
	}

	for wayGroupIndex := 0; wayGroupIndex < (len(ways)/8000)+1; wayGroupIndex++ {
		beg := (wayGroupIndex + 0) * 8000
		end := (wayGroupIndex + 1) * 8000
		if len(ways) < end {
			end = len(ways)
		}
		wayGroup := ways[beg:end]

		stringTable := make([][]byte, 1, 1000)
		stringTableIndexes := make(map[string]uint32, 0)

		for _, way := range wayGroup {
			for _, s := range way.keys {
				idx := stringTableIndexes[s]
				if idx == 0 {
					stringTableIndexes[s] = uint32(len(stringTable))
					stringTable = append(stringTable, []byte(s))
				}
			}
			for _, s := range way.values {
				idx := stringTableIndexes[s]
				if idx == 0 {
					stringTableIndexes[s] = uint32(len(stringTable))
					stringTable = append(stringTable, []byte(s))
				}
			}
		}

		osmWays := make([]*OSMPBF.Way, len(wayGroup))

		for idx, way := range wayGroup {
			osmWay := &OSMPBF.Way{}

			var wayId int64 = way.id
			osmWay.Id = &wayId

			// delta-encode the node ids
			nodeRefs := make([]int64, len(way.nodeIds))
			var prevNodeId int64 = 0
			for i, nodeId := range way.nodeIds {
				nodeIdDelta := nodeId - prevNodeId
				prevNodeId = nodeId
				nodeRefs[i] = nodeIdDelta
			}
			osmWay.Refs = nodeRefs

			osmWay.Keys = make([]uint32, len(way.keys))
			for i, s := range way.keys {
				osmWay.Keys[i] = stringTableIndexes[s]
			}
			osmWay.Vals = make([]uint32, len(way.values))
			for i, s := range way.values {
				osmWay.Vals[i] = stringTableIndexes[s]
			}
			osmWays[idx] = osmWay
		}

		group := OSMPBF.PrimitiveGroup{}
		group.Ways = osmWays

		block := OSMPBF.PrimitiveBlock{}
		block.Stringtable = &OSMPBF.StringTable{stringTable, nil}
		block.Primitivegroup = []*OSMPBF.PrimitiveGroup{&group}
		err := WriteBlock(file, &block, "OSMData")
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	inputFile := flag.String("i", "input.pbf.osm", "input OSM PBF file")
	outputFile := flag.String("o", "output.pbf.osm", "output OSM PBF file")
	highMemory := flag.Bool("high-memory", false, "use higher amounts of memory for higher performance")
	filterTag := flag.String("t", "leisure", "tag to filter ways based upon")
	filterValue := flag.String("v", "golf_course", "value to ensure that the way's tag is set to")
	flag.Parse()

	file, err := os.Open(*inputFile)
	if err != nil {
		println("Unable to open file:", err.Error())
		os.Exit(1)
	}

	// Count the total number of blobs; provides a nice progress indicator
	totalBlobCount := 0
	for {
		blobHeader, err := ReadNextBlobHeader(file)
		if err == io.EOF {
			break
		} else if err != nil {
			println("Blob header read error:", err.Error())
			os.Exit(2)
		}

		totalBlobCount += 1
		file.Seek(int64(*blobHeader.Datasize), os.SEEK_CUR)
	}
	println("Total number of blobs:", totalBlobCount)

	if *highMemory {
		cacheUncompressedBlobs = make(map[int64][]byte, totalBlobCount)
	}

	println("Pass 1/6: Find OSMHeaders")
	supportedFilePass(file)
	println("Pass 1/6: Complete")

	println("Pass 2/6: Find node references of matching areas")
	wayNodeRefs := findMatchingWaysPass(file, *filterTag, *filterValue, totalBlobCount)
	println("Pass 2/6: Complete;", len(wayNodeRefs), "matching ways found.")

	println("Pass 3/6: Establish bounding boxes")
	boundingBoxes := calculateBoundingBoxesPass(file, wayNodeRefs, totalBlobCount)
	println("Pass 3/6: Complete;", len(boundingBoxes), "bounding boxes calculated.")

	println("Pass 4/6: Find nodes within bounding boxes")
	nodes := findNodesWithinBoundingBoxesPass(file, boundingBoxes, totalBlobCount)
	println("Pass 4/6: Complete;", len(nodes), "nodes located.")

	println("Pass 5/6: Find ways using intersecting nodes")
	ways := findWaysUsingNodesPass(file, nodes, totalBlobCount)
	println("Pass 5/6: Complete;", len(ways), "ways located.")

	println("Pass 6/6: Find nodes referenced by intersected ways")
	nodes = findNodesReferencedByWaysPass(file, ways, nodes, totalBlobCount)
	println("Pass 6/6: Complete;", len(nodes), "total nodes (pass 4 + pass 6) located.")

	output, err := os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Out 1/3: Writing header")
	err = WriteHeader(output)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Out 2/3: Writing nodes")
	err = writeNodes(output, nodes)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Out 3/3: Writing ways")
	err = writeWays(output, ways)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	err = output.Close()
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}
}
