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
	"github.com/mfenniak/go-osmpbf-filter/src/OSMPBF"
)

type OsmNodeAbstraction interface {
	GetNodeId() int64
	GetLonLat() (float64, float64)
	GetKeyValues() ([]string, []string)
}

type sparseOsmNode struct {
	osmPrimitiveBlock *OSMPBF.PrimitiveBlock
	osmNode           *OSMPBF.Node
}

type denseOsmNode struct {
	osmPrimitiveBlock  *OSMPBF.PrimitiveBlock
	osmDenseNodes      *OSMPBF.DenseNodes
	nodeId             int64
	rawLon             int64
	rawLat             int64
	startKeyValueIndex int
	endKeyValueIndex   int
}

func calculateLonLat(primitiveBlock *OSMPBF.PrimitiveBlock, rawlon int64, rawlat int64) (float64, float64) {
	var lonOffset int64 = 0
	var latOffset int64 = 0
	var granularity int64 = 100
	if primitiveBlock.LonOffset != nil {
		lonOffset = *primitiveBlock.LonOffset
	}
	if primitiveBlock.LatOffset != nil {
		latOffset = *primitiveBlock.LatOffset
	}
	if primitiveBlock.Granularity != nil {
		granularity = int64(*primitiveBlock.Granularity)
	}

	lon := .000000001 * float64(lonOffset+(granularity*rawlon))
	lat := .000000001 * float64(latOffset+(granularity*rawlat))

	return lon, lat
}

func newSparseOsmNode(osmPrimitiveBlock *OSMPBF.PrimitiveBlock, osmNode *OSMPBF.Node) OsmNodeAbstraction {
	return &sparseOsmNode{osmPrimitiveBlock, osmNode}
}

func (node *sparseOsmNode) GetNodeId() int64 {
	return *node.osmNode.Id
}

func (node *sparseOsmNode) GetLonLat() (float64, float64) {
	return calculateLonLat(node.osmPrimitiveBlock, *node.osmNode.Lon, *node.osmNode.Lat)
}

func (node *sparseOsmNode) GetKeyValues() ([]string, []string) {
	keys := make([]string, len(node.osmNode.Keys))
	vals := make([]string, len(node.osmNode.Keys))
	for i, keyIndex := range node.osmNode.Keys {
		valueIndex := node.osmNode.Vals[i]
		keys[i] = string(node.osmPrimitiveBlock.Stringtable.S[keyIndex])
		vals[i] = string(node.osmPrimitiveBlock.Stringtable.S[valueIndex])
	}
	return keys, vals
}

func newDenseOsmNode(osmPrimitiveBlock *OSMPBF.PrimitiveBlock, osmDenseNodes *OSMPBF.DenseNodes, nodeId int64, rawLon int64, rawLat int64, startKeyValIndex int, endKeyValIndex int) OsmNodeAbstraction {
	return &denseOsmNode{osmPrimitiveBlock, osmDenseNodes, nodeId, rawLon, rawLat, startKeyValIndex, endKeyValIndex}
}

func (node *denseOsmNode) GetNodeId() int64 {
	return node.nodeId
}

func (node *denseOsmNode) GetLonLat() (float64, float64) {
	return calculateLonLat(node.osmPrimitiveBlock, node.rawLon, node.rawLat)
}

func (node *denseOsmNode) GetKeyValues() ([]string, []string) {
	numItems := 0
	if len(node.osmDenseNodes.KeysVals) != 0 {
		numItems = (node.endKeyValueIndex - node.startKeyValueIndex) / 2
	}
	keys := make([]string, numItems)
	vals := make([]string, numItems)
	for i := 0; i < numItems; i++ {
		keys[i] = string(node.osmPrimitiveBlock.Stringtable.S[node.osmDenseNodes.KeysVals[node.startKeyValueIndex+(i*2)]])
		vals[i] = string(node.osmPrimitiveBlock.Stringtable.S[node.osmDenseNodes.KeysVals[node.startKeyValueIndex+(i*2)+1]])
	}
	return keys, vals
}

func MakeNodeReader(primitiveBlock *OSMPBF.PrimitiveBlock) <-chan OsmNodeAbstraction {
	retval := make(chan OsmNodeAbstraction)

	go func() {
		for _, primitiveGroup := range primitiveBlock.Primitivegroup {
			for _, osmNode := range primitiveGroup.Nodes {
				retval <- newSparseOsmNode(primitiveBlock, osmNode)
			}

			if primitiveGroup.Dense != nil {
				var prevNodeId int64 = 0
				var prevLat int64 = 0
				var prevLon int64 = 0
				keyValIndex := 0

				for idx, deltaNodeId := range primitiveGroup.Dense.Id {
					nodeId := prevNodeId + deltaNodeId
					rawlon := prevLon + primitiveGroup.Dense.Lon[idx]
					rawlat := prevLat + primitiveGroup.Dense.Lat[idx]

					prevNodeId = nodeId
					prevLon = rawlon
					prevLat = rawlat

					startKeyValIndex := 0

					// Not sure why KeysVals can be length zero, this
					// doesn't seem to be documented, but I'll assume that
					// means none of the nodes have data associated with
					// them.
					if len(primitiveGroup.Dense.KeysVals) != 0 {
						startKeyValIndex = keyValIndex
						for primitiveGroup.Dense.KeysVals[keyValIndex] != 0 {
							keyValIndex += 2
						}
					}

					retval <- newDenseOsmNode(primitiveBlock, primitiveGroup.Dense, nodeId, rawlon, rawlat, startKeyValIndex, keyValIndex)

					keyValIndex += 1
				}
			}
		}

		close(retval)
	}()

	return retval
}
