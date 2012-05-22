go-osmpbf-filter
----------------

go-osmpbf-filter is a program to filter OpenStreetMap PBF format data files.
The filter performs five passes on the input PBF filter, and then writes a new
output PBF file.  The five passes are:

1. Find the OSMHeaders data block and ensure that we support reading this
   file.

2. Find ways that match a given tag criteria (eg. leisure=golf_course), and
   extract their node references.

3. Calculate bounding boxes for each way given the bounding boxes found in (2).

4. Find all the nodes within the bounding boxes from (3).

5. Find all the ways that reference nodes found in (4).

The nodes and ways collected in pass (4) and (5) are then output into a new PBF
format data file.

go-osmpbf-filter is written in Go_.  It is highly concurrent, so you can
expect it to use up all your CPU power.  go-osmpbf-filter can filter out 1MB of
golf course areas from a 1GB PBF file in about 9 minutes, using 789% of a Core
i7 processor.

.. _Go: http://golang.org/


Building
========

Building go-osmpbf-filter is easy::

    cd go-osmpbf-filter
    GOPATH=`pwd` go build


Examples
========

Filter a pbf file with the default settings (leisure=golf_course)::

    go-osmpbf-filter -i alberta.osm.pbf -o alberta-filtered.osm.pbf


Filter searching for a specified tag, like sport=baseball::

    go-osmpbf-filter -i japan.osm.pbf -o japan-baseball.osm.pbf -t sport -v baseball


Command-Line Options
====================

-i
  Input file

-o
  Output file

-t
  Filter tag key

-v
  Filter tag value

--high-memory
  Cache every decompressed block in memory.  This can cause a 25% performance
  improvement in filtering, but is only recommended for small files.  For a
  ~100MB PBF file, this will increase memory usage by about 100MB; for a 1GB
  PBF file, memory usage will jump to 8GB or more.

