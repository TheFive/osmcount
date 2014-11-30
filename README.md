osmcount
========

Project to count several OpenStreetMap objects based on Overpass to motivate quality assurance.

1. Vision

osmcount will be a tool to execute overpass querys on a [daily | weekly | monthly] basis and store the numbers of results 
in a MongoDB. The query should run on a "clustering" type, like destrics, motorways...
The tool will show the results in a simple HTML table and allow aggregation and date filtering. There should be the possibilty 
comparing the results numbers with a target number.

On Top - of course - the vision is that the developers will learn about

a) a nosql store (choosen: mongodb)
b) a server implemented with node.js
c) list can be extended...


2. Status

The tool is far away from its vision.
