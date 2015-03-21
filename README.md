osmcount
========

[![Build Status](https://travis-ci.org/TheFive/osmcount.svg?branch=SwitchToPostgres)](https://travis-ci.org/TheFive/osmcount) (Switch to Postgres)
[![Build Status](https://travis-ci.org/TheFive/osmcount.svg?branch=master)](https://travis-ci.org/TheFive/osmcount) (Master)

Project to count several OpenStreetMap objects with the Overpass API to motivate quality assurance.

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

In Jan 2015 the display of the "Wochenaufgabe" Adresses Without Streets is implemented,
and a check for the "amenity=pharmacy" objects.
