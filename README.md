This Project was used to do some statistics for OpenStreetMap "Wochenaufgaben".
As it was also a "Training" project for the developer, and is not in use any longer,
repository is archived was archived in November 2018

-----


osmcount
========

Master:   | develop:
----------|----------------------
[![Build Status](https://travis-ci.org/TheFive/osmcount.svg?branch=master)](https://travis-ci.org/TheFive/osmcount) | [![Build Status](https://travis-ci.org/TheFive/osmcount.svg?branch=develop)](https://travis-ci.org/TheFive/osmcount)
[![codecov.io](https://codecov.io/github/TheFive/osmcount/coverage.svg?branch=master)](https://codecov.io/github/TheFive/osmcount?branch=master) | [![codecov.io](https://codecov.io/github/TheFive/osmcount/coverage.svg?branch=develop)](https://codecov.io/github/TheFive/osmcount?branch=develop)


Project to count several OpenStreetMap objects with the Overpass API to motivate quality assurance.

1. Vision

osmcount will be a tool to execute overpass querys on a [daily | weekly | monthly] basis and store the numbers of results. The query should run on a "clustering" type, like destrics, motorways...
The tool will show the results in a simple HTML table and allow aggregation and date filtering. There should be the possibilty 
comparing the results numbers with a target number.

On Top - of course - the vision is that the developers will learn about

a) dbms (choosen postgres)

b) a server implemented with node.js

c) list can be extended...



2. Status

OSMCount managed it first "Wochenaufgabe" in February 2015.
On the choosen server, there occured trouble with memory need, and so i have opened a branch to migrate everything to postgres, hoping, that the topic is better configurable there.
In Mai 2015 the underlaying database was switched from mongodb to postgres.
