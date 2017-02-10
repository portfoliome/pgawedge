[![Build Status](https://travis-ci.org/portfoliome/pgawedge.svg?branch=master)](https://travis-ci.com/portfoliome/pgawedge)
[![codecov.io](http://codecov.io/github/portfoliome/pgawedge/coverage.svg?branch=master)](http://codecov.io/github/portfoliome/pgawedge?branch=master)
[![Code Health](https://landscape.io/github/portfoliome/pgawedge/master/landscape.svg?style=flat)](https://landscape.io/github/portfoliome/pgawedge/master)

# pgawedge
Postgresql Sqlalchemy adapter.

# Purpose

pgawedge acts as an adapter(wedge) for setting sqlalchemy to use postgres conventions. Additionally, many utilities that relying on implementations buried throughout sqlalchemy's extensive documentation are included.

These include:
 - default postgres connection environmental variables names.
 - Primary key, foreign key, constraint naming conventions.
 - Server side UUID and UTC TIMESTAMP creation.
 - Serialization/deserialization for UUID's and JSON objects.

# Pre-release Status

NOTE: pgawedge is still a pre-release and the API Could change. The most significant changes would affect the schema reflection API in alchemy.py.
