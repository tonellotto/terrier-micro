# Terrier 5 Micro 

This project provides a lightweight implementation of some query processing strategies built on top of Terrier 5. It re-implements from scratch the query processing pipeline of the Terrier search engine, removing all unnecessary features such as document score modifiers, multiple weighting models, etc.

This package is [free software](http://www.gnu.org/philosophy/free-sw.html) distributed under the [GNU Lesser General Public License](http://www.gnu.org/copyleft/lesser.html).

## Pre-requisites

Terrier 5.0 is required.
Elias-Fano compression for Terrier 5 is optional.

## Usage

If not already available, e.g. from Maven Central, you should git clone and install terrier-micro, version 1.5:

```
mvn -DskipTests clean package appassembler:assemble
```

The main script to perform batch query processing is the [retrieve](./docs/retrieve.md) tool.
Two other scripts are provided, to support advanced query processing strategies: the [ms-generate](./docs/ms-gen.md) and [bmw-generate](./docs/bmw-gen.md) tools.