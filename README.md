# Terrier Micro

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/be331c1b98ca42b588db6115c548df07)](https://www.codacy.com?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=tonellotto/terrier-micro&amp;utm_campaign=Badge_Grade)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

This project provides a lightweight implementation of some query processing strategies built on top of Terrier 5. It re-implements the query processing pipeline of the Terrier search engine, removing all unnecessary features such as document score modifiers, multiple weighting models, etc.

This package is [free software](http://www.gnu.org/philosophy/free-sw.html) distributed under the [GNU Lesser General Public License](http://www.gnu.org/copyleft/lesser.html).

## Pre-requisites

[Elias-Fano compression for Terrier](https://github.com/tonellotto/terrier-ef) is required for testing purposes, but it is not explicitly required for using the Terrier Micro package.

To install the Elias-Fano compression for Terrier package (version 1.5.1) on your local machine, please run the following commands.

```bash
git clone https://github.com/tonellotto/terrier-ef
cd terrier-ef
git checkout 1.5.1
mvn install
```

## Usage

If not already available, e.g. from Maven Central, you should git clone and install Terrier Micro (version 1.5.1):

```bash
git clone https://github.com/tonellotto/terrier-micro
cd terrier-micro
git checkout 1.5.1
mvn -DskipTests clean install appassembler:assemble
```

The main script to perform batch query processing is the [retrieve](./docs/retrieve.md) tool.

If you want to use all available processors on your machine to perform batch query processing, use the [parallel retrieve](./docs/parallel_retrieve.md) tool.

Two other scripts are provided, to support advanced query processing strategies: the [ms-generate](./docs/ms-gen.md) and [bmw-generate](./docs/bmw-gen.md) tools.

#Usage from within Terrier

Following the previous two steps, you can also use terrier-micro from within the familiar `bin/terrier` commandline scripts.

Firstly, ensure that terrier-ef and terrier-micro are installed on your machine. Then, configure Terrier's Maven resolver to import these by appending the following line to your terrier.properties file:

    terrier.mvn.coords=it.cnr.isti.hpclab:terrier-micro:1.5.1
    
You can then use terrier-ef and terrier-micro tools, as folllows: 

    bin/terrier ef-recompress $PWD/var/index ef
    bin/terrier micro-ms-generator -I $PWD/var/index/ef.properties -w BM25
    bin/terrier micro-bmw-generator -I $PWD/var/index/ef.properties -b 512

## Credits

Developed by Nicola Tonellotto, ISTI-CNR. Contributions by Craig Macdonald, University of Glasgow, and Matteo Catena, ISTI-CNR.

