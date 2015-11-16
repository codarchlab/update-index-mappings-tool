# update-index-mappings-tool

A tool to update elasticsearch mappings with zero downtime and less mapping troubles.

```
Disclaimer:

This tool is meant to be for development only. 
Any use in a production environment may happen at your own risk!
```

The purpose of this script is to update the mappings of all types of
a given index. This is to avoid troubles that usually occur with elasticsearch
when trying to update mappings over indices already containing data which may collide
with the new mappings. By creating new indices with current mappings and copying over 
the data these troubles are usually less.

In order to to this, an new index gets created with the new mapping 
and the contents of the old index got copied over to the new index. It does so by
internally using an alias which points to the new index at that time.

## Installation

The packages node and npm must be installed on your machine in order to 
run the script.

```bash
npm install

Before running the tool, check out the *config/application.json* to match your
environment.

## Usage

The first time you call this tool it is assumed you only have one index whose name
is configures in *config/application.json*. There must be no aliases and no suffixed versions
of this index name. By running the line

```bash
node updateMappings.js init
```

a new index gets created. The new index gets provided with the mappings from config/types. Note
that only the types configured in *config/application.json* get mapped. After its creation the 
new index will have the name of the old index plus *_1* while the original index name
will be used as an alias to this new index. 

Now the index is in a state where the index can be updated with the mappings from config/types
again and again by running

```bash
node updateMappings.js
```

The tool then creates a new index named like the alias plus the suffix *_2* (or *_1* respective), 
inits it with the mappings from *config/types* copies over the contents from the old index and
then sets the alias to this new index accordingly.

**Note** that while the init version has to happen when the index is not accessed the second version can 
be done anytime without downtime.
