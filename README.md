# JsonSQL
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[![Build Status](https://travis-ci.org/tim-patterson/jsonsql.svg?branch=master)](https://travis-ci.org/tim-patterson/jsonsql)

A standalone app that allows querying of newline delimited json using simple sql expressions

## Getting Started
All that's required to get started is to checkout the code and then compile and run. ie:
```sh
git clone git@github.com:tim-patterson/jsonsql.git
cd jsonsql
./run
```

## Basics
The basic syntax of JsonSQL is very similar to basic sql with a couple of major points of difference
* Instead of tables we select from files or directories, ie `json '/some/location'`
* JsonSQL is dynamically/weakly typed, the actual structure of the data being queried isn't known at query planning/compilation time

### Describing json structure
```sql
describe json 'test_data/nested.json';
```
![describe output](https://github.com/tim-patterson/jsonsql/raw/master/docs/describe.png)

### Basic selects
```sql
select rownum, arrayval, structval from json 'test_data/nested.json';
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-basic.png)

One thing to note here is that because traditionally the `*` in select `*` is an expansion that it done at query
planning time based on the known structure of a table, it simply wont work in JsonSQL, in
some cases it might be handy to use the `__all__` virtual column that returns the table whole row as a single column, ie

```sql
select __all__ from json 'test_data/nested.json';
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-all.png)

### Dealing with nested values - structs/maps
structs are pretty simple to deal with, just use dot notation to drill down into nested structures
```sql
select rownum, structval.inner_key from json 'test_data/nested.json';
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-struct.png)

As an alternative an indexing style syntax may be used, this can be useful if the key is to be computed
```sql
select rownum, structval["inner_key"] from json 'test_data/nested.json';
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-struct-index.png)

> Note internally both syntaxes actually gets converted to use the `idx` function

> ie. `select rownum, idx(structval, 'inner_key') from json 'test_data/nested.json';` will return the same output

### Dealing with nested values - arrays
For arrays we use a similar `lateral view` type syntax as is used in hive.

With this syntax we "explode" out the array type to produce a row for each entry
```sql
select rownum, arrayval from json 'test_data/nested.json'
lateral view arrayval limit 5;
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-array.png)
> Note in the above the exploded `arrayval` is shadowing the underlying table's `arrayval`.
> the lateral view can be aliased if needed, ie `lateral view arrayval as exploded`

If we instead just want to return a single element out of an array we can use the same index style syntax as used by structs
```sql
select rownum, arrayval[0] from json 'test_data/nested.json';
```
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-array-index.png)

### Querying from AWS S3
By using an S3 url in our table JsonSQL can query data stored in s3.  It uses the aws java sdk to do this.
Standard aws environment vars like `AWS_PROFILE`, `AWS_REGION` etc will be picked up and used.
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-s3.png)


### Querying from http sources
Just as we can query from s3 urls we can also query from http(s) urls.

Try this one to see how it works
```sql
select
  d.title,
  d.score,
  d.permalink,
  d.num_comments
from (
  select children.data as d
  from json 'https://www.reddit.com/r/all.json?limit=100'
  lateral view data.children
)
order by num_comments desc;
```

## Project Goals
The main goals for this project are to be a lightweight standalone simple to use tool for adhoc querying of unstructured data using a
syntax that's as close as possible to standard sql.

## Alternatives
The major alternative that provides querying of json data using sql without having to create table definitions etc is
[Apache Drill](https://drill.apache.org/)
