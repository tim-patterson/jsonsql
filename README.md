# JsonSQL

A toy project that allows querying of newline delimited json using simple sql expressions

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
> Note internally this actually gets converted to use the `idx` function

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

### Querying from AWS S3
By using an S3 url in our table JsonSQL can query data stored in s3.  It uses the aws java sdk to do this.
Standard aws environment vars like `AWS_PROFILE`, `AWS_REGION` etc will be picked up and used.
![select output](https://github.com/tim-patterson/jsonsql/raw/master/docs/select-s3.png)
