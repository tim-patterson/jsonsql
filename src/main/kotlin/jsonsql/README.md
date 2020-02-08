## jsonsql directory

### Directory Contents
* *fileformats* - Serdes for csv, json etc
* *filesystems* - Sources for data, aka protocols, kafka, s3, http, local etc
* *functions* - Sql functions
* *logical* - Logical operators, ie scan, filter etc
* *physical* - Actual physical operators that actually execute the query
* *query* - Data structures etc that map pretty closely to queries as you'd write them, validation, scoping etc done in here
* *shell* - Code that interacts with the terminal, renders tables etc, main method lives in here