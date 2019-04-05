# Ahwen

A really simple SQL database.

## Layers

A good way to understand the system is to start from the lowest level layers and build your
way up from there. The layers are listed here in that order:

- `file` - read pages from disk and write them back to disk
- `log` - read and write log records
- `buffer` - maintain a cache of frequently accessed pages  
- `tx.recovery` - provide durability for changes
- `tx.concurrency` - implement locking needed to guarantee atomicity for changes
- `tx` - tie recovery and concurrency properties together
- `record` - provide structure on top of raw pages to represent database records
- `metadata` - maintain metadata about structure of different database objects
- `query` - queries and query plans expressed in relational algebra
- `parse` - parsing SQL statements
- `planner` - translate parsed SQL trees into relational algebra 
- `jdbc` - JDBC wrapper on top of everything else

## Credits

The implementation takes heavy inspiration from Edward Sciore's 
[SimpleDB](http://www.cs.bc.edu/~sciore/simpledb/), augmented by implementations
of various exercises in his textbook _Database Design and Implementation_. 
 
