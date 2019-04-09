# Interactive Spark SQL Report Engine
An interactive spark shell for developing, debugging and running hundreds of Spark SQL count and median queries.

## Build, Deploy and Bootstrapping Overhead in Iterative Development
_Problem_: Developing, compiling and pushing compiled Spark SQL applications from an IDE to an AWS cluster for testing and debugging has huge overhead costs.

_Example_: One similar application that I inherited had over 300 Spark SQL queries compiled into the source code.  It took a minute to compile, and a few more minutes to push and launch on cluster.  That's over four minutes of waiting for the gears to grind just to test a one-character change in the source code.

_Solution_: Separate SQL text from compiled code. Compile and bootstrap Spark application once, in less than 10 seconds.  Save query text edits, push via scp and reload query definition files (from w/in interactive Spark application shell) in less than 5 seconds.  That's more than 40x faster to iterate on query development.


## Query Verbosity
_Problem_: SQL queries can be repetitive and verbose

_Solution_: Create abbrevations with decompressed forms for dynamic replacment.

_Example_: The abbreviation "scfIw" becomes "SELECT count( * ) AS count FROM Input WHERE" at runtime

_Result_: The amount of text necessary to create a working Spark SQL query is reduced by ~43%.

