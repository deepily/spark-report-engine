# Interactive Spark SQL Report Engine

Interactive spark shell for developing, debugging and running hundreds of Spark SQL count and median queries.

_Problem_: Developing, compiling and pushing compiled Spark SQL applications from an IDE to an AWS cluster for testing and debugging has huge overhead costs.

_Example_: One application that I inherited had over 300 Spark SQL queries compiled into the source code.  It took a minute to compile, and a few more minutes to push and launch on cluster.  Over four minutes of waiting for the gears to grind just to test a one-character change in the source code.

_Solution_: Compile and bootstrap Spark application once, in less than 10 seconds.  Separate SQL text from compiled code.  Edit, push and test query definition files in less than 10 seconds.  That's more than 20x faster to iterate on queries.

