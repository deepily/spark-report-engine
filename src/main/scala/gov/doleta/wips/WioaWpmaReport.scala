package gov.doleta.wips

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import java.text.NumberFormat

object WioaWpmaReport {

  val debugging = true

  def main( args: Array[String] ) {

    // timing conversion of 19M rows of CSV into parquet
    if ( 0 == 1 ) {
      val spark = SparkSession.builder.appName( "Repartitioner" ).getOrCreate()

      val df = spark.read.format( "csv" ).option( "header", "false" ).load( "s3://wips-dev-redshift-ready/spra/13/allrows" ).cache

      var counter = 4
      while ( counter <= 128 ) {

        println( "==========================================================================================" )
        println( s"Counter [$counter]" )
        println( "==========================================================================================" )

        var startMillis = System.currentTimeMillis()

        // write w/ n partitions
        val s3path = s"s3://ricks-foo-bucket/wioa-wpma/19M-repartitioned-$counter"
        val newDf = df.repartition( counter ).cache
        println( s"Time to create [$counter] partitions [${( System.currentTimeMillis() - startMillis ) / 1000.0 / 60.0}]m" )

        //      // write as csv
        //      startMillis = System.currentTimeMillis()
        //      newDf.write.option( "header", "false" ).csv( s3path + ".csv" )
        //      println( s"Time to write [$counter] partitions as csv [${ ( System.currentTimeMillis() - startMillis ) / 1000.0 / 60.0 }]m" )

        // write as parquet
        startMillis = System.currentTimeMillis()
        newDf.write.parquet( s3path + ".parquet" )
        println( s"Time to write [$counter] partitions as parquet [${( System.currentTimeMillis() - startMillis ) / 1000.0 / 60.0}]m" )

        counter = counter * 2
      }
    }

    val startMillis = System.currentTimeMillis()

    // Begin regular work of reporting engine
    val argsMap = loadArgumentsMap( args )

    val appName = argsMap.getOrElse( "app-name", "WIOA WPMA Report" ).toString
    var spark: SparkSession = null
    var statsPrefix = "Stats |"

    // start and force to run on driver. Defaults to distributed
    if ( argsMap.getOrElse( "driver-only", "" ) == "true" ) {
      spark = SparkSession.builder.master( "local[1]" ).appName( appName ).getOrCreate()
    } else {
      spark = SparkSession.builder.appName( appName ).getOrCreate()
    }

    val reportEngine = new ReportEngine( spark, argsMap, appName, statsPrefix )

    // allow us to skip init if in interactive mode
    val interactive = argsMap.getOrElse( "interactive", "false" ) == "true"
    val runInit     = argsMap.getOrElse( "run-init",     "true" ) == "true"
    val batch = !interactive

    // always runs when in batch mode, or when interactive and runinit arg == true
    if ( batch || ( interactive && runInit ) ) {
      reportEngine.init()
    }
    // are we in interactive or batch ( default ) mode?
    if ( interactive ) {

      reportEngine.startInteractiveMode()

    } else {

      // batch mode:
      reportEngine.run()

      // get row count *AFTER* caching of input set via run() and *BEFORE* killing spark session
      val rows = reportEngine.getInputCount()

      reportEngine.stop()

      calculateRuntimeStats( appName, argsMap, rows, startMillis, spark, reportEngine, statsPrefix )
    }
  }
  /**
    * Parses name value pairs passed in the main methods's args array and populates a map with them for non-position dependent lookup
    * @param args
    */
  def loadArgumentsMap( args: Array[ String ] ): Map[ String, Any ] = {

    if ( debugging ) {
      println( "\n\n===================================================" )
      println( "= loadArgumentsMap called..." )
      println( "===================================================" )
    }
    var name = ""
    var value = ""
    val argsMap = Map[ String, Any ]()
    for ( arg <- args ) {

      // quick sanity check: Does arg have 'name=value' format?
      if ( arg.split( "=" ).length == 1 ) {
        println( "\n\n===================================================" )
        println( s"= args missing value: [$arg]" )
        println( "===================================================" )
        System.exit( -1 )
      }
      name = arg.split( "=" )( 0 )
      value = arg.split( "=" )( 1 )

      argsMap += ( name -> value )
    }

    if ( debugging ) {

      for ( key <- argsMap.keys ) {

        val value = argsMap( key )
        println( s"[$key] = [$value]" )
      }
    }
    argsMap
  }

  /**
    * Helper method for main( ... ) Calculates runtime performance stats
    * @param startMillis
    * @param spark
    * @param reportEngine
    */
  def calculateRuntimeStats(appName: String, argsMap: Map[ String, Any ], rows: Long, startMillis: Long, spark: SparkSession, reportEngine: ReportEngine, statsPrefix: String ): Unit = {

    val commaFormatter = NumberFormat.getIntegerInstance
    val seconds = ( System.currentTimeMillis() - startMillis ) / 1000.0
    val minutes = ( seconds / 60.0 )
    val rowsPerSecond = rows / seconds
    val kRowsPerSecond = rowsPerSecond / 1000.0
    val millionsOfRows = rows / 1000000.0
    val partitionsRequested = argsMap.getOrElse( "partitions", "0" )
    val partitionsActual = spark.table( "input" ).rdd.partitions.size
    val cellCalcsCount = reportEngine.getCellCalcsCount()
    val totalCalcsB = cellCalcsCount * rows / 1000000000.0
    val calcsPerSecB = totalCalcsB / seconds

    // print stats
    println( "\n\n===================================================" )
    println( s"= [$appName] run completed" )
    println( "===================================================" )
    println( f"$statsPrefix Input [${argsMap.getOrElse( "input", "" )}]" )
    println( f"$statsPrefix Partitions requested [$partitionsRequested] vs. actual/created [$partitionsActual]" )
    println( f"$statsPrefix Calcs finished in [$minutes%.1f] minutes @ rate of [$kRowsPerSecond%.1f]K rows/sec" )
    println( f"$statsPrefix [$totalCalcsB%.1f] Billion( s ) of calcs = [${commaFormatter.format( cellCalcsCount ) }] ( queries * subsets ) over [$millionsOfRows%.1f] Million( s ) of rows @ rate of [$calcsPerSecB%.1f]B calcs/sec\n\n" )
  }
}