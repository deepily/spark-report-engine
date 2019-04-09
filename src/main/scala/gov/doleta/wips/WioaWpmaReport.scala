package gov.doleta.wips

import java.sql.DriverManager
import java.io._
import java.nio.file.{Files, Paths}
import java.util.{Base64, Calendar, Collections, Date, Properties}
import java.text.SimpleDateFormat
import java.text.NumberFormat
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.util.concurrent.ThreadLocalRandom
import java.time.LocalDate

import scala.io.Source
import scala.io.StdIn
import sys.process._
import scala.util.parsing.json._
import scala.util.control.Exception.allCatch
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_date, split}
import com.amazonaws.services.kms.AWSKMSClient
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kms.model.DecryptRequest

class WioaWpmaReport( ss: SparkSession, am: Map[ String, Any ], an: String ) {

  /** START Global declarations */
  // Runtime flags
  private var debugging = true
  private var verbose = false
  private var dryRun = false

  // passed in CLI args in the form of name=value pairs
  private val argsMap = am
  // once we have global args ref assigned, update the boolean flags above
  updateGlobalFlags()

  private val startMillis = System.currentTimeMillis()
  private val appName = an
  private var spark = ss
  private val rawQueryMap               = Map[ String, ( Any, Any ) ]()
  private var generatedQueriesMap       = Map[ String, String ]().withDefaultValue( "" )
  private var resultsMap                = Map[ String, Any ]().withDefaultValue( "" )
  private var divisorsMap               = Map[ String, ( String, String ) ]().withDefaultValue( ( "", "" ) )
  private val expectedResultsMap        = Map[ String, Any ]().withDefaultValue( "" )
  private var abbreviationsList         = ListBuffer[ String ]()
  private var columnsToKeepList         = ListBuffer[ String ]()
  private var dateColumnNamesList       = ListBuffer[ String ]()
  private var staticRunList             = ListBuffer[ String ]()
  private var staticNationalMediansList = ListBuffer[ String ]()
  // Subsets (states) pulled from Tom's email on metadata
  private var subsetsList               = Array[ String ]()

  // Constants
  private val FORMULA_CONCATENATOR = "formula-concatenator"
  private val FORMULA_DIVISION     = "formula-division"
  private val QUERY_COUNT          = "count"
  private val QUERY_MEDIAN         = "median"
  private val UNDEFINED            = "undefined"
  private val WINDOW_RQ4           = "r4q"
  private val WINDOW_CQ            = "cq"
  private val WINDOW_PTD           = "ptd"
  private val QUARTER_ID           = "13"
  private val SUBSETS              = "subsets"
  private val NATIONAL             = "national"
  private val PARTITIONS           = "partitions"
  private val PARTITIONS_DEFAULT   = "64"


  // stats trackers
  private var medianRunTime       = 0L
  private var runCountRuleCounter = 0
  private var       appendCounter = 0
  /** END Global declarations */

  /**------------------------------------------------------------------------------------------------------------------*/
  /** START init block */
  def init(): Unit = {

    if ( debugging ) printBanner( "init() called" )

    loadConfigurationData()

    // default to true
    if ( argsMap.getOrElse( "load-cohorts-and-measures", "true" ) == "true" ) {
      loadCohortsAndMeasures()
      extractPeriods()
      extractStartAndEndDates()
    }
    // default to true
    if ( argsMap.getOrElse( "load-input", "true" ) == "true" ) {
      loadAndPrepareInput()
    }
  }

  /**
    * Updates global boolean flags from argsMap
    */
  private def updateGlobalFlags(): Unit = {

    // all default to false
    debugging      = argsMap.getOrElse( "debugging",       "" ) == "true"
    verbose        = argsMap.getOrElse( "verbose",         "" ) == "true"
    dryRun         = argsMap.getOrElse( "dry-run",         "" ) == "true"
  }
  /**
    * Loads query abbreviations from local path or resource/jar, according to runtime args supplied
    */
  private def loadQueryAbbreviations(): Unit = {

    if ( verbose ) printBanner( "loadQueryAbbreviations:" )

    var trimmedLine = ""

    // when in debugging mode read from file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "abbreviations", "/home/ruiz.richard.p/queries-abbreviations.txt" ).toString
      abbreviationsList = loadPlainTextFromLocalFile( path )

    } else {

      abbreviationsList = loadPlainTextFromJarFile( "/queries-abbreviations.txt" )
    }
  }

  /**
    * Loads query "columns to keep" list from local path or resource/jar, according to runtime args supplied
    */
  private def loadColumnsToKeep(): Unit = {

    if ( verbose ) printBanner( "loadColumnsToKeep:" )

    // when in debugging mode read from file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "column-to-keep", "/home/ruiz.richard.p/input-columns-to-keep.txt" ).toString
      columnsToKeepList = loadPlainTextFromLocalFile( path )

    } else {

      columnsToKeepList = loadPlainTextFromJarFile( "/input-columns-to-keep.txt" )
    }
  }
  /**
    * Loads list of column names that should be treated as dates from local path or resource/jar, according to runtime args supplied
    */
  private def loadDateColumns(): Unit = {

    if ( verbose ) printBanner( "loadDateColumns:" )

    // when in debugging mode read from file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "date-columns", "/home/ruiz.richard.p/input-date-columns.txt" ).toString
      dateColumnNamesList = loadPlainTextFromLocalFile( path )

    } else {

      dateColumnNamesList = loadPlainTextFromJarFile( "/input-date-columns.txt" )
    }
  }

  /**
    * Helper method for loading text files
    * @param line
    * @return true if input is not a commented or not zero lenth
    */
  private def isNotComment( line: String ): Boolean = { !line.startsWith( "//" ) && line != "" }

  /**
    * Loads a plaintext file and assigns each line to an arraybuffer
    * @param fileName
    * @return
    */
  private def loadPlainTextFromJarFile( fileName: String ): ListBuffer[ String ] = {

    // ...else read it from jar when in batch/production mode
    val is = getClass.getResourceAsStream( fileName )
    val isr = new InputStreamReader( is )
    val br = new BufferedReader( isr )

    var lines = ListBuffer[ String ]()

    try {

      var line = ""
      var trimmedLine = ""

      while( { line = br.readLine(); line!= null } ) {

        trimmedLine = line.trim

        // skip commented and empty lines
        if ( isNotComment( trimmedLine ) ) {

          // append line
          lines += trimmedLine
          if ( verbose ) println( s"[$fileName] line [$trimmedLine]" )
        }
      }
    } catch {

      case ex: Exception => stopWithMessage( s"ERROR reading [$fileName] from jar [$ex.toString]" )

    } finally {

      br.close
      isr.close
      is.close
    }
    lines
  }

  /**
    * Loads a plaintext file and assigns each line to an arraybuffer
    * @param fileName
    * @return
    */
  private def loadPlainTextFromLocalFile( fileName: String ): ListBuffer[ String ] = {

    var lines = ListBuffer[ String ]()

    val bufferedSource = Source.fromFile( fileName )
    var trimmedLine = ""

    for ( line <- bufferedSource.getLines ) {

      trimmedLine = line.trim

      // skip commented and empty lines
      if ( isNotComment( trimmedLine ) ) {

        // append line
        lines += trimmedLine
        if ( verbose ) println( s"[$fileName] line [$trimmedLine]" )
      }
    }
    bufferedSource.close
    lines
  }

  /**
    * Loads query subsets from local path or resource/jar, according to runtime args supplied.
    * Assumes: subsets are stored in plain text file in one long, comma-delimited, line
    */
  private def loadStaticQuerySubsets(): Unit = {

    if ( verbose ) printBanner( "loadQuerySubsets:" )

    var trimmedLine = ""

    // when in debugging mode read from file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "subsets", "/home/ruiz.richard.p/queries-subsets.txt" ).toString

      // Read first line only
      val source = Source.fromFile( path )
      val rawSubsets = try source.getLines().toList( 0 ).mkString finally source.close()
      subsetsList = rawSubsets.split( "," ).toArray

    } else {

      // ...else read it from jar when in batch/production mode
      val is = getClass.getResourceAsStream( "/queries-subsets.txt" )
      val isr = new InputStreamReader( is )
      val br = new BufferedReader( isr )

      var line = ""
      try {

        // we only need to read one line!
        line = br.readLine()

      } catch {

        case ex: Exception => stopWithMessage( s"ERROR reading subsets from jar [$ex.toString]" )

      } finally {

        br.close
        isr.close
        is.close
      }
      subsetsList = line.split( "," ).toArray
    }
    if ( debugging ) {
      println( s"subsetsList loaded [${subsetsList.mkString( "," )}]" )
    }
  }
  /**
    * Loads expected query results from local path or resource/jar, according to runtime args supplied
    */
  private def loadExpectedResults(): Unit = {

    if ( verbose ) printBanner( "loadExpectedResults" )

    var rawExpectedResultsList = ListBuffer[ String ]()

    // when in debugging mode, read from local file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "expected-results", "/home/ruiz.richard.p/queries-expected-results.txt" ).toString
      rawExpectedResultsList = loadPlainTextFromLocalFile( path )

      for ( line <- rawExpectedResultsList ) {
        parseAndAppendPairToExpectedResults( line )
      }
    } else {

      // ...else read it from jar when in batch/production mode, and default to both
      rawExpectedResultsList = loadPlainTextFromJarFile( "/queries-expected-results.txt" )

      for ( line <- rawExpectedResultsList ) {
        parseAndAppendPairToExpectedResults( line )
      }
    }
  }

  /**
    * Helper method for loadExpectedResults
    * @param line
    */
  private def parseAndAppendPairToExpectedResults( line: String ): Unit = {

    val trimmedLine = line.trim

    // skip commented or empty lines
    if ( isNotComment( trimmedLine ) ) {

      // trim and split line
      val pair = trimmedLine.split( " = " )
      if ( pair.length == 2 ) {
        expectedResultsMap += pair(0) -> pair(1)
      } else {
        // allow expected values to be zero-len strings
        expectedResultsMap += pair(0) -> ""
      }
      if ( verbose ) println(s"Expected results pair [$trimmedLine]" )
    }
  }
  /**
    * Loads queries from local path or resource/jar, according to runtime args supplied.
    * Also creates two separate lists: 1) All queries and 2) Only median queries
    */
  private def loadQueryMaps(): Unit = {

    if ( verbose ) printBanner( "loadQueryMaps" )
    var jsonRaw = ""

    // when in debugging mode, just read from file system
    if ( debugging ) {

      val path = argsMap.getOrElse( "queries", "/home/ruiz.richard.p/queries.json" ).toString

      // read it all  at one go, from: https://stackoverflow.com/questions/1284423/read-entire-file-in-scala
      val source = Source.fromFile( path )
      jsonRaw = try source.mkString finally source.close()

    } else {

      // ...otherwise, get it from jar file
      val is = getClass.getResourceAsStream( "/queries.json" )
      val isr = new InputStreamReader( is )
      val br = new BufferedReader( isr )

      var queryList = ListBuffer[ String ]()

      try {

        var line = ""
        while( { line = br.readLine(); line!= null } ) {
          queryList += line
        }
      } catch {

        case ex: Exception => stopWithMessage( s"ERROR reading queries from jar [$ex.toString]" )

      } finally {

        br.close
        isr.close
        is.close
      }
      jsonRaw = queryList.mkString
    }
    // now that it's loaded, iterate it
    // Parse JSON in one shot, from: https://stackoverflow.com/questions/32716609/get-json-values-in-scala-scala-util-parsing-json-json
    val queries = JSON.parseFull( jsonRaw ).get.asInstanceOf[ List[ scala.collection.immutable.Map[ String, String ] ] ]

    // reset static list before reloading and appending
    staticRunList = ListBuffer[ String ]()
    staticNationalMediansList = ListBuffer[ String ]()

    for ( query <- queries ) {

      // load map w/ key = id and ( type, query ) tuple
      rawQueryMap += query( "id" ) -> ( query( "type" ), query( "query" ) )

      // build static run list to force static depencency resolution. This is important for division calcs: G05 = G05n / F04
      staticRunList += query( "id" )

      // TODO/KLUDGE: build national medians list too
      if ( query( "type" ) == QUERY_MEDIAN ) {
        staticNationalMediansList += query( "id" )
      }

      if ( verbose ) println( s"""${query( "id" )} -> [${query( "type" )}, ${query( "query" )}]""" )
    }
    if ( debugging ) {

      // iterate and dump national medians list
      printBanner("National medians list")
      for (id <- staticNationalMediansList) {
        println(s"Query [$id] in national medians list")
      }
    }
  }

  /**
    * Loads cohorts & measures tables, either cached from s3 (faster) or live (slower) via JDBC
    */
  private def loadCohortsAndMeasures(): Unit = {

    if ( debugging ) printBanner( "loadCohortsAndMeasures" )

    // load config params from args map
    val useCachedCohorts = argsMap.getOrElse( "use-cached-cohorts", "false" ).toString.toBoolean

    if ( useCachedCohorts ) {

      val cohortsS3 = argsMap( "cohorts" ).toString
      val measuresS3 = argsMap( "measures" ).toString

      val cohortsDf = spark.read.format( "csv" ).option( "header", "true" ).load( cohortsS3 ).cache()
      val measuresDf = spark.read.format( "csv" ).option( "header", "true" ).load( measuresS3 ).cache()

      cohortsDf.createOrReplaceTempView( "cohorts" )
      spark.table( "cohorts" ).cache

      measuresDf.createOrReplaceTempView( "measures" )
      spark.table( "measures" ).cache

    } else {

      // TODO: Add build/runtime differentiation for: dev/test/stage/prod
      var propFileName = "dev-application.properties"
      val properties = new Properties()
      properties.load( getClass.getClassLoader.getResourceAsStream( propFileName ) )

      var jdbcUsername = properties.getProperty( "jdbcUsername" )
      var jdbcPassword = properties.getProperty( "jdbcPassword" )
      val jdbcHostname = properties.getProperty( "jdbcHostname" )
      val jdbcPort     = properties.getProperty( "jdbcPort" )
      val jdbcDatabase = properties.getProperty( "jdbcDatabase" )

      jdbcUsername = getPlainText( jdbcUsername, Collections.singletonMap( "Name", "RDSUser" ) )
      jdbcPassword = getPlainText( jdbcPassword, Collections.singletonMap( "Name", "RDSPassword" ) )

      val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
      val connectionProperties = new java.util.Properties()

      //load the MySQL driver
      Class.forName( "com.mysql.jdbc.Driver" )

      val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      val cohorts = spark.sqlContext.read.jdbc(jdbcUrl, "PRS_Report_Cohorts", connectionProperties )
      val measures = spark.sqlContext.read.jdbc(jdbcUrl, "PRS_Report_Measures", connectionProperties )

      cohorts.createOrReplaceTempView( "cohorts" )
      spark.table( "cohorts" ).cache

      measures.createOrReplaceTempView( "measures" )
      spark.table( "measures" ).cache
    }
  }

  /**
    * Extracts periods from measures and cohorts and then refactors column names & date/time strings
    */
  private def extractPeriods(): Unit = {

    val quarterId = argsMap.getOrElse( "quarter-id", QUARTER_ID ).toString

    // now that raw data is loaded, query it for periods
    var periodsDs = spark.sql( s"""SELECT "Current" as Period, m.Measure, c.CoveredDataStartDate, c.CoveredDataEndDate, m.MeasureID as Sequence
                                  |FROM measures m
                                  |LEFT JOIN cohorts c on m.MeasureID=c.MeasureID
                                  |WHERE m.ReportTypeID = 1
                                  |AND c.QuarterID = $quarterId
                                  |AND c.IsActive = 1
                                  |UNION ALL
                                  |SELECT "Previous", m.Measure, c.CoveredDataStartDate, c.CoveredDataEndDate, m.MeasureID + 12
                                  |FROM measures m
                                  |LEFT JOIN cohorts c on m.MeasureID = c.MeasureID
                                  |WHERE m.ReportTypeID = 1
                                  |AND c.QuarterID = $quarterId - 1
                                  |AND c.IsActive = 1""".stripMargin )

    // From: https://stackoverflow.com/questions/29383107/how-to-change-column-types-in-spark-sqls-dataframe
    periodsDs = periodsDs.selectExpr( "Period", "Measure", "CoveredDataStartDate", "CoveredDataEndDate", "cast( Sequence as int ) Sequence" ).sort( "Sequence" )

    // transform names
    val colNames = Seq( "period", "measure", "start", "end", "sequence" )
    periodsDs = periodsDs.toDF( colNames: _* )

    // whittle down the date/time strings to just date
    periodsDs = periodsDs.withColumn( "start", split( col( "start" ), " " ).getItem( 0 ) )
    periodsDs = periodsDs.withColumn(   "end", split( col(   "end" ), " " ).getItem( 0 ) )

    periodsDs.createOrReplaceTempView( "periods" )
    spark.table( "periods" ).cache

    if ( verbose ) {

      periodsDs.printSchema()
      periodsDs.show( 24 )
    }
  }

  /**
    * Gets dates for substitutions and stashes them in the abbrevations map.  Assumes that abbrevations have already been loaded.
    */
  // TODO: Make this generic for the next report generated using this framework
  private def extractStartAndEndDates(): Unit = {

    // default to r4q
    val window = argsMap.getOrElse( "report-window", WINDOW_RQ4 ).toString

    val queryAbc    = """SELECT start, end FROM periods WHERE measure = "Number Served (Reportable Individual)"     AND period = "Current" """
    val queryExit2q = """SELECT start, end FROM periods WHERE measure = "Employment Rate Second Quarter After Exit" AND period = "Current" """
    val queryExit4q = """SELECT start, end FROM periods WHERE measure = "Employment Rate Fourth Quarter After Exit" AND period = "Current" """

    // fetch start and end date strings
    val ( begAbc, endAbc )       = getBeginAndEndDates( window, spark.sql( queryAbc    ).cache )
    val ( begExit2q, endExit2q ) = getBeginAndEndDates( window, spark.sql( queryExit2q ).cache )
    val ( begExit4q, endExit4q ) = getBeginAndEndDates( window, spark.sql( queryExit4q ).cache )

    // add date pairs to list
    abbreviationsList += ( s"_begAbc_ = to_date( '$begAbc' )" )
    abbreviationsList += ( s"_endAbc_ = to_date( '$endAbc' )" )

    abbreviationsList += ( s"_begExit2q_ = to_date( '$begExit2q' )" )
    abbreviationsList += ( s"_endExit2q_ = to_date( '$endExit2q' )" )

    abbreviationsList += ( s"_begExit4q_ = to_date( '$begExit4q' )" )
    abbreviationsList += ( s"_endExit4q_ = to_date( '$endExit4q' )" )

    if ( debugging ) printAbbreviationsList()
  }

  /**
    * Helper method for extractStartAndEndDates
    * @param df
    * @return
    */
  private def getBeginAndEndDates( window: String, df: DataFrame ): ( String, String ) = {

    // current quarter requires a bit of calculation before returning values
    if ( window == WINDOW_CQ ) {

      // get end date as LocalDate
      val date = LocalDate.parse(df.head().getString(1), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      // add one day, taking us to the 1st day of the next month, and *then* subtract 3 months
      val startDate = date.plusDays(1).minusMonths(3).toString

      ( startDate,                df.head().getString( 1 ) )

    } else if ( window == WINDOW_PTD ) {

      // ptd has a non-negotiable begin date
      ( "2016-07-01",             df.head().getString( 1 ) )

    } else {

      // default to WINDOW_RQ4
      ( df.head().getString( 0 ), df.head().getString( 1 ) )
    }
  }
  /**
    * Helper method for loadCohortsAndMeasures.  Pulled verbatim from Shiva's QPR code
    */
  private def getPlainText( cipherText: String, encContext: java.util.Map[ String, String ] ) = {

    val ciphertextBlob = ByteBuffer.wrap( Base64.getDecoder.decode( cipherText.getBytes ) )
    val req = new DecryptRequest().withCiphertextBlob( ciphertextBlob ).withEncryptionContext( encContext )
    val kms = new AWSKMSClient()
    kms.setRegion( Region.getRegion( Regions.US_WEST_2 ) )
    val plainText = kms.decrypt( req ).getPlaintext
    new String( plainText.array )
  }

  /**
    * Loads raw csv, and then whittles it down to a subset of columns referenced in the specs spreadsheet.
    * List of colsToKeep is compiled in Zeppelin worksheet "PIRL Extractor"
    */
  private def loadAndPrepareInput(): Unit = {

    if ( debugging ) printBanner( "loadAndPrepareInput" )

    val inputPath         = argsMap.getOrElse( "input", "" ).toString
    val inputPreprocessed = argsMap.getOrElse( "input-preprocessed", "false" ) == "true"

    if ( inputPath != "" ) {

      if ( inputPreprocessed && inputPath.endsWith( ".parquet" ) ) {

        loadPreparedParquetInput( inputPath )

      } else if ( inputPreprocessed && inputPath.endsWith( ".csv" ) ) {

        loadPreparedCsvInput( inputPath, dateColumnNamesList )

      } else if ( inputPath.endsWith( ".parquet" ) ) {

        loadAndPrepareParquetInput( inputPath, dateColumnNamesList )

      } else  {

        // Defaults to CSV
        loadAndPrepareCsvInput( inputPath, dateColumnNamesList )
      }
      // default to loading subsets dynamically
      if ( argsMap.getOrElse( "load-subsets-dynamically", "true" ) == "true" ) {

        val colName = argsMap.getOrElse( "subsets-name", "p3000" ).toString
        loadDynamicQuerySubsets( colName )
      }

    } else {

      stopWithMessage( s"Input path is empty.  Use: 'input=s3://bucket-blah-blah/foo'" )
    }
  }

  // TODO: Make this generic like date processing is
  private def adHocCastToFloat( df: Dataset[ Row ] ): Dataset[ Row ] = {

    df.withColumn( "p1704", df( "p1704" ).cast( "float" ) )
  }
  /**
    * Helper method for loadAndPrepareInput
    */
  private def loadPreparedParquetInput( inputPath: String ): Unit = {

    val startMillis = System.currentTimeMillis()
    var preprocessedParquetDf = spark.read.parquet( inputPath )

    // TODO: This is too ad-hoc.  You can do better!
    preprocessedParquetDf = adHocCastToFloat( preprocessedParquetDf )

    preprocessedParquetDf.createOrReplaceTempView( "input" )
    spark.table( "input" ).cache

    if (debugging && verbose) {

      printSchema( preprocessedParquetDf, "Preprocessed input schema (.parquet)" )

      val seconds = (System.currentTimeMillis() - startMillis) / 1000
      println(s"Time to load preprocessed parquet input records: [$seconds]s" )
    }
  }

  /**
    * Helper method for loadAndPrepareInput
    */
  private def loadPreparedCsvInput( inputPath: String, dateColumnNames: ListBuffer[ String ] ): Unit = {

    val startMillis = System.currentTimeMillis()
    var inputRawReducedRenamedDf = spark.read.format( "csv" ).option( "header", "true" ).load(inputPath)

    // iterate and update date cols from strings to date objs
    for (dateColumnName <- dateColumnNames) {
      // this only promotes string values ('2018-06-13') to date objects, it doesn't reformat them.
      inputRawReducedRenamedDf = inputRawReducedRenamedDf.withColumn(dateColumnName, to_date(col(dateColumnName), "yyyy-MM-dd" ) )
    }
    // TODO: This is too ad-hoc.  You can do better!
    inputRawReducedRenamedDf = adHocCastToFloat( inputRawReducedRenamedDf )

    inputRawReducedRenamedDf.createOrReplaceTempView( "input" )
    spark.table( "input" ).cache

    if ( debugging && verbose ) {

      printSchema( inputRawReducedRenamedDf, "Preprocessed input schema (.csv)" )

      val seconds = (System.currentTimeMillis() - startMillis) / 1000
      println(s"Time to load preprocessed cvs input records: [$seconds]s" )
    }
  }

  /**
    * Helper method for loadAndPrepareInput: load production .csv file, and whittle it down to manageable size (drops ~275 columns)
    */
  private def loadAndPrepareCsvInput( inputPath: String, dateColumnNames: ListBuffer[ String ] ): Unit = {

    val startMillis = System.currentTimeMillis()
    var inputRawDf = spark.read.format( "csv" ).option( "header", "false" ).load(inputPath)

    // stash it
    inputRawDf.createOrReplaceTempView( "input" )

    // production data will not have headers, so build up list of cols to keep
    // TODO: Make this generic, not report specific!
    val headerlessQuery = s"""
                      SELECT _c12 as PIRL202, _c27 AS PIRL301, _c44 AS PIRL401, _c55 AS PIRL413, _c72 AS PIRL808, _c73 AS PIRL900,
                      _c74 AS PIRL901, _c76 AS PIRL904, _c89 AS PIRL918, _c94 AS PIRL923, _c106 AS PIRL1000, _c107 AS PIRL1001,
                      _c108 AS PIRL1002, _c113 AS PIRL1007, _c114 AS PIRL1100, _c115 AS PIRL1101, _c116 AS PIRL1102, _c117 AS PIRL1103,
                      _c118 AS PIRL1104, _c119 AS PIRL1105, _c121 AS PIRL1107, _c126 AS PIRL1112, _c127 AS PIRL1113, _c132 AS PIRL1201,
                      _c193 AS PIRL1602, _c197 AS PIRL1606, _c213 AS PIRL1704, _c263 AS PIRL3000 FROM input"""

    inputRawDf = spark.sql( headerlessQuery )
    if ( debugging ) printSchema( inputRawDf, "Headerless dataframe after SELECT statement" )

    //val inputRawReducedDf = inputRawDf.select( columnsToKeepList.map( col ): _* )

    // update col names: carve 'PIRL' down to 'p'
    val updatedColNames = columnsToKeepList.map( name => name.replace("PIRL", "p" ) )

    // create new df w/ new names
    var inputRawReducedRenamedDf = inputRawDf.toDF( updatedColNames: _* )

    // TODO: Find out what's up w/ "PIRL 1007", it's missing in dev dataset and referenced in the cell specs!
    // create a stand-in column for now.
    if ( argsMap.getOrElse( "do-p1007-kludge", "false" ) == "true" ) {
      inputRawReducedRenamedDf = inputRawReducedRenamedDf.withColumn( "p1007", col( "p1000" ) )
    }

    // iterate and update date cols from strings to date objs
    if (debugging) printBanner("= Converting strings to dates..." )
    val colsInInputDf = inputRawReducedRenamedDf.columns
    for ( dateColumnName <- dateColumnNames ) {

      if (debugging) println(s"= updating dateColumnName [$dateColumnName] to date object")
      // make sure cols in list of date cols are actually present before operating on them!
      if ( colsInInputDf.contains( dateColumnName ) ) {
        inputRawReducedRenamedDf = inputRawReducedRenamedDf.withColumn( dateColumnName, to_date(col(dateColumnName), "yyyyMMdd" ) )
      } else {
        if (debugging) println(s"= dateColumnName [$dateColumnName] missing from input DF")
      }
    }

    // TODO: This is too ad-hoc.  You can do better!
    inputRawReducedRenamedDf = adHocCastToFloat( inputRawReducedRenamedDf )

    // Do conditional repartitioning of massive CSV file (only after paring it down)
    val actualPartitions = inputRawReducedRenamedDf.rdd.partitions.size
    val requestedPartitions = argsMap.getOrElse( PARTITIONS, PARTITIONS_DEFAULT ).toString.toInt

    if ( requestedPartitions != actualPartitions ) {

      inputRawReducedRenamedDf = inputRawReducedRenamedDf.repartition( requestedPartitions )
    }
    // stash and cache
    inputRawReducedRenamedDf.createOrReplaceTempView("input")
    spark.table("input").cache

    if (debugging) {

      printSchema( inputRawReducedRenamedDf, "Dynamically processed input schema")

      val seconds = (System.currentTimeMillis() - startMillis) / 1000
      println(s"Time to load and whittle down production input records from CSV: [$seconds]s")
    }
  }

  /**
    * Helper method for loadAndPrepareInput: load production .parquet file, and whittle it down to manageable size (drops ~275 columns)
    * @param inputPath
    * @param dateColumnNames
    */
  private def loadAndPrepareParquetInput( inputPath: String, dateColumnNames: ListBuffer[ String ] ): Unit = {

    val startMillis = System.currentTimeMillis()
    var inputRawDf = spark.read.parquet( inputPath )//.option( "header", "false" )

    // stash it
    inputRawDf.createOrReplaceTempView( "input" )

    // production data will not have headers, so build up list of cols to keep
    // TODO: Make this generic, not report specific!
    val headerlessQuery = s"""
                      SELECT _c12 as PIRL202, _c27 AS PIRL301, _c44 AS PIRL401, _c55 AS PIRL413, _c72 AS PIRL808, _c73 AS PIRL900,
                      _c74 AS PIRL901, _c76 AS PIRL904, _c89 AS PIRL918, _c94 AS PIRL923, _c106 AS PIRL1000, _c107 AS PIRL1001,
                      _c108 AS PIRL1002, _c113 AS PIRL1007, _c114 AS PIRL1100, _c115 AS PIRL1101, _c116 AS PIRL1102, _c117 AS PIRL1103,
                      _c118 AS PIRL1104, _c119 AS PIRL1105, _c121 AS PIRL1107, _c126 AS PIRL1112, _c127 AS PIRL1113, _c132 AS PIRL1201,
                      _c193 AS PIRL1602, _c197 AS PIRL1606, _c213 AS PIRL1704, _c263 AS PIRL3000 FROM input"""

    inputRawDf = spark.sql( headerlessQuery )
    if ( debugging ) printSchema( inputRawDf, "Headerless dataframe after SELECT statement" )

    // update col names: carve 'PIRL' down to 'p'
    val updatedColNames = columnsToKeepList.map( name => name.replace("PIRL", "p" ) )

    // create new df w/ new names
    var inputRawReducedRenamedDf = inputRawDf.toDF( updatedColNames: _* )

    // iterate and update date cols from strings to date objs
    if (debugging) printBanner("= Converting strings to dates..." )
    val colsInInputDf = inputRawReducedRenamedDf.columns
    for ( dateColumnName <- dateColumnNames ) {

      if (debugging) println(s"= updating dateColumnName [$dateColumnName] to date object")
      // make sure cols in list of date cols are actually present before operating on them!
      if ( colsInInputDf.contains( dateColumnName ) ) {
        inputRawReducedRenamedDf = inputRawReducedRenamedDf.withColumn( dateColumnName, to_date(col(dateColumnName), "yyyyMMdd" ) )
      } else {
        if (debugging) println(s"= dateColumnName [$dateColumnName] missing from input DF")
      }
    }

    // TODO: This is too ad-hoc.  You can do better!
    inputRawReducedRenamedDf = adHocCastToFloat( inputRawReducedRenamedDf )

    // Do conditional repartitioning of massive CSV file (only after paring it down)
    val actualPartitions = inputRawReducedRenamedDf.rdd.partitions.size
    val requestedPartitions = argsMap.getOrElse( PARTITIONS, PARTITIONS_DEFAULT ).toString.toInt

    if ( requestedPartitions != actualPartitions ) {

      inputRawReducedRenamedDf = inputRawReducedRenamedDf.repartition( requestedPartitions )
    }
    // stash and cache
    inputRawReducedRenamedDf.createOrReplaceTempView("input")
    spark.table("input").cache

    if (debugging) {

      printSchema( inputRawReducedRenamedDf, "Dynamically processed input schema" )

      val seconds = (System.currentTimeMillis() - startMillis) / 1000
      println(s"Time to load and whittle down production input records from parquet file: [$seconds]s")
    }
  }
  /**
    * One liner to dynamically build subsets list
    */
  private def loadDynamicQuerySubsets(colName: String ): Unit = {

    // TODO/KLUDGE: parquet 128 has 'PIRL 3000' leakage into state/subsets list.  Dunno why it's leaking, but using
    // 'LENGTH( colName ) < 3' filters it out.
    subsetsList = spark.sql( s"SELECT DISTINCT $colName FROM input WHERE LENGTH( $colName ) < 3 ORDER BY $colName" ).rdd.map( row => row( 0 ).asInstanceOf[ String ] ).collect()
  }
  /**
    * Helper method for loadRawInput. Dumps schema
    */
  private def printSchema( df: DataFrame, msg: String ) = {

    printBanner( msg )
    df.printSchema
  }

  /**------------------------------------------------------------------------------------------------------------------*/
  /** START interactive block */
  def startInteractiveMode(): Unit = {

    clearConsole()

    if ( debugging ) {
      val seconds = (System.currentTimeMillis() - startMillis) / 1000.0
      println( f"Initialization of [$appName] complete in [$seconds%.1f] seconds.\n\n" )
    }

    var runList = Array[ String ]()

    val prompt =
      s"""\n\n============================================================================================
         |= Interactive mode choices: Use lower case for mode
         |============================================================================================
         | [i] init: executes init()
         | [r]  run: executes run()
         | [s] stop: executes stop() NOTE: executes stop(...) method using last defined runList
         | [l] (re)load query defs, abbreviations and expected results
         |
         | [sql] Multiline input, hit ENTER twice to execute
         | [r=id,id,id] run comma delimited list of ids. Id case matters!
         | [dr] run batch w/o actually doing queries
         | [pa] print abbrevations to console
         | [pr] print resultsMap to console
         | [pe] print expected results to console
         | [pp] print partition size to console
         | [pc] print current configuration
         | [pq] print query strings
         | [ps] print subsets list
         |
         | [rr] reset results map
         | [rs] reset stats counters
         |
         | [c] compare actual with expected results
         | [u] update configuration: name=value
         |
         | [q] quit cold\n\n
     """.stripMargin

    val hitEnter =
      s"""\n\n============================================================================================
         |= Hit ENTER to continue
         |============================================================================================
       """.stripMargin

    // Loop indefinitely until stop or exit is chosen
    while ( true ) {

      // clean input up and split on =
      val inputs = StdIn.readLine( prompt ).trim.split( "=" )

      inputs( 0 ) match {

        case "i"   => init
        case "l"   => loadConfigurationData()
        case "r"   => {

          val startMillis = System.currentTimeMillis()

          // is this 'r' by itself?
          if ( inputs.length == 1 ) {
            runList = getRunList()
          } else {
            // ...or with cell id arguments?
            runList = inputs( 1 ).trim.split( "," )
          }

          // Run both?
          runCalculationsConditionally( runList )

          if ( debugging ) {
            val seconds = ( System.currentTimeMillis() - startMillis ) / 1000.0
            val minutes = seconds / 60.0
            println( s"Time to run query(ies) [$seconds]s -or- [$minutes]m" )
          }
        }
        case "dr"  => {

          // cache current dryRun state
          val previousDryRunValue = dryRun
          dryRun = true

          // reset query map
          generatedQueriesMap = Map[ String, String ]()

          // runs whatever value for run-type we started with
          runCalculationsConditionally( getRunList() )

          // reset to prior value
          dryRun = previousDryRunValue

          // print out dryRunMap
          printDryRunMap
        }
        case "pa"  => printAbbreviationsList
        case "pr"  => printResultsMap
        case "pe"  => printExpectedResults
        case "pp"  => printPartitionSize
        case "pc"  => printConfiguration
        case "ps"  => printSubsetsList
        case "pq"  => printQueries
        case "rr"  => resetResultsMap
        case "rs"  => resetCounters
        case "c"   => processResults
        case "sql" => getAndRunMultilineSql
        case "u"   => updateConfiguration
        case "s"   => { stop(); System.exit( 1 ) }
        case "q"   => stopWithMessage( "Quitting cold" )
        case _     => println( "Unrecognized input.  Try again" )
      }

      // force a pause until ENTER key is hit...
      StdIn.readLine( hitEnter )
      clearConsole()
    }
  }

  /**
    * Wraps calls to both aggregation types so we can run both in one session
    */
  private def runCalculationsConditionally(runList: Array[ String ] ): Unit = {

    // Run both? Defaults to false
    if ( argsMap.getOrElse( "run-both-aggregations", "false" ) == "true" ) {

      printBanner("Run Both: Subsets")
      argsMap += "aggregate-by" -> SUBSETS
      run(runList)

      printBanner("Run Both: National")
      argsMap += "aggregate-by" -> NATIONAL
      run(runList)

    } else {

      // run according to passed in values: 'national'|'subsets'
      run( runList )

      if ( argsMap.getOrElse( "do-national-medians", "" ) == "true" ) {

        runNationalMedians( staticNationalMediansList.toArray )
      }
    }
  }

  /**
    * Helper method for interactiveMode
    */
  private def loadConfigurationData(): Unit = {

    loadQueryAbbreviations()
    loadQueryMaps()
    loadExpectedResults()
    loadColumnsToKeep()
    loadDateColumns()

    // where do we load subsets from?
    // If we're loading static subsets, then do it now.  If we're doing dynamic, do it *AFTER* the 'input' table has been loaded and cached
    if ( argsMap.getOrElse( "load-subsets-dynamically", "true" ) != "true" ) {
      loadStaticQuerySubsets()
    }
  }
  /**
    * Helper method for startInteractiveMode
    */
  private def clearConsole(): Unit = {

    // From: https://stackoverflow.com/questions/26317091/clear-in-scala-call-cls-in-scala-program-running-in-cmd
    "clear" !
  }

  /**
    * Helper method for startInteractiveMode
    */
  private def resetResultsMap(): Unit = {
    resultsMap = Map[ String, Any ]().withDefaultValue( "" )
    divisorsMap = Map[ String, ( String, String ) ]().withDefaultValue( ( "", "" ) )
  }

  /**
    * Helper method for startInteractiveMode
    */
  private def resetCounters(): Unit = {

    medianRunTime = 0L
    runCountRuleCounter = 0
    appendCounter = 0
  }

//  /**
//    * Helper method for startInteractiveMode
//    */
//  private def printCounters(): Unit = {
//
//    println( s"Stats: runCountRuleCounter: [$runCountRuleCounter], appendCounter: [$appendCounter]" )//, updateCounter: [$updateCounter]" )
//  }


  /**
    * Helper method for startInteractiveMode
    */
  private def printPartitionSize(): Unit = {

    val count = spark.table( "input" ).rdd.partitions.size
    println( s"Input data partitions [$count]" )
  }
  /**
    * Helper method for startInteractiveMode
    */
  private def printConfiguration(): Unit = {

    printBanner( "Current configuration" )

    for ( key <- argsMap.keys.toArray.sorted ) {
      println( s"key [$key] = [${argsMap( key )}]" )
    }
  }
  /**
    * Helper method for startInteractiveMode
    */
  private def updateConfiguration(): Unit = {

    // get name=value pair
    val pair = StdIn.readLine( "name=value: " ).trim.split( "=" )

    // update argsMap
    if ( pair.length == 2 ) {

      println( s"Updating [${pair( 0 )}=${pair( 1 )}]" )
      argsMap += pair(0) -> pair(1)

      // always update globals
      updateGlobalFlags()

    } else {
      println( s"Invalid format.  Must be 'name=value' w/ no spaces" )
    }
  }
  /**
    * Helper method for startInteractiveMode: Does what it says!
    */
  private def getAndRunMultilineSql(): Unit = {

    // concept of accumulator from: https://stackoverflow.com/questions/5055349/how-to-take-input-from-a-user-in-scala
    var line = "F00!"
    var accumulatedInput = ""
    var lineCounter = 0

    while ( line != "" ) {
      line = StdIn.readLine()
      accumulatedInput += ( line + " " )
      lineCounter += 1
    }
    lineCounter -= 1
    accumulatedInput = accumulatedInput.trim
    if ( debugging ) println( s"You input [$lineCounter] lines of SQL [$accumulatedInput]" )

    var startMillis = System.currentTimeMillis()

    try {

      val maxRows = argsMap.getOrElse( "sql-max-rows", "10" ).toString.toInt
      val resultsDf = spark.sql( accumulatedInput )
      println( "\n\n====================================================================" )
      resultsDf.show( maxRows )

    } catch {

      case ex: Exception => {
        printBanner( "ERROR" )
        println( "\n\n" + ex.printStackTrace() )
      }
    }
    if ( debugging ) {

      val millis = System.currentTimeMillis() - startMillis
      val seconds = millis / 1000.0
      println( s"Time to run query [$millis]ms --or-- [$seconds]s" )
      println( s"Query [$accumulatedInput]" )
    }
  }

  /**------------------------------------------------------------------------------------------------------------------*/
  /** START run block */

  /**
    * Wrapper for run( runList )
    */
  def run(): Unit = {

    runCalculationsConditionally( getRunList() )
  }

  /**
    * Iterates runList of cellIds and executes them
    * @param runList
    */
  private def run( runList: Array[ String ] ): Unit = {

    if ( debugging ) printBanner( "run() called..." )

    // iterate the keys list and call appropriate wrapper
    for ( id <- runList ) {

      // check to see if key=query pair exists (users can enter invalid cellIds when in interactive mode
      if ( rawQueryMap.contains( id ) ) {

        val queryObj = rawQueryMap(id)
        val queryType = queryObj._1
        val query = queryObj._2.toString

        queryType match {

          case QUERY_COUNT          => runCountWrapper( query, id )
          case QUERY_MEDIAN         => runMedianWrapper( query, id )
          case FORMULA_DIVISION     => runDivisionFormulaWrapper( query, id )
          case FORMULA_CONCATENATOR => runConcatentorFormulaWrapper( query, id )
          case _                    => stopWithMessage( "run() ELSE queryType encountered" )
        }

      } else {

        // no key found, log it
        updateResultsMap( id,"MISSING" )
      }
    }
  }

  /**
    * Kludgey method for running national medians when in subsets mode.  Artifact of desigining engine to create two
    * types of report: 1) National and 2) Subsets.  Once requirement changed to do both in one run I switched to getting
    * all national counts as cummulative rollup byproduct of iterating results for subsets.
    *
    * That approach words for *ALL BUT* national medians, hence this kludge
    * @param runList
    */
  private def runNationalMedians( runList: Array[ String ] ): Unit = {

    if ( debugging ) printBanner( "runNationalMedians() called..." )

    // iterate the keys list and call appropriate wrapper
    for ( id <- runList ) {


      val queryObj = rawQueryMap(id)
      val query = queryObj._2.toString

      updateResultsMap( id, runMedianCountRule( query, id ) )
    }
  }

  // allows calling objec to get count of records in dataset
  def getInputCount(): Long = {
    return spark.sql( "SELECT count( * ) AS count FROM input" ).first()(0).asInstanceOf[Long]
  }

  /**
    * Allows calling object to get result of query count * subsets
    * @return
    */
  def getCellCalcsCount(): Int = {

    return rawQueryMap.size * subsetsList.length
  }
  /**
    * Helper method for run().  Handled all the kludgey bits associated with the scala.runtime.BoxedUnit resultmap
    * corruption
    */
  private def updateResultsMap( id: String, value: String ): Unit = {
    resultsMap += id -> value
  }
  /**
    * Helper method for run().  Stores numerator and denominator in a tuple
    */
  private def updateDivisorsMap( id: String, value: ( String, String ) ): Unit = {
    divisorsMap += id -> value
  }

  /**
    * Helper method for run().
    */
  private def runCountWrapper(query: String, id: String ): Unit = {

    if ( debugging && verbose ) println( s"runCountWrapper called raw query [$query]" )
    try {

      // default aggregation type is 'national'
      if ( argsMap.getOrElse( "aggregate-by", SUBSETS ) == NATIONAL ) {

        val count = runCountRule( query, id )
        updateResultsMap( id, count.toString )

      } else if ( argsMap.getOrElse( "aggregate-by", SUBSETS ) == SUBSETS ) {

        // build group by query
        // 1) Swap prefixes: replace count w/ group by setup, and attach group by conditions suffix
        var groupByQuery = query.replaceAll( "_scfIw_", "_sgbcfIw_" ) + " _gbCs_"

        // 2) Replace other placeholders
        groupByQuery = replaceAbbrevations( groupByQuery )

        // keep a copy of the query for reporting purposes:  once per group in the 1st subset's compound key
        val compoundKey = id + "-" + subsetsList( 0 )
        if ( debugging ) println( s"Writing groupByQuery to compoundKey [$compoundKey] [$groupByQuery]" )
        generatedQueriesMap += compoundKey -> groupByQuery

        if ( !dryRun ) {

          // run query
          val countsDf = spark.sql( groupByQuery ).cache()
          // collect and parse it.  stash results
          collectCountsAndUpdateResultsMap( countsDf, id )
        }

      } else {

        stopWithMessage( "Unknown aggregation type. Use aggregate-by={'national'|'subsets'}.  Default is 'subsets'" )
      }
    } catch {

      case ex: Exception => {
        updateResultsMap( id, "FAILED" )
        if ( debugging ) println( s"Count query [$id] failed [${ex.toString}]" )
      }
    }
  }


  /**
    * Helper for runCountWrapper.  Collects group by query in one shot, and then parses and stashes it in the resultMap
    * @param countsDf
    * @param id
    */
  private def collectCountsAndUpdateResultsMap( countsDf: DataFrame, id: String ): Unit = {

    // collect into local list of two element arrays
    val subsetCounts = countsDf.select( "subset", "count" ).collect.toList
    var subset = ""
    var count  = ""
    var tempCountsMap = Map[ String, String ]()

    // iterate local list and populate map w/ subsets -> count entries
    for ( row <- 0 to subsetCounts.length - 1 ) {

      subset = subsetCounts( row )( 0 ).toString
      count = subsetCounts( row )( 1 ).toString

      tempCountsMap += ( subset -> count )
    }
    // Now, iterate states list, query map for value, and update results cache
    // Also: Track rollup total, obviating need for national iteration.  Should save ~20% at runtime
    // TODO: Keep or toss?
    var rollupCount = 0
    for ( subset <- subsetsList ) {

      count = tempCountsMap.getOrElse( subset, "0" )
      rollupCount += count.toInt

      // subsets queries get compound ids, e.g.: 'F04-MD'
      updateResultsMap( id + "-" + subset, count )
    }
    // update count for "ALL" states
    updateResultsMap( id, rollupCount.toString )
  }

  /**
    * Helper method for run().  Writes calcs to resultsMap
    */
  private def runMedianWrapper( query: String, id: String ): Unit = {

    if ( debugging && verbose && !dryRun ) println( s"Running MEDIAN query, id [$id]" )
    try {

      if ( argsMap.getOrElse( "aggregate-by", NATIONAL ) == NATIONAL ) {

        updateResultsMap( id, runMedianCountRule( query, id ) )

      } else if ( argsMap.getOrElse( "aggregate-by", "" ) == SUBSETS ) {

        // TODO: Remove this switch!
        //if ( argsMap.getOrElse( "skip-medians", "" ) != "true" ) {
          runMedianSubsetsRule(query, id)
        //}
      } else {

        stopWithMessage( "Unknown aggregation type. Use aggregate-by={'national'|'subsets'}.  Default is 'national'" )
      }

    } catch {
      case ex: Exception => {
        updateResultsMap( id, "FAILED" )
        if ( debugging ) println( s"Median query [$id] failed [{$ex.toString}]" )
      }
    }
  }

  /**
    * Runs median calculation for 'national' type aggregations
    * @param query
    * @param id
    * @return
    */
  private def runMedianCountRule(query: String, id: String ): String = {

    // NOTE: when no results are returned, then 'indefined' string is returned... As it should be!

    var result: Any = UNDEFINED
    var startMillis = 0L
    if ( debugging ) startMillis = System.currentTimeMillis()

    val updatedQueryString = replaceAbbrevations( query )
    generatedQueriesMap += id -> updatedQueryString

    // when in dry run mode, only output the SQL
    if ( !dryRun ) {

      val valuesDf = spark.sql( updatedQueryString )

      if (valuesDf.count > 0) {

        // this is faster by ~100ms
        val collectedFloats: Array[Float] = valuesDf.rdd.map(row => row.getFloat(0)).collect()
        // ...than this method recommended on: https://stackoverflow.com/questions/32000646/extract-column-values-of-dataframe-as-list-in-apache-spark#
        //val colledArray: Array[ String ] = rule.select( "values" ).map( r => r.getString( 0 ) ).collect

        result = getExactMedian( collectedFloats )

      } else {

        result = UNDEFINED
      }
      if (debugging) {
        val millis = System.currentTimeMillis() - startMillis
        medianRunTime += millis
        println(s"Time to sort and calculate median [$millis]ms")
      }
    }
    // done!
    result.toString
  }

  /**
    * Runs median calculation on a subset-by-subset basis.  Builds group by query by munging original count query
    * @param query
    * @param id
    */
  // NOTE: previous version attempted to collect *everything* and then iterate through it.  Ended up hanging w/ ~19M records after 1.5hrs
  private def runMedianSubsetsRule( query: String, id: String ): Unit = {

    var startMillis = 0L
    if ( debugging ) startMillis = System.currentTimeMillis()

    // 0 grab subsetName
    val subsetName = argsMap.getOrElse( "subsets-name", "p3000" ).toString

    // 1) Replace other placeholders
    var queryStem = replaceAbbrevations( query )
    if ( debugging && verbose ) printBanner( s"Medians query [$queryStem]" )

    // keep a copy of the query for reporting purposes
    generatedQueriesMap += id -> queryStem

    // run query and collect all medians...
    if ( !dryRun ) {

      for (subset <- subsetsList) {

        // TODO: *DO NOT* sort on workers ('ORDER BY'), it's easier to filter on workers, and then sort here, on driver
        // before calculating the median.  It's *MUCH* faster to sort locally: 11x times faster!
        val fullQuery = s"""$queryStem AND $subsetName = '$subset'"""// ORDER BY $sortByName"""
        if ( debugging ) println( s"median query for [$subset] [$fullQuery]" )

        // keep a copy of the query for reporting purposes
        generatedQueriesMap += ( id + "-" + subset ) -> fullQuery

        // get wage values as float for median calc
        val values = spark.sql( fullQuery ).rdd.map( row => row( 0 ).asInstanceOf[ Float ] ).collect.toSeq

        // calculate median
        var median: Any = UNDEFINED
        if ( values.length > 0 ) {
          median = getExactMedian( values )
        }
        if ( debugging ) println( s"median for [$subset] = [$median]" )
        updateResultsMap( id + "-" + subset, median.toString )
      }
      if (debugging) {

        val millis = System.currentTimeMillis() - startMillis
        medianRunTime += millis

        val seconds = millis / 1000.0
        val meanCalcTime = seconds / subsetsList.length
        println(s"Time to calculate [${subsetsList.length}] medians: [${seconds}]s mean s/subset [$meanCalcTime]")
      }
    }
  }

  /**
    * Given a sortWith-able sequence, finds the exact median. From: http://fruzenshtein.com/scala-median-funciton/
    * NOTE: Assumes length > 0.  Check for length *before* calling this method
    * @param seq
    * @return
    */
  // NOTE: WE WANT TO SORT HERE ON THE DRIVER, INSTEAD OF ON WORKERS.  It's ~11x faster
  private def getExactMedian( seq: Seq[ Float ] ): Float = {

    // Make sure it's sorted first
    val sortedSeq = seq.sortWith( _ < _ )

    // odd length, take the middle
    if ( seq.size % 2 == 1 ) {

      // will round down to int, e.g.: 7 / 2 = 3
      sortedSeq( sortedSeq.size / 2 )

      // even lengths
    } else {

      val ( left, right ) = sortedSeq.splitAt( seq.size / 2 )
      ( left.last + right.head ) / 2.0F
    }
  }

  /**
    * Helper method for run(). Writes calcs to resultsMap
    * Allows us to take results from one cell and divide it by another
    */
  private def runDivisionFormulaWrapper( query: String, id: String ): Unit = {

    // both cells must currently be defined and executed prior to arriving at this point
    val pair = query.trim.split( "/" )
    if ( debugging && verbose ) println( s"Args for id [$id] division: [${pair( 0 )}] / [${pair( 1 )}]" )

    var numId = pair( 0 ).toString
    var denId = pair( 1 ).toString

    // default aggregation type is 'national' states
    if ( argsMap.getOrElse( "aggregate-by", NATIONAL ) == NATIONAL ) {

      // stash formula for post-mortem
      generatedQueriesMap += id -> query

      runDivisionFormulaCount( numId, denId, id )

    } else if ( argsMap.getOrElse( "aggregate-by", "" ) == SUBSETS ) {

      var numIdSubset   = ""
      var denIdSubset   = ""
      var cellSubsetKey = ""

      for (subset <- subsetsList ) {

        numIdSubset   = numId + "-" + subset
        denIdSubset   = denId + "-" + subset
        cellSubsetKey =    id + "-" + subset

        generatedQueriesMap += cellSubsetKey ->  ( numIdSubset + "/" + denIdSubset )
        runDivisionFormulaCount( numIdSubset, denIdSubset, cellSubsetKey )
      }

    } else {

      stopWithMessage( "Unknown aggregation type. Use aggregate-by={'national'|'subsets'}.  Default is 'national'" )
    }
  }

  /**
    * Helper method for runDivisionFormulaWrapper.  Does division for one num/den pair
    * @param numId
    * @param denId
    * @param id
    */
  private def runDivisionFormulaCount( numId: String, denId: String, id: String ): Unit = {

    if ( !dryRun ) {

      val num = resultsMap.getOrElse( numId, "" ).toString
      val den = resultsMap.getOrElse( denId, "" ).toString

      // verify that values are numeric before dividing
      if ( isDoubleable( num ) && isDoubleable( den ) ) {

        val ratio = doSafeDivision( num.toDouble, den.toDouble )
        if (debugging) println(s"cell [$id] contains [$num]/[$den] = [$ratio]")
        updateResultsMap( id, ratio )
        updateDivisorsMap( id, ( num, den ) )

      } else {

        if (debugging) println(s"runDivisionWrapper FAILED: num [$num], den [$den]")
        updateResultsMap(id, "FAILED")

        if (argsMap.getOrElse("stop-on-division-error", "").toString == "true") {
          printResultsMap()
          stopWithMessage("runDivisionWrapper FAILED" )
        }
      }
    }
  }
  /**
    * Back walks concatentation graph by fetching prefix query by id and prepending it to this idAndFilterString
    * @param idAndFilterString
    * @param id
    */
  private def runConcatentorFormulaWrapper( idAndFilterString: String, id: String ): Unit = {

    if ( debugging && verbose ) println( s"Entering runFilterFormulaWrapper( '$idAndFilterString', '$id' )" )

    // format: CellNumber|filterString, e.g.: F5 :: AND ( foo > 5 OR bar < 0 )
    var pair = idAndFilterString.trim.split( " :: " )
    var prefixId = pair(0).trim
    var filterString = pair(1).trim

    if ( debugging && verbose ) println(s"Args for idAndFilterString: [$prefixId] [$filterString]" )

    // prepare to get parent query(ies)...
    var queryType = FORMULA_CONCATENATOR
    var query = ""
    var queryAccumulator = ""

    // 1) ...get parent prefixes...
    while ( queryType == FORMULA_CONCATENATOR ) {

      if ( debugging && verbose ) println( s"Querying for prefix by prefixId [$prefixId]..." )
      val queryObj = rawQueryMap( prefixId )
      queryType = queryObj._1.toString
      query     = queryObj._2.toString

      // split out pieces if it's another formula
      if ( queryType == FORMULA_CONCATENATOR ) {

        pair = query.split( " :: " )
        prefixId = pair( 0 ).trim
        query = pair( 1 ).trim
      }
      // prepend just fetched query to accumulator
      queryAccumulator = query + " " + queryAccumulator
    }

    // 2) ...prepend them to filter, removing excess spaces that may have crept in
    query = ( queryAccumulator + " " + filterString ).replaceAll("\\s{2,}"," " )

    if ( debugging && verbose ) {
      println( s"[$id] prepended queryAccumulator [$queryAccumulator] to filter [$filterString]" )
      println( s"[$id] acumulated query chunks [$query]" )
    }

    // Run the appropriate query type, according to query type attached to the head of the concatenation graph
    if ( queryType == QUERY_COUNT ) {
      runCountWrapper( query, id )
    } else if ( queryType == QUERY_MEDIAN ) {
      runMedianWrapper( query, id )
    } else {
      if ( debugging ) println( s"Head of concatenation graph is UNKNOWN. id [$prefixId] type [$queryType]" )
    }
  }

  /**
    * Parses runtime args and builds array of strings containing keys for queriesMap
    * @return
    */
  def getRunList(): Array[ String ] = {

    var runList = Array[ String ]()

    // default is run all
    val runType = argsMap.getOrElse( "run-type", "all" ).toString
    if ( debugging && verbose ) println( s"runType [$runType]" )

    if ( runType == "ad-hoc" ) {

      runList = argsMap.getOrElse( "ad-hoc-args", "" ).toString.split( "," )

    } else {

      // get all queries, as loaded at loadQuery time
      runList = staticRunList.toArray
    }
    runList
  }

  /**
    * Util method for running sql query AND extracting Long value from a 1x1 DF, using "count" as col name.
    *
    * NOTE: Assumes all queries use "select count( foo ) as count"
    */
  private def runCountRule( queryString: String, id: String ): Long = {

    runCountRuleCounter += 1
    val startMillis = System.currentTimeMillis()
    val updatedQueryString = replaceAbbrevations( queryString )
    var count = -1L

    // keep a copy of the query for reporting purposes
    generatedQueriesMap += id -> updatedQueryString

    if ( dryRun ) {

      // do nothing

    } else {

      var countDf = spark.sql( updatedQueryString )
      count = countDf.select( "count" ).first()(0).asInstanceOf[Long]

      if (debugging && verbose) {
        println(s"queryString [$updatedQueryString]" )
        println(s"count: [$count]" )
      }
      if (debugging) println( s"RunCountRule called [$runCountRuleCounter] time(s) Time to run [$id]: [${System.currentTimeMillis() - startMillis}]ms" )
    }
    count
  }

  /**
    * Replaces all _some_ placeholder abbreviations in query string w/ their decompressed versions
    * @param queryString
    * @return
    */
  private def replaceAbbrevations( queryString: String ): String = {

    var updatedQueryString = queryString
    var key = ""
    var value = ""
    var pair = Array[ String ]()

    // iterate abbreviations list
    for ( abbreviation <- abbreviationsList ) {

      pair = abbreviation.split( " = " )
      key = pair( 0 )
      value = pair( 1 )

      //if ( debugging && verbose ) println( s"Searching for [$key], replacing with [$value]" )
      updatedQueryString = updatedQueryString.replaceAll( key, value )
    }
    updatedQueryString
  }
  /**
    * Does division, but only if den != 0
    * @param num
    * @param den
    * @return
    */
  private def doSafeDivision( num: Double, den: Double ): String = {

    if ( den != 0 ) {
      ( num / den ).toString
    } else {
      "a/0"
    }
  }

  /**------------------------------------------------------------------------------------------------------------------*/
  /** START stop block */
  private def stop(): Unit = {

    if ( debugging ) {
      printBanner( "stop() called" )
      printResultsMap()
    }

    processResults()

    if ( debugging ) {
      val minutes = (System.currentTimeMillis() - startMillis) / 1000.0 / 60.0
      println(f"RUN $appName complete in [$minutes%.1f] minutes.\n\n" )
    }
  }


  /**
    * Writes generatedQueriesMap to console: map = value
    */
  private def printDryRunMap(): Unit = {

    printBanner( "printDryRunMap" )

    // dump as sorted list of keys = values
    for ( key <- generatedQueriesMap.keys.toArray.sorted ) {
      println( s"key [$key] query [${generatedQueriesMap.getOrElse( key, "" )}]" )
    }
  }

  /**
    * Writes abbreviationsList to console
  */
  private def printAbbreviationsList(): Unit = {

    printBanner( "printAbbreviationsList" )

    // dump as sorted list of keys = values
    for ( abbreviation <- abbreviationsList ) {
      println( s"abbreviation [$abbreviation]" )
    }
  }

  /**
    * Writes abbreviationsList to console
    */
  private def printSubsetsList(): Unit = {

    printBanner( "printSubsetsList" )

    // dump as sorted list of keys = values
    for (subsets <- subsetsList ) {
      println( s"subsets [$subsets]" )
    }
  }
  /**
    * Writes resultsMap to console: map = value
    */
  private def printResultsMap(): Unit = {

    printBanner( "printResultsMap (resultsMap)" )

    for ( key <- resultsMap.keys.toArray.sorted ) {

      // two formats: long for when num and den are present...
      if ( divisorsMap.contains( key ) ) {
        println(s"key [$key] result [${resultsMap(key)}] = [${divisorsMap.getOrElse(key, ("NA", ""))._1}/${divisorsMap.getOrElse(key, ("", "NA"))._2}]")
      } else {
        // ...and short when it's a simple count
        println(s"key [$key] result [${resultsMap(key)}]")
      }
    }
  }

  /**
    * Writes expected to console: map = value
    */
  private def printQueries(): Unit = {

    printBanner( "printQueries: Raw" )

    // dump as sorted list of keys = values
    for ( key <- rawQueryMap.keys.toArray.sorted ) {
      println( s"key [$key] = [${rawQueryMap.getOrElse( key, "" )}]" )
    }

    printBanner( "printQueries: Generated" )
    for ( key <- generatedQueriesMap.keys.toArray.sorted ) {
      println( s"key [$key] = [${generatedQueriesMap.getOrElse( key, "" )}]" )
    }
  }
  /**
    * Writes expected to console: map = value
    */
  private def printExpectedResults(): Unit = {

    printBanner( "printExpectedResults" )

    // dump as sorted list of keys = values
    for ( key <- expectedResultsMap.keys.toArray.sorted ) {
      println( s"key [$key] = [${expectedResultsMap.getOrElse( key, "" )}]" )
    }
  }
  /**
    * Quick test to see if string represents a numeric value
    * @param s
    * @return
    */
  def isDoubleable( s: String ): Boolean = ( allCatch opt s.toDouble ).isDefined

  /**
    * Tests string to see if it represents a numeric.  If so, returns string.  If not, returns ""
    * @param s
    * @return
    */
  def getNumericOrNothing( s: String ): String = {

    if ( isDoubleable( s ) ) {
      return s
    } else {
      return ""
    }
  }
  /**
    * Compares actual w/ expected results.  Dumps results and summary to console and local file system (debugging)
    * or to console and s3 bucket (batch)
    */
  private def processResults(): Unit = {

    if ( debugging ) printBanner( "Expected vs. Actual results " )

    // default all to false
    val showHits    = argsMap.getOrElse( "show-hits",    "" ) == "true"
    val showMisses  = argsMap.getOrElse( "show-misses",  "" ) == "true"
    val showMissing = argsMap.getOrElse( "show-missing", "" ) == "true"

    // default to 'national' for now
    val aggregationType = argsMap.getOrElse( "aggregate-by", NATIONAL ).toString

    var comparison = ( "", "", "", "", "", "", "", "", "" )
    var comparisons = ArrayBuffer[ ( String, String, String, String, String, String, String, String, String ) ]()

    // Declare 5 on one line, from: https://stackoverflow.com/questions/1981748/declaring-multiple-variables-in-scala
    var ( expected, actual, query, num, den ) = ( "", "", "", "", "" )
    var ( hits, misses, missing ) = ( 0, 0, 0 )

    // iterate the results map
    for ( cellId <- resultsMap.keys.toArray.sorted ) {

      // split out subset ids from cell ids
      val ( id, subset ) = getIds( cellId )
      // expected values are imported as G29-ALL, etc.
      expected = expectedResultsMap( id + "-" + subset ).toString
      // actuals for nationals are stored w/o the "-ALL" suffix
      actual   =         resultsMap( cellId ).toString
      num = getNumericOrNothing( divisorsMap( cellId )._1 )
      den = getNumericOrNothing( divisorsMap( cellId )._2 )

      query = generatedQueriesMap.getOrElse( cellId, s"See 1st subset query above").toString

      // Compare results: Start w/ least common cases: missing values
      if ( expected == "" && actual == "" ) {

        missing += 1
        comparison = ( id, subset, "", "", "", "MISSING", "MISSING", "false", query )
        if ( showMissing ) println(s"Query id-subset: [$id]-[$subset] Expected [MISSING] == Actual: [MISSING] [false]" )

      } else if ( expected == "" ) {

        missing += 1
        comparison = ( id, subset, getNumericOrNothing( actual ), num, den, "MISSING", actual, "false", query )
        if ( showMissing ) println(s"Query id-subset: [$id]-[$subset] Expected [MISSING] == Actual: [$actual] [false]" )

      } else if ( actual == "" ) {

        missing += 1
        comparison = ( id, subset, getNumericOrNothing( actual ), num, den, expected, "MISSING", "false", query )
        if ( showMissing ) println( s"Query id-subset: [$id]-[$subset] Expected [$expected] == Actual: [MISSING] [false]" )

      } else  if ( expected == actual ) {

        hits += 1
        comparison = ( id, subset, getNumericOrNothing( actual ), num, den, expected, actual, "true", query )
        if ( showHits ) println( s"Query id-subset: [$id]-[$subset] Expected [$expected] == Actual: [$actual] [${expected == actual}]" )

      } else {

        misses += 1
        comparison = ( id, subset, getNumericOrNothing( actual ), num, den, expected, actual, "false", query )
        if ( showMisses ) println( s"Query id-subset: [$id]-[$subset] Expected [$expected] == Actual: [$actual] [${expected == actual}]" )
      }
      comparisons += comparison
    }
    val accuracy = ( hits * 1.0 / ( hits + misses + missing ) ) * 100
    printBanner( f"Hits: [$hits], misses: [$misses], missing: [$missing], accuracy: [$accuracy%.2f]%%" )

    // write to local file or S3
    writeResults( comparisons )

    // calculate total median runtimes
    if ( debugging ) {

      val minutes = medianRunTime / 1000.0 / 60.0
      println( s"Total time spent calculating medians [$minutes] minutes" )
    }
  }

  /*** Kludgey little helper method for processResults(...).  Depending on aggregationType, breaks out hyphenated key bits
    * when they are present: id1-id2, otherwise returns id1, id2 = ALL
    * @param id
    * @param aggregationType
    * @return ( id1, id2 ) as separate strings
    */
  private def getIds( compoundKey: String ): ( String, String ) = {

    var id1 = compoundKey
    var id2 = ""

    val pair = compoundKey.split( "-" )

    // the id is always present, unlike subsets below
    id1 = pair( 0 )

    if ( pair.length == 2 ) {
      id2 = pair( 1 )
    } else {
      // we must be in "national" mode if the pair doesn't have a length of 2
      id2 = "ALL"
    }

    return ( id1, id2 )
  }

  /**
    * Util method for processResults: writes output to s3, local temp file, or both, according to runtime args.
    * @param comparisonStrings
    */
  private def writeResults( comparisons: ArrayBuffer[ ( String, String, String, String, String, String, String, String, String ) ] ): Unit = {

    // conditional write to local file system
    if ( argsMap.getOrElse( "local-output", "" ) == "true" ) {

      // Build path for output, include date & time
      val localResultsPath = getUniqueResultsPath( getLocalResultsPath() )

      val file = new File( localResultsPath )
      val bw = new BufferedWriter(new FileWriter(file))

      if (debugging) println(s"Writing local results to [${file.toString}]")

      try {

        // write header first
        bw.write("cell,subset,value,numerator,denominator,expected,actual,match,query\n")

        // write all comparisons
        for (comparison <- comparisons) {

          // convert tuple into string: https://stackoverflow.com/questions/26751441/scala-tuple-to-string
          bw.write(comparison.productIterator.mkString(",") + "\n")
        }
      } catch {

        case ex: Exception => println(s"ERROR writing to [$localResultsPath]: [$ex.toString]")

      } finally {

        bw.close()
      }
    }
    // conditional write to s3, if spec'd
    if ( argsMap.getOrElse( "s3-output", "" ).toString.startsWith( "s3://" ) ) {

      // calculate infix text
      var resultsType = ""
      if ( argsMap.getOrElse( "run-both-aggregations", "false" ) == "true" ) {
        resultsType = "both"
      } else {
        resultsType = argsMap.getOrElse( "aggregate-by", SUBSETS ).toString
      }
      val windowType = argsMap.getOrElse( "report-window", WINDOW_RQ4 )
      val quarterID  = argsMap.getOrElse( "quarter-id",    QUARTER_ID )

      val s3path = getUniqueResultsPath( argsMap.get( "s3-output" ).get.toString + "-" + resultsType + "-" + windowType + "-" + quarterID )
      if (debugging) println(s"Writing results to [$s3path]")

      // Hah! https://stackoverflow.com/questions/44094108/not-able-to-import-spark-implicits-in-scalatest#44094228
      val sparkToo = spark // sparkToo *MUST* be val, never var!
      import sparkToo.implicits._

      val startMillis = System.currentTimeMillis()

      comparisons.toSeq.toDF( "cell", "subset", "value", "numerator", "denominator", "expected", "actual", "match", "query" ).coalesce( 1 ).write.option( "header", "true" ).csv( s3path )

      if ( debugging ) {
        println( s"Time to write results to S3 [${System.currentTimeMillis() - startMillis}]ms")
        println( s"s3path  [$s3path]" )
      }
    }
  }

  /**
    * Util method for writing local output: creates unique local /tmp path
    */
  private def getLocalResultsPath(): String = {

    // calculate infix text
    var resultsType = ""
    if ( argsMap.getOrElse( "run-both-aggregations", "false" ) == "true" ) {
      resultsType = "both"
    } else {
      resultsType = argsMap.getOrElse( "aggregate-by", SUBSETS ).toString
    }
    java.io.File.createTempFile( "results-" + resultsType + "-", "" ).toString
  }
  /**
    * Util method for writing output: Creates unique path by appending date/time to generic path
    */
  private def getUniqueResultsPath( path: String ): String = {

    val sdf = new SimpleDateFormat( "YYYY-MM-dd_@_HH:mm:ss" )
    val dateAndTime = sdf.format( Calendar.getInstance().getTime() )
    path + "-" + dateAndTime + ".csv"
  }

  /**
    * Prints output to console w/ banner formatting
    */
  private def printBanner( msg: String ): Unit = {

    println( "\n\n======================================================================================================" )
    println( s"= $msg" )
    println( "======================================================================================================" )
  }
  /**
    * Allows simple way to stop app from stubbed in, or broken, sections
    * @param msg
    */
  private def stopWithMessage( msg: String ): Unit = {

    printBanner( msg )
    spark.sparkContext.stop()
    System.exit( -1 )
  }
}

object WioaWpmaReport {

  val debugging = true

  def main( args: Array[String] ) {

    // timing conversion of 19M rows of CSV into parquet
    if ( 0 == 1 ) {
      val spark = SparkSession.builder.appName("Repartitioner").getOrCreate()

      val df = spark.read.format("csv").option("header", "false").load("s3://wips-dev-redshift-ready/spra/13/allrows").cache

      var counter = 4
      while (counter <= 128) {

        println("==========================================================================================")
        println(s"Counter [$counter]")
        println("==========================================================================================")

        var startMillis = System.currentTimeMillis()

        // write w/ n partitions
        val s3path = s"s3://ricks-foo-bucket/wioa-wpma/19M-repartitioned-$counter"
        val newDf = df.repartition(counter).cache
        println(s"Time to create [$counter] partitions [${(System.currentTimeMillis() - startMillis) / 1000.0 / 60.0}]m")

        //      // write as csv
        //      startMillis = System.currentTimeMillis()
        //      newDf.write.option( "header", "false" ).csv( s3path + ".csv" )
        //      println( s"Time to write [$counter] partitions as csv [${ ( System.currentTimeMillis() - startMillis ) / 1000.0 / 60.0 }]m" )

        // write as parquet
        startMillis = System.currentTimeMillis()
        newDf.write.parquet(s3path + ".parquet")
        println(s"Time to write [$counter] partitions as parquet [${(System.currentTimeMillis() - startMillis) / 1000.0 / 60.0}]m")

        counter = counter * 2
      }
    }

    val startMillis = System.currentTimeMillis()

    // Begin regular work of reporting engine
    val argsMap = loadArgumentsMap( args )

    val appName = "WIOA WPMA Report"
    var spark: SparkSession = null

    // start and force to run on one partition. Defaults to distributed
    if ( argsMap.getOrElse( "driver-only", "" ) == "true" ) {
      spark = SparkSession.builder.master("local[1]").appName(appName).getOrCreate()
    } else {
      spark = SparkSession.builder.appName(appName).getOrCreate()
    }

    val reportEngine = new WioaWpmaReport( spark, argsMap, appName )

    // allow us to skip init if in interactive mode
    val interactive = argsMap.getOrElse( "interactive", "false" ) == "true"
    val runInit     = argsMap.getOrElse( "run-init",     "true" ) == "true"
    val batch = !interactive

    // always runs when in batch mode, or when interactive and runinit arg == true
    if ( batch || ( interactive && runInit ) ) {
      reportEngine.init()
    }
    // are we in interactive or batch (default) mode?
    if ( interactive ) {

      reportEngine.startInteractiveMode()

    } else {

      // batch mode:
      reportEngine.run()

      // get row count *AFTER* caching of input set via run() and *BEFORE* killing spark session
      val rows = reportEngine.getInputCount()

      reportEngine.stop()

      runStats( appName, argsMap, rows, startMillis, spark, reportEngine )
    }
  }
  /**
    * Parses name value pairs passed in the main methods's args array and populates a map with them for non-position dependent lookup
    * @param args
    */
  def loadArgumentsMap( args: Array[ String ] ): Map[ String, Any ] = {

    if ( debugging ) {
      println("\n\n===================================================")
      println("= loadArgumentsMap called...")
      println("===================================================")
    }
    var name = ""
    var value = ""
    val argsMap = Map[ String, Any ]()
    for ( arg <- args ) {

      // quick sanity check: Does arg have 'name=value' format?
      if ( arg.split( "=" ).length == 1 ) {
        println( "\n\n===================================================")
        println( s"= args missing value: [$arg]" )
        println( "===================================================")
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
    * Helper method for main(...) Calculates runtime performance stats
    * @param startMillis
    * @param spark
    * @param reportEngine
    */
  def runStats( appName: String, argsMap: Map[ String, Any ], rows: Long, startMillis: Long, spark: SparkSession, reportEngine: WioaWpmaReport ): Unit = {

    val commaFormatter = NumberFormat.getIntegerInstance
    val seconds = ( System.currentTimeMillis() - startMillis) / 1000.0
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
    println("\n\n===================================================")
    println( s"= [$appName] run completed" )
    println("===================================================")
    println( f"Input [${argsMap.getOrElse( "input", "" )}]" )
    println( f"Partitions requested [$partitionsRequested] vs. actual/created [$partitionsActual]" )
    println( f"Calcs finished in [$minutes%.1f] minutes @ rate of [$kRowsPerSecond%.1f]K rows/sec" )
    println( f"[$totalCalcsB%.1f] Billion(s) of calcs = [${commaFormatter.format( cellCalcsCount ) }] (queries * subsets) over [$millionsOfRows%.1f] Million(s) of rows @ rate of [$calcsPerSecB%.1f]B calcs/sec\n\n" )
  }
}