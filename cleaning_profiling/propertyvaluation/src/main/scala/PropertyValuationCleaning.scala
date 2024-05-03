import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, DoubleType}

object PropertyValuationCleaning {
  def main(args: Array[String]): Unit = {
    // Check if the required number of arguments is provided
    if (args.length != 1) {
      println("Usage: spark-submit --class <MainClassName> <jar_file_path> <input_directory_path>")
      sys.exit(1)
    }

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("PropertyValuationCleaning")
      .getOrCreate()

    // Get the input directory path from command-line arguments
    val directoryPath = args(0)

    // Read the CSV files and select only the required columns
    val rawDataframe = spark.read
      .option("header", "true")
      .csv(directoryPath + "/*.csv")

    // Print the count of rows before any filtering
    println(s"Number of rows in raw data: ${rawDataframe.count()}")

    val deduplicatedDataframe = rawDataframe.dropDuplicates()
    println(s"Number of rows after deduplication: ${deduplicatedDataframe.count()}")

    // Function to drop specified columns
    def dropColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
      df.drop(columns: _*)
    }

    // List of columns to drop
    val columnsToDrop = Seq("HOUSENUM_LO", "HOUSENUM_HI", "STREET_NAME", "PYMKTLAND", "PYMKTTOT", "PYACTLAND", "PYACTTOT", "PYACTEXTOT", "PYTRNLAND", "PYTRNTOT",
      "PYTRNEXTOT", "PYTXBTOT", "PYTXBEXTOT", "FINMKTLAND", "FINMKTTOT", "FINACTLAND",
      "FINACTTOT", "FINACTEXTOT", "FINTRNLAND", "FINTRNTOT", "FINTRNEXTOT", "FINTXBTOT",
      "FINTXBEXTOT", "CURACTEXTOT", "CURTRNLAND", "CURTRNTOT", "CURTRNEXTOT", "CURTXBTOT",
      "CURTXBEXTOT", "PARID", "YRBUILT")

    // Drop the specified columns
    val columnDroppedDataframe = dropColumns(deduplicatedDataframe, columnsToDrop)
    println(s"Dropped columns ${columnsToDrop}")

    def filterZeroRows(df: DataFrame): DataFrame = {
      val zeroColumns = List("GROSS_SQFT", "UNITS", "LAND_AREA", "CURACTTOT", "CURACTLAND", "CURMKTTOT", "CURMKTLAND")
      df.filter(zeroColumns.map(colName => col(colName) =!= 0).reduce(_ && _))
    }

    // Filter out the rows with 0 values in specified columns
    val filteredDataframe = filterZeroRows(columnDroppedDataframe)
    println(s"Number of rows after filtering rows having invalid value in critical columns: ${filteredDataframe.count()}")

    // Define the UDF to convert BORO code to Borough name
    val boroCodeToBorough = udf((boroCode: Int) => {
      boroCode match {
        case 1 => "MANHATTAN"
        case 2 => "BRONX"
        case 3 => "BROOKLYN"
        case 4 => "QUEENS"
        case 5 => "STATEN ISLAND"
        case _ => "UNKNOWN"
      }
    })

    // Add a new column with Borough names in all caps based on BORO code
    val dataframeWithBorough = filteredDataframe.withColumn("BOROUGH", boroCodeToBorough(col("BORO")))
      .withColumn("BOROUGH", upper(col("BOROUGH")))

    // Filter out rows where BLOCK is not within the valid ranges for the corresponding borough
    val filteredDataframeBlock = dataframeWithBorough.filter(
      (col("BORO") === 1 && col("BLOCK").between(1, 2255)) ||
        (col("BORO") === 2 && col("BLOCK").between(2260, 5958)) ||
        (col("BORO") === 3 && col("BLOCK").between(1, 8955)) ||
        (col("BORO") === 4 && col("BLOCK").between(1, 16350)) ||
        (col("BORO") === 5 && col("BLOCK").between(1, 8050))
    )

    println(s"Number of rows after filtering block numbers: ${dataframeWithBorough.count()}")

    val noZipDataframe = filteredDataframeBlock.filter((col("ZIP_CODE").isNull || col("ZIP_CODE") === "") && col("BOROUGH").isNotNull)

    // Count of rows with no zip code but having Borough, Block, Lot (BBL)
    val noZipCount = noZipDataframe.count()

    // Data manually sourced from https://propertyinformationportal.nyc.gov/
    val manuallySourcedData = Seq(
      ("QUEENS", 11151, 11429), ("QUEENS", 11531, 11417), ("QUEENS", 12169, 11433), ("QUEENS", 12573, 11434), ("QUEENS", 13013, 11413),
      ("QUEENS", 1313, 11377), ("QUEENS", 13383, 11434), ("MANHATTAN", 1493, 10028), ("QUEENS", 15744, 11691), ("QUEENS", 15891, 11691),
      ("QUEENS", 16134, 11693), ("QUEENS", 16178, 11694), ("QUEENS", 1918, 11368), ("QUEENS", 2157, 11375), ("QUEENS", 2342, 11377),
      ("QUEENS", 2611, 11378), ("BRONX", 2829, 10453), ("BRONX", 3029, 10457), ("BRONX", 3156, 10457), ("MANHATTAN", 36, 10016),
      ("QUEENS", 4077, 11356), ("QUEENS", 4197, 11356), ("QUEENS", 4362, 11354), ("QUEENS", 4945, 11354), ("QUEENS", 4966, 11354),
      ("QUEENS", 5168, 11355), ("BRONX", 5409, 10465), ("QUEENS", 5523, 11355), ("QUEENS", 6371, 11355), ("QUEENS", 6741, 11365),
      ("BROOKLYN", 7021, 11224), ("QUEENS", 9976, 11435)
    )

    // Create DataFrame from the data
    val manualDataframe = broadcast(spark.createDataFrame(manuallySourcedData).toDF("BOROUGH", "BLOCK", "DISCOVERED_ZIP_CODE"))

    // Left outer join on Borough and Block
    val joinedDataframe = filteredDataframeBlock.join(manualDataframe, Seq("BOROUGH", "BLOCK"), "left_outer")

    // Use the discovered zip code to override null or empty zip code
    val combinedDataframe = joinedDataframe.withColumn("ZIP_CODE", coalesce(col("ZIP_CODE"), col("DISCOVERED_ZIP_CODE"))).drop("DISCOVERED_ZIP_CODE")

    // Trim zip codes having length > 5
    val trimmedZipDataframe = combinedDataframe.withColumn("ZIP_CODE", expr("substring(ZIP_CODE, 1, 5)"))

    // Count of rows having valid but 0 zipcode
    val zeroZipRowCount = trimmedZipDataframe.filter(col("ZIP_CODE") === 0).count()

    // Drop rows having invalid zip code since there is no API to automate ZIP_CODE extraction from BBL
    val cleanedDataframe = trimmedZipDataframe.filter(col("ZIP_CODE") =!= 0)

    println(s"Number of rows after deleting rows with invalid zip code: ${cleanedDataframe.count()}")

    // Function to profile columns

    def profileColumns(df: DataFrame): Unit = {
      val intColumns = List("CURMKTLAND", "CURMKTTOT", "CURACTLAND", "CURACTTOT",
        "LAND_AREA", "NUM_BLDGS", "BLD_STORY", "GROSS_SQFT")
      val doubleColumns = List("UNITS")
      val columns = df.columns
      for (columnName <- columns) {
        // Check if the column is expected to contain integers or doubles
        val isInt = intColumns.contains(columnName) && df.select(columnName).filter(col(columnName).rlike("^\\d+$")).count() == df.count()
        val isDouble = doubleColumns.contains(columnName) && df.select(columnName).filter(col(columnName).rlike("^\\d+\\.?\\d*$")).count() == df.count()

        if (isInt || isDouble) {
          // If the column contains integers or doubles, cast it to IntegerType and display statistics
          println(s"Overall statistics for column: $columnName")

          if (isInt)
            df.withColumn(columnName, col(columnName).cast(IntegerType)).describe(columnName).show() // Display statistics for the column

          if (isDouble)
            df.withColumn(columnName, col(columnName).cast(DoubleType)).describe(columnName).show()

          // Borough-wise statistics for integer or double columns
          println(s"Borough-wise statistics for column: $columnName")
          df.groupBy("BOROUGH").agg(avg(columnName).as("Average"),
            min(columnName).as("Minimum"),
            max(columnName).as("Maximum"),
            stddev(columnName).as("Standard Deviation")).show()
        } else {
          // If the column is not integer or double, display distinct values and their counts
          println(s"Distinct values for column: $columnName")
          val distinctCount = df.select(columnName).distinct().count()
          df.groupBy(columnName).count().show()
          println(s"Total distinct values for $columnName: $distinctCount\n") // Display number of distinct values for the column
        }
      }
    }

    // Call the function with your DataFrame
    profileColumns(cleanedDataframe)

    // Save the cleaned dataframe to HDFS
    cleanedDataframe.write
      .option("header", "true")
      .csv("hdfs:///user/ap7986_nyu_edu/bdad_project/cleaned_property_valuation_data.csv")

    // Stop SparkSession
    spark.stop()
  }
}

