import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;


public class Main {
    public static void main(String[] args) {

        // Check program parameters
        int typeMode = 0;
        if (args.length == 0)
        {
            System.out.println("ERROR! Need parametrs: 1 - create parquet-files, 2 - calculate percent");
            System.exit(1);
        }
        else
        {
            switch (args[0])
            {
                case "1":   typeMode = 1;
                            break;
                case "2":   typeMode = 2;
                            break;
                default:    System.out.println("ERROR! Incorrect parametrs! You type'"+args[0]+"', but allowable: 1 - create parquet-files, 2 - calculate percent");
                            System.exit(1);

            }
        }

        // Start Spark on local
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

        try {
            if (typeMode == 1) {

                // STAGE I
                // * Generate 2 parquet-file from one json-file
                Dataset<Row> events = spark.read().json("data/events.json");
                events.printSchema(); // show schema

                // Create view for execute SQL later...
                events.createOrReplaceTempView("events");

                spark.sql("SELECT cast(_t as timestamp) as time, _p as email, device_type FROM events WHERE _n = 'app_loaded'").write().mode(SaveMode.Overwrite).parquet("/home/user/events/app_loaded/app_loaded.parquet"); //"data/app_loaded.parquet"
                spark.sql("SELECT cast(_t as timestamp) as time, _p as email, channel FROM events WHERE _n = 'registered'").write().mode(SaveMode.Overwrite).parquet("/home/user/events/registered/registered.parquet"); // "data/registred.parquet"
            } else if (typeMode == 2) {

                // STAGE II
                // * SQL under parquet-files
                // * Print into console data
                Dataset<Row> parquetFileApp = spark.read().parquet("/home/user/events/app_loaded/app_loaded.parquet");
                Dataset<Row> parquetFileReg = spark.read().parquet("/home/user/events/registered/registered.parquet");

                parquetFileApp.createOrReplaceTempView("parquetApp");
                parquetFileReg.createOrReplaceTempView("parquetReg");

                // these are the guys who downloaded the application one week after registration
                String sqlText = "SELECT count(*) as cnt" +
                        "  FROM parquetReg t1, " +
                        "       (select email, min(time) as time " + // only first download app
                        "          from parquetApp " +
                        "         group by email) t2 " +
                        " WHERE t1.email=t2.email" +
                        "   and datediff(t2.time,t1.time) <= 7" +
                        "   and datediff(t2.time,t1.time) >= 0";
                Dataset<Row> namesDF = spark.sql(sqlText);
                long cntGoodGuys = namesDF.select(col("cnt")).first().getLong(0);

                // Count of All registered users
                sqlText = "SELECT count(email) as cnt" +
                        "  FROM parquetReg ";
                Dataset<Row> namesDF2 = spark.sql(sqlText);
                long cntAll = namesDF2.select(col("cnt")).first().getLong(0);

                // Print results
                float prc = ((float) cntGoodGuys) / ((float) cntAll) * 100;
                System.out.printf("Percentage of app downloads: %.2f", prc);
                System.out.println("%");
            } else {
                System.out.println("ERROR! Unknown typeMode! Check the program code!");
                System.exit(1);
            }
        }
        finally {
            // SparkSession stop
            spark.stop();
        }
    }
}