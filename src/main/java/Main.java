import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;


public class Main {
    public static void main(String[] args) {

        // Start Spark on local
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

        // STAGE I
        // * Generate 2 parquet-file from one json-file
        Dataset<Row> events = spark.read().json("data/events.json");
        events.printSchema(); // show schema

        // Create view for execute SQL later...
        events.createOrReplaceTempView("events");

        spark.sql("SELECT cast(_t as timestamp) as time, _p as email, device_type FROM events WHERE _n = 'app_loaded'").write().mode(SaveMode.Overwrite).parquet("/home/user/events/app_loaded/app_loaded.parquet"); //"data/app_loaded.parquet"
        spark.sql("SELECT cast(_t as timestamp) as time, _p as email, channel FROM events WHERE _n = 'registered'").write().mode(SaveMode.Overwrite).parquet("/home/user/events/registered/registered.parquet"); // "data/registred.parquet"


        // STAGE II
        // * SQL under parquet-files
        // * Print into console data
        Dataset<Row> parquetFileApp = spark.read().parquet("/home/user/events/app_loaded/app_loaded.parquet");
        Dataset<Row> parquetFileReg = spark.read().parquet("/home/user/events/registered/registered.parquet");

        parquetFileApp.createOrReplaceTempView("parquetApp");
        parquetFileReg.createOrReplaceTempView("parquetReg");

        // these are the guys who downloaded the application one week after registration
        String SqlText = "SELECT datediff(t2.time,t1.time) as diff_t,t1.time as t_reg, t2.time as t_down, t2.email" +
                         "  FROM parquetReg t1, " +
                         "       (select email, min(time) as time " + // only first download app
                         "          from parquetApp " +
                         "         group by email) t2 " +
                         " WHERE t1.email=t2.email" +
                         "   and datediff(t2.time,t1.time) <= 7" +
                         "   and datediff(t2.time,t1.time) > 0";
        Dataset<Row> namesDF = spark.sql(SqlText);
        //namesDF.show(); // Show for debug
        // In production, it is better to calculate in SQL
        long cnt_good_guys = namesDF.count();

        // Count of All registered users
        SqlText = "SELECT count(*) as cnt" +
                  "  FROM parquetReg ";
        Dataset<Row> namesDF2 = spark.sql(SqlText);
        // get first record
        long cnt_all = namesDF2.select(col("cnt")).first().getLong(0);

        // Print results
        System.out.println("cnt_good_guys: " + cnt_good_guys + "   cnt_all: "+ cnt_all);
        float prc = ((float) cnt_good_guys)/((float)cnt_all)*100;
        System.out.printf("Percent: %.2f", prc); System.out.println("%");

        // SparkSession stop
        spark.stop();
    }
}