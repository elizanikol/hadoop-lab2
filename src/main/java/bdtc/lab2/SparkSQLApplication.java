package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Main SparkSQLApplication class to count average number of messages sent per user of a group.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: input file, args[1] - config, args[2] - output folder
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar inputDir config outputDirectory");
        }
        String hdfsURL = "hdfs://127.0.0.1:9000/";
        log.info("Application started!");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        Dataset<Row> dfInput = sc.read().csv(hdfsURL + args[0]);
        Dataset<Row> dfConfig = sc.read().format("csv").option("header", true).load(hdfsURL + args[1]);
        dfInput.show();
        dfConfig.show();
        dfInput = DatasetColumnsRenamer.renameColumns(dfInput);
        log.info("===============COUNTING...================");
        Dataset<Row> result = MessagesCounter.countMessages(dfInput, dfConfig);
        log.info("============SAVING FILE TO " + hdfsURL + args[2] + " directory============");
        result.toDF(result.columns())
                .write()
                .option("header", true)
                .csv(hdfsURL + args[2]);
    }
}
