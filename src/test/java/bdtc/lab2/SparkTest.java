package bdtc.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static bdtc.lab2.MessagesCounter.countMessages;

/**
 *  Tests to evaluate application performance.
 *  Testing values, config and test results are stored in src/test/testing-values/ directory.
 *  */
public class SparkTest {

    String testsDir = "src/test/testing-values/";
    String testValues = "test.csv";
    String expectedResult = "result.csv";
    String testConfig = testsDir + "config.csv";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    /**
     *  Test for only one entry in statistics.
     *  */
    @Test
    public void testMessagingStatisticsOneEntry() {
        test("one-entry/");
    }

    /**
     *  General test: multiple users from multiple groups.
     *  */
    @Test
    public void testMessagingStatistics() {
        test("general/");
    }

    /**
     *  Test number of messages sent per user in a group
     *  if not all members of the group sent messages.
     *  */
    @Test
    public void testMessagingStatisticsNotAllUsersMessaged() {
        test("not-all-users-messaged/");
    }

    /**
     *  Function that runs app and checks the correct calculation.
     *  @param args: caseDirName to specify directory with testing values
     *             and result for each test case
     *  */
    public void test(String caseDirName) {
        String caseDir = testsDir + caseDirName;

        Dataset<Row> config = ss.read()
                .format("csv")
                .option("header", true)
                .load(testConfig);

        Dataset<Row> expectedDataset = ss.read()
                .format("csv")
                .option("header", true)
                .load(caseDir + expectedResult);

        Dataset<Row> testDataset = ss.read().csv(caseDir + testValues);
        testDataset = DatasetColumnsRenamer.renameColumns(testDataset);

        Dataset<Row> result = countMessages(testDataset, config);
        result.show();

        checkEquality(expectedDataset, result);
    }

    /**
     *  Function to assert that two datasets are equal.
     *  @param args: expected dataset, received dataset
     *  */
    private void checkEquality(Dataset<Row> expected, Dataset<Row> received) {
        Dataset<Row> df = received.except(expected);
        assert df.count() == 0;
    }
}
