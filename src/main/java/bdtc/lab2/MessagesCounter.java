package bdtc.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 *  Class to calculate average number of messages sent per user of a group.
 *  */
public class MessagesCounter {

    /**
     *  Function to calculate average number of messages sent per user of a group.
     * @param args: inputDataset, config with groups and their users
     *  */
    public static Dataset<Row> countMessages(Dataset<Row> inputDataset, Dataset<Row> config) {

        // count number of messages each user sent
        Dataset<Row> userMessages = inputDataset.groupBy("sender").count();

        // join groups and number of messages sent by users
        Dataset<Row> joinedTables = config.join(userMessages,
                        userMessages.col("sender").equalTo(config.col("username")),
                        "left")
                .na().fill(0)
                .drop("sender");

        // calculate average number of messages per user for each group
        Dataset<Row> result = joinedTables.groupBy("group")
                .agg(round(avg("count"), 2).alias("messages_per_user"))
                .sort("group");

        result.show();
        return result;
    }
}