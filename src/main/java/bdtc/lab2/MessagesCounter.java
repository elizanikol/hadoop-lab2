package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

//@Slf4j
/**
 *  Class to calculate average number of messages sent per user of a group.
 *  */
public class MessagesCounter {

    /**
     *  Function to calculate average number of messages sent per user of a group.
     * @param args: inputDataset, config with groups and their users
     *  */
    public static Dataset<Row> countMessages(Dataset<Row> inputDataset, Dataset<Row> config) {

        // join messaging statistics with users groups
        Dataset<Row> joinedTables = inputDataset.join(config,
                inputDataset.col("sender").equalTo(config.col("username"))
        ).select("group", "sender");

        // count the number of messages each user sent
        Dataset<Row> messagesPerGroup = joinedTables.groupBy("group").count()
                .withColumnRenamed("count", "count_messages");

        // count the number of users in each group
        Dataset<Row> usersPerGroup = config.groupBy("group").count()
                .withColumnRenamed("count", "count_users");

        // for each group calculate the average number of messages sent by its members
        Dataset<Row> result = usersPerGroup.join(messagesPerGroup, "group")
                .withColumn("messages_per_user",
                        round(col("count_messages").divide(col("count_users")), 2))
                .drop("count_messages", "count_users").sort("group");

        result.show();
        return result;
    }
}