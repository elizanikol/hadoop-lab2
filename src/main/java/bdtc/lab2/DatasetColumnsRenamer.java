package bdtc.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *  Class that renames columns of a dataset which was created
 *  from the database table.
 *  */
public class DatasetColumnsRenamer {

    /**
     *  Map of column names of the table.
     *  */
    private static final Map<String, String> columnNames = Stream.of(new String[][] {
            {"_c0", "id"},
            {"_c1", "sender"},
            {"_c2", "receiver"},
            {"_c3", "date"},
            {"_c4", "message"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    /**
     *  Function to rename columns of a dataset.
     * @param args: dataset
     * @return renamed dataset
     *  */
    public static Dataset<Row> renameColumns(Dataset<Row> df) {
        for (String key : columnNames.keySet()) {
            df = df.withColumnRenamed(key, columnNames.get(key));
        }
        return df;
    }
}

