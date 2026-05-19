package org.homework.service.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.homework.service.ReportSink;

import java.util.Map;

public class MongoDbService implements ReportSink {

    private static final String MONGO_URI = "mongodb://student:student@mongodb:27017/reports?authSource=admin";
    private static final String DB = "reports";

    @Override
    public void writeReports(Map<String, Dataset<Row>> reports) {
        reports.forEach(this::write);
        System.out.println("Отчёты загружены в MongoDB");
    }

    private void write(String collection, Dataset<Row> df) {
        df.write()
            .format("mongodb")
            .mode("overwrite")
            .option("connection.uri", MONGO_URI)
            .option("database", DB)
            .option("collection", collection)
            .save();
    }
}
