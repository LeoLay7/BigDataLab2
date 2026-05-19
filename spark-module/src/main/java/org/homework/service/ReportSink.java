package org.homework.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public interface ReportSink {
    void writeReports(Map<String, Dataset<Row>> reports);
}
