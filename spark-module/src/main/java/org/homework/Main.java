package org.homework;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.homework.datasource.SnowflakeTransformer;
import org.homework.report.ReportService;
import org.homework.service.impl.CassandraService;
import org.homework.service.impl.ClickHouseService;
import org.homework.datasource.DataSourceService;
import org.homework.service.impl.MongoDbService;
import org.homework.service.ReportSink;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main {

    private static final String PG_URL      = "jdbc:postgresql://postgres:5555/lab2";
    private static final String PG_USER     = "student";
    private static final String PG_PASSWORD = "student";

    private static final String CH_URL      = "jdbc:clickhouse://clickhouse:8123/reports";
    private static final String CH_USER     = "student";
    private static final String CH_PASSWORD = "student";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("BigDataLab2-ETL")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.cassandra.connection.host", "cassandra")
                .config("spark.cassandra.connection.port", "9042")
                .config("spark.mongodb.write.connection.uri", "mongodb://student:student@mongodb:27017/reports?authSource=admin")
                .config("spark.mongodb.read.connection.uri",  "mongodb://student:student@mongodb:27017/reports?authSource=admin")
                .getOrCreate();

        Properties pgProps = new Properties();
        pgProps.setProperty("user",     PG_USER);
        pgProps.setProperty("password", PG_PASSWORD);
        pgProps.setProperty("driver",   "org.postgresql.Driver");

        Properties chProps = new Properties();
        chProps.setProperty("user",     CH_USER);
        chProps.setProperty("password", CH_PASSWORD);
        chProps.setProperty("driver",   "com.clickhouse.jdbc.ClickHouseDriver");

        SnowflakeTransformer transformer     = new SnowflakeTransformer(PG_URL, pgProps);
        DataSourceService dataSource      = new DataSourceService(PG_URL, pgProps);
        ReportService reportService   = new ReportService();

        List<ReportSink> sinks = List.of(
            new ClickHouseService(CH_URL, chProps),
            new MongoDbService(),
            new CassandraService()
        );

        Map<String, Dataset<Row>> reports = null;
        try {
            // ЭТАП 1: Чтение сырых данных
            Dataset<Row> rawData = spark.read().jdbc(PG_URL, "mock_data", pgProps);
            System.out.println("Загружено строк: " + rawData.count());

            // ЭТАП 2: Трансформация в снежинку → PostgreSQL
            transformer.transform(spark, rawData);

            // ЭТАП 3: Загрузить снежинку как views — один раз для всех сервисов
            dataSource.loadSnowflakeViews(spark);

            // ЭТАП 4: Вычислить и закешировать все отчёты — один раз
            reports = reportService.buildAndCacheAll(spark);

            // ЭТАП 5: Записать в каждое хранилище из кеша
            for (ReportSink sink : sinks) {
                try {
                    sink.writeReports(reports);
                } catch (Exception e) {
                    System.err.println("Ошибка записи в " + sink.getClass().getSimpleName() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reports != null) {
                reportService.unpersistAll(reports);
            }
            spark.stop();
        }
    }
}
