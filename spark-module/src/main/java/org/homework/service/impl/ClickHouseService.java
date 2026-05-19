package org.homework.service.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.homework.report.ReportService;
import org.homework.service.ReportSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class ClickHouseService implements ReportSink {

    private final String chUrl;
    private final Properties chProps;

    public ClickHouseService(String chUrl, Properties chProps) {
        this.chUrl = chUrl;
        this.chProps = chProps;
    }

    @Override
    public void writeReports(Map<String, Dataset<Row>> reports) {
        createTables();
        reports.forEach(this::write);
        System.out.println("Отчёты загружены в ClickHouse");
    }

    private void write(String table, Dataset<Row> df) {
        df.write()
            .mode("append")
            .jdbc(chUrl, table, chProps);
    }

    private void createTables() {
        Map<String, String> ddls = new LinkedHashMap<>();
        ddls.put(ReportService.PRODUCT_SALES,
            "CREATE TABLE IF NOT EXISTS " + ReportService.PRODUCT_SALES + " (" +
            "  product_name String, category_name String," +
            "  total_quantity_sold Int64, total_revenue Float64," +
            "  avg_rating Float64, reviews_count Int64, sales_rank Int32" +
            ") ENGINE = MergeTree() ORDER BY product_name"
        );
        ddls.put(ReportService.CUSTOMER_SALES,
            "CREATE TABLE IF NOT EXISTS " + ReportService.CUSTOMER_SALES + " (" +
            "  customer_id Int32, first_name String, last_name String," +
            "  email String, country String," +
            "  orders_count Int64, total_spent Float64, avg_check Float64, spending_rank Int32" +
            ") ENGINE = MergeTree() ORDER BY customer_id"
        );
        ddls.put(ReportService.TIME_SALES,
            "CREATE TABLE IF NOT EXISTS " + ReportService.TIME_SALES + " (" +
            "  year Int32, month Int32," +
            "  orders_count Int64, total_revenue Float64," +
            "  avg_order_size Float64, cumulative_revenue Float64" +
            ") ENGINE = MergeTree() ORDER BY (year, month)"
        );
        ddls.put(ReportService.STORE_SALES,
            "CREATE TABLE IF NOT EXISTS " + ReportService.STORE_SALES + " (" +
            "  store_name String, city String, country String," +
            "  orders_count Int64, total_revenue Float64, avg_check Float64, revenue_rank Int32" +
            ") ENGINE = MergeTree() ORDER BY store_name"
        );
        ddls.put(ReportService.SUPPLIER_SALES,
            "CREATE TABLE IF NOT EXISTS " + ReportService.SUPPLIER_SALES + " (" +
            "  supplier_name String, supplier_country String," +
            "  orders_count Int64, total_revenue Float64," +
            "  avg_product_price Float64, revenue_rank Int32" +
            ") ENGINE = MergeTree() ORDER BY supplier_name"
        );
        ddls.put(ReportService.PRODUCT_QUALITY,
            "CREATE TABLE IF NOT EXISTS " + ReportService.PRODUCT_QUALITY + " (" +
            "  product_name String, category_name String," +
            "  rating Float64, reviews_count Int64," +
            "  total_sold Int64, total_revenue Float64, rating_sales_corr Float64" +
            ") ENGINE = MergeTree() ORDER BY product_name"
        );

        Properties ddlProps = new Properties();
        ddlProps.putAll(chProps);
        ddlProps.remove("driver");

        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC драйвер не найден", e);
        }

        try (Connection conn = DriverManager.getConnection(chUrl, ddlProps);
             Statement stmt = conn.createStatement()) {
            for (Map.Entry<String, String> entry : ddls.entrySet()) {
                String table = entry.getKey();
                String ddl = entry.getValue();
                stmt.execute(ddl);
                stmt.execute("TRUNCATE TABLE IF EXISTS " + table);
            }
        } catch (Exception e) {
            throw new RuntimeException("Ошибка создания таблиц ClickHouse", e);
        }
    }
}
