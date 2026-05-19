package org.homework.service.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.homework.report.ReportService;
import org.homework.service.ReportSink;

import java.net.InetSocketAddress;
import java.util.Map;

public class CassandraService implements ReportSink {

    private static final String HOST     = "cassandra";
    private static final int    PORT     = 9042;
    private static final String KEYSPACE = "reports";
    private static final String DC       = "datacenter1";

    @Override
    public void writeReports(Map<String, Dataset<Row>> reports) {
        createKeyspaceAndTables();
        reports.forEach(this::write);
        System.out.println("Отчёты загружены в Cassandra");
    }

    private void write(String table, Dataset<Row> df) {
        df.write()
            .format("org.apache.spark.sql.cassandra")
            .mode("overwrite")
            .option("keyspace", KEYSPACE)
            .option("table", table)
            .option("confirm.truncate", "true")
            .save();
    }

    private void createKeyspaceAndTables() {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(HOST, PORT))
                .withLocalDatacenter(DC)
                .build()) {

            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.PRODUCT_SALES + " (" +
                "  product_name text, category_name text," +
                "  total_quantity_sold bigint, total_revenue double," +
                "  avg_rating double, reviews_count bigint, sales_rank int," +
                "  PRIMARY KEY (category_name, total_quantity_sold, product_name)" +
                ") WITH CLUSTERING ORDER BY (total_quantity_sold DESC, product_name ASC)"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.CUSTOMER_SALES + " (" +
                "  customer_id int, first_name text, last_name text," +
                "  email text, country text," +
                "  orders_count bigint, total_spent double, avg_check double, spending_rank int," +
                "  PRIMARY KEY (country, total_spent, customer_id)" +
                ") WITH CLUSTERING ORDER BY (total_spent DESC, customer_id ASC)"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.TIME_SALES + " (" +
                "  year int, month int," +
                "  orders_count bigint, total_revenue double," +
                "  avg_order_size double, cumulative_revenue double," +
                "  PRIMARY KEY (year, month)" +
                ") WITH CLUSTERING ORDER BY (month ASC)"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.STORE_SALES + " (" +
                "  store_name text, city text, country text," +
                "  orders_count bigint, total_revenue double, avg_check double, revenue_rank int," +
                "  PRIMARY KEY (country, total_revenue, store_name)" +
                ") WITH CLUSTERING ORDER BY (total_revenue DESC, store_name ASC)"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.SUPPLIER_SALES + " (" +
                "  supplier_name text, supplier_country text," +
                "  orders_count bigint, total_revenue double," +
                "  avg_product_price double, revenue_rank int," +
                "  PRIMARY KEY (supplier_country, total_revenue, supplier_name)" +
                ") WITH CLUSTERING ORDER BY (total_revenue DESC, supplier_name ASC)"
            );

            executeCql(session,
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + ReportService.PRODUCT_QUALITY + " (" +
                "  product_name text, category_name text," +
                "  rating double, reviews_count bigint," +
                "  total_sold bigint, total_revenue double, rating_sales_corr double," +
                "  PRIMARY KEY (category_name, rating, product_name)" +
                ") WITH CLUSTERING ORDER BY (rating DESC, product_name ASC)"
            );
        }
    }

    private void executeCql(CqlSession session, String cql) {
        session.execute(cql);
    }
}
