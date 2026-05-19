package org.homework.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedHashMap;
import java.util.Map;

public class ReportService {

    public static final String PRODUCT_SALES   = "report_product_sales";
    public static final String CUSTOMER_SALES  = "report_customer_sales";
    public static final String TIME_SALES      = "report_time_sales";
    public static final String STORE_SALES     = "report_store_sales";
    public static final String SUPPLIER_SALES  = "report_supplier_sales";
    public static final String PRODUCT_QUALITY = "report_product_quality";

    /**
     * Вычисляет все 6 отчётов
     */
    public Map<String, Dataset<Row>> buildAndCacheAll(SparkSession spark) {
        Map<String, Dataset<Row>> reports = new LinkedHashMap<>();
        reports.put(PRODUCT_SALES,   buildProductSalesReport(spark).cache());
        reports.put(CUSTOMER_SALES,  buildCustomerSalesReport(spark).cache());
        reports.put(TIME_SALES,      buildTimeSalesReport(spark).cache());
        reports.put(STORE_SALES,     buildStoreSalesReport(spark).cache());
        reports.put(SUPPLIER_SALES,  buildSupplierSalesReport(spark).cache());
        reports.put(PRODUCT_QUALITY, buildProductQualityReport(spark).cache());

        reports.values().forEach(Dataset::count);
        return reports;
    }

    public void unpersistAll(Map<String, Dataset<Row>> reports) {
        reports.values().forEach(Dataset::unpersist);
    }

    // 1. Витрина продаж по продуктам
    private Dataset<Row> buildProductSalesReport(SparkSession spark) {
        return spark.sql(
            "SELECT product_name, category_name, total_quantity_sold, total_revenue," +
            "  avg_rating, reviews_count," +
            "  CAST(RANK() OVER (ORDER BY total_quantity_sold DESC) AS INT) AS sales_rank" +
            " FROM (" +
            "  SELECT p.product_name, cat.category_name," +
            "    CAST(SUM(f.quantity) AS BIGINT) AS total_quantity_sold," +
            "    SUM(f.total_price)              AS total_revenue," +
            "    AVG(p.rating)                   AS avg_rating," +
            "    CAST(MAX(p.reviews) AS BIGINT)  AS reviews_count" +
            "  FROM fact_sales f" +
            "  JOIN dim_product  p   ON f.product_id  = p.product_id" +
            "  JOIN dim_category cat ON p.category_id = cat.category_id" +
            "  GROUP BY p.product_name, cat.category_name" +
            " ) t"
        );
    }

    // 2. Витрина продаж по клиентам
    private Dataset<Row> buildCustomerSalesReport(SparkSession spark) {
        return spark.sql(
            "SELECT CAST(customer_id AS INT) AS customer_id, first_name, last_name, email, country," +
            "  orders_count, total_spent, avg_check," +
            "  CAST(RANK() OVER (ORDER BY total_spent DESC) AS INT) AS spending_rank" +
            " FROM (" +
            "  SELECT c.customer_id, c.first_name, c.last_name, c.email, co.country," +
            "    CAST(COUNT(f.sale_id) AS BIGINT) AS orders_count," +
            "    SUM(f.total_price)               AS total_spent," +
            "    AVG(f.total_price)               AS avg_check" +
            "  FROM fact_sales f" +
            "  JOIN dim_customer c  ON f.customer_id = c.customer_id" +
            "  JOIN dim_location l  ON c.location_id = l.location_id" +
            "  JOIN dim_country  co ON l.country_id  = co.country_id" +
            "  GROUP BY c.customer_id, c.first_name, c.last_name, c.email, co.country" +
            " ) t"
        );
    }

    // 3. Витрина продаж по времени
    private Dataset<Row> buildTimeSalesReport(SparkSession spark) {
        return spark.sql(
            "SELECT year, month, orders_count, total_revenue, avg_order_size," +
            "  SUM(total_revenue) OVER (PARTITION BY year ORDER BY month) AS cumulative_revenue" +
            " FROM (" +
            "  SELECT d.year, d.month," +
            "    CAST(COUNT(f.sale_id) AS BIGINT) AS orders_count," +
            "    SUM(f.total_price)               AS total_revenue," +
            "    AVG(f.total_price)               AS avg_order_size" +
            "  FROM fact_sales f" +
            "  JOIN dim_date d ON f.date_id = d.date_id" +
            "  GROUP BY d.year, d.month" +
            " ) t"
        );
    }

    // 4. Витрина продаж по магазинам
    private Dataset<Row> buildStoreSalesReport(SparkSession spark) {
        return spark.sql(
            "SELECT store_name, COALESCE(city, 'Unknown') AS city, country," +
            "  orders_count, total_revenue, avg_check," +
            "  CAST(RANK() OVER (ORDER BY total_revenue DESC) AS INT) AS revenue_rank" +
            " FROM (" +
            "  SELECT s.store_name, ci.city, co.country," +
            "    CAST(COUNT(f.sale_id) AS BIGINT) AS orders_count," +
            "    SUM(f.total_price)               AS total_revenue," +
            "    AVG(f.total_price)               AS avg_check" +
            "  FROM fact_sales f" +
            "  JOIN dim_store    s  ON f.store_id    = s.store_id" +
            "  JOIN dim_location l  ON s.location_id = l.location_id" +
            "  LEFT JOIN dim_city ci ON l.city_id    = ci.city_id" +
            "  JOIN dim_country  co ON l.country_id  = co.country_id" +
            "  GROUP BY s.store_name, ci.city, co.country" +
            " ) t"
        );
    }

    // 5. Витрина продаж по поставщикам
    private Dataset<Row> buildSupplierSalesReport(SparkSession spark) {
        return spark.sql(
            "SELECT supplier_name, supplier_country, orders_count, total_revenue, avg_product_price," +
            "  CAST(RANK() OVER (ORDER BY total_revenue DESC) AS INT) AS revenue_rank" +
            " FROM (" +
            "  SELECT su.name AS supplier_name, co.country AS supplier_country," +
            "    CAST(COUNT(f.sale_id) AS BIGINT) AS orders_count," +
            "    SUM(f.total_price)               AS total_revenue," +
            "    AVG(p.price)                     AS avg_product_price" +
            "  FROM fact_sales f" +
            "  JOIN dim_supplier su ON f.supplier_id  = su.supplier_id" +
            "  JOIN dim_product  p  ON f.product_id   = p.product_id" +
            "  JOIN dim_location l  ON su.location_id = l.location_id" +
            "  JOIN dim_country  co ON l.country_id   = co.country_id" +
            "  GROUP BY su.name, co.country" +
            " ) t"
        );
    }

    // 6. Витрина качества продукции
    private Dataset<Row> buildProductQualityReport(SparkSession spark) {
        Dataset<Row> productAgg = spark.sql(
            "SELECT p.product_id, p.product_name, cat.category_id, cat.category_name," +
            "  p.rating, CAST(p.reviews AS BIGINT) AS reviews_count," +
            "  CAST(SUM(f.quantity) AS BIGINT) AS total_sold," +
            "  SUM(f.total_price) AS total_revenue" +
            " FROM fact_sales f" +
            " JOIN dim_product  p   ON f.product_id  = p.product_id" +
            " JOIN dim_category cat ON p.category_id = cat.category_id" +
            " GROUP BY p.product_id, p.product_name, cat.category_id, cat.category_name, p.rating, p.reviews"
        );

        Dataset<Row> categoryCorr = spark.sql(
            "SELECT cat.category_id," +
            "  CORR(p.rating, CAST(f.quantity AS DOUBLE)) AS rating_sales_corr" +
            " FROM fact_sales f" +
            " JOIN dim_product  p   ON f.product_id  = p.product_id" +
            " JOIN dim_category cat ON p.category_id = cat.category_id" +
            " GROUP BY cat.category_id"
        );

        return productAgg
            .join(categoryCorr, "category_id", "left")
            .na().fill(0.0, new String[]{"rating_sales_corr"})
            .drop("product_id", "category_id");
    }
}
