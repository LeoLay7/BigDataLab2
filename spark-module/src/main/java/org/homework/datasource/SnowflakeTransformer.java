package org.homework.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SnowflakeTransformer {

    private final String pgUrl;
    private final Properties pgProps;

    public SnowflakeTransformer(String pgUrl, Properties pgProps) {
        this.pgUrl = pgUrl;
        this.pgProps = pgProps;
    }

    public void transform(SparkSession spark, Dataset<Row> data) {
        data.createOrReplaceTempView("mock_data");

        // ── dim_country ──────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY country) AS INT) AS country_id, country" +
            " FROM (" +
            "   SELECT DISTINCT country FROM (" +
            "     SELECT customer_country AS country FROM mock_data" +
            "     UNION SELECT seller_country   FROM mock_data" +
            "     UNION SELECT store_country    FROM mock_data" +
            "     UNION SELECT supplier_country FROM mock_data" +
            "   ) t WHERE country IS NOT NULL AND country <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_country", pgProps);
        readPg(spark, "dim_country").createOrReplaceTempView("dim_country");

        // ── dim_city ─────────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY city, state) AS INT) AS city_id, city, state" +
            " FROM (" +
            "   SELECT DISTINCT city, state FROM (" +
            "     SELECT store_city AS city, store_state AS state FROM mock_data" +
            "     UNION SELECT supplier_city, NULL AS state FROM mock_data" +
            "   ) t WHERE city IS NOT NULL AND city <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_city", pgProps);
        readPg(spark, "dim_city").createOrReplaceTempView("dim_city");

        // ── dim_location ─────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY country_id, city_id, postal_code, address) AS INT) AS location_id," +
            "  country_id, city_id, postal_code, address" +
            " FROM (" +
            "   SELECT DISTINCT dc.country_id, CAST(NULL AS INT) AS city_id, m.customer_postal_code AS postal_code, CAST(NULL AS STRING) AS address" +
            "   FROM mock_data m JOIN dim_country dc ON m.customer_country = dc.country" +
            "   UNION" +
            "   SELECT DISTINCT dc.country_id, CAST(NULL AS INT) AS city_id, m.seller_postal_code AS postal_code, CAST(NULL AS STRING) AS address" +
            "   FROM mock_data m JOIN dim_country dc ON m.seller_country = dc.country" +
            "   UNION" +
            "   SELECT DISTINCT dc.country_id, dci.city_id, CAST(NULL AS STRING) AS postal_code, m.store_location AS address" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc  ON m.store_country = dc.country" +
            "   JOIN dim_city dci    ON m.store_city = dci.city" +
            "    AND (m.store_state = dci.state OR (m.store_state IS NULL AND dci.state IS NULL))" +
            "   UNION" +
            "   SELECT DISTINCT dc.country_id, dci.city_id, CAST(NULL AS STRING) AS postal_code, m.supplier_address AS address" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc  ON m.supplier_country = dc.country" +
            "   JOIN dim_city dci    ON m.supplier_city = dci.city" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_location", pgProps);
        readPg(spark, "dim_location").createOrReplaceTempView("dim_location");

        // ── dim_pet_type ─────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY type) AS INT) AS pet_type_id, type" +
            " FROM (" +
            "   SELECT DISTINCT customer_pet_type AS type FROM mock_data" +
            "   WHERE customer_pet_type IS NOT NULL AND customer_pet_type <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_pet_type", pgProps);
        readPg(spark, "dim_pet_type").createOrReplaceTempView("dim_pet_type");

        // ── dim_pet_breed ────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY breed) AS INT) AS pet_breed_id, breed" +
            " FROM (" +
            "   SELECT DISTINCT customer_pet_breed AS breed FROM mock_data" +
            "   WHERE customer_pet_breed IS NOT NULL AND customer_pet_breed <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_pet_breed", pgProps);
        readPg(spark, "dim_pet_breed").createOrReplaceTempView("dim_pet_breed");

        // ── dim_pet ──────────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY pet_type_id, pet_breed_id) AS INT) AS pet_id," +
            "  pet_type_id, pet_breed_id" +
            " FROM (" +
            "   SELECT DISTINCT dpt.pet_type_id, dpb.pet_breed_id" +
            "   FROM mock_data m" +
            "   JOIN dim_pet_type dpt  ON m.customer_pet_type  = dpt.type" +
            "   JOIN dim_pet_breed dpb ON m.customer_pet_breed = dpb.breed" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_pet", pgProps);
        readPg(spark, "dim_pet").createOrReplaceTempView("dim_pet");

        // ── dim_category ─────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY category_name) AS INT) AS category_id, category_name" +
            " FROM (" +
            "   SELECT DISTINCT product_category AS category_name FROM mock_data" +
            "   WHERE product_category IS NOT NULL AND product_category <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_category", pgProps);
        readPg(spark, "dim_category").createOrReplaceTempView("dim_category");

        // ── dim_pet_category ─────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY category_name) AS INT) AS pet_category_id, category_name" +
            " FROM (" +
            "   SELECT DISTINCT pet_category AS category_name FROM mock_data" +
            "   WHERE pet_category IS NOT NULL AND pet_category <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_pet_category", pgProps);
        readPg(spark, "dim_pet_category").createOrReplaceTempView("dim_pet_category");

        // ── dim_brand ────────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY brand_name) AS INT) AS brand_id, brand_name" +
            " FROM (" +
            "   SELECT DISTINCT product_brand AS brand_name FROM mock_data" +
            "   WHERE product_brand IS NOT NULL AND product_brand <> ''" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_brand", pgProps);
        readPg(spark, "dim_brand").createOrReplaceTempView("dim_brand");

        // ── dim_date ─────────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY full_date) AS INT) AS date_id, full_date," +
            "  year(full_date) AS year, month(full_date) AS month, dayofweek(full_date) AS day_of_week" +
            " FROM (" +
            "  SELECT DISTINCT full_date FROM (" +
            "    SELECT to_date(sale_date, 'M/d/yyyy') AS full_date FROM mock_data" +
            "    UNION SELECT to_date(product_release_date, 'M/d/yyyy') FROM mock_data" +
            "    UNION SELECT to_date(product_expiry_date, 'M/d/yyyy') FROM mock_data" +
            "  ) t WHERE full_date IS NOT NULL" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_date", pgProps);
        readPg(spark, "dim_date").createOrReplaceTempView("dim_date");

        // ── dim_customer ─────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(sale_customer_id AS INT) AS customer_id, first_name, last_name, age, email, location_id, pet_id, pet_name" +
            " FROM (" +
            "   SELECT m.sale_customer_id, m.customer_first_name AS first_name, m.customer_last_name AS last_name," +
            "     m.customer_age AS age, m.customer_email AS email, dl.location_id, dp.pet_id," +
            "     m.customer_pet_name AS pet_name," +
            "     ROW_NUMBER() OVER (PARTITION BY m.sale_customer_id ORDER BY m.id) AS rn" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc ON m.customer_country = dc.country" +
            "   JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id IS NULL" +
            "    AND (dl.postal_code = m.customer_postal_code" +
            "      OR (dl.postal_code IS NULL AND (m.customer_postal_code IS NULL OR m.customer_postal_code = '')))" +
            "   JOIN dim_pet_type dpt ON m.customer_pet_type = dpt.type" +
            "   JOIN dim_pet_breed dpb ON m.customer_pet_breed = dpb.breed" +
            "   JOIN dim_pet dp ON dp.pet_type_id = dpt.pet_type_id AND dp.pet_breed_id = dpb.pet_breed_id" +
            " ) t WHERE rn = 1"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_customer", pgProps);
        readPg(spark, "dim_customer").createOrReplaceTempView("dim_customer");

        // ── dim_seller ───────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(sale_seller_id AS INT) AS seller_id, first_name, last_name, email, location_id" +
            " FROM (" +
            "   SELECT m.sale_seller_id, m.seller_first_name AS first_name, m.seller_last_name AS last_name," +
            "     m.seller_email AS email, dl.location_id," +
            "     ROW_NUMBER() OVER (PARTITION BY m.sale_seller_id ORDER BY m.id) AS rn" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc ON m.seller_country = dc.country" +
            "   JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id IS NULL" +
            "    AND (dl.postal_code = m.seller_postal_code" +
            "      OR (dl.postal_code IS NULL AND (m.seller_postal_code IS NULL OR m.seller_postal_code = '')))" +
            " ) t WHERE rn = 1"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_seller", pgProps);
        readPg(spark, "dim_seller").createOrReplaceTempView("dim_seller");

        // ── dim_store ────────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY store_name, email) AS INT) AS store_id," +
            "  store_name, location_id, phone, email" +
            " FROM (" +
            "   SELECT DISTINCT m.store_name, dl.location_id, m.store_phone AS phone, m.store_email AS email" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc ON m.store_country = dc.country" +
            "   JOIN dim_city dci ON m.store_city = dci.city" +
            "    AND (m.store_state = dci.state OR (m.store_state IS NULL AND dci.state IS NULL))" +
            "   JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id = dci.city_id" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_store", pgProps);
        readPg(spark, "dim_store").createOrReplaceTempView("dim_store");

        // ── dim_supplier ─────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY name, email) AS INT) AS supplier_id," +
            "  name, contact, email, phone, location_id" +
            " FROM (" +
            "   SELECT DISTINCT m.supplier_name AS name, m.supplier_contact AS contact," +
            "     m.supplier_email AS email, m.supplier_phone AS phone, dl.location_id" +
            "   FROM mock_data m" +
            "   JOIN dim_country dc ON m.supplier_country = dc.country" +
            "   JOIN dim_city dci ON m.supplier_city = dci.city" +
            "   JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id = dci.city_id" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_supplier", pgProps);
        readPg(spark, "dim_supplier").createOrReplaceTempView("dim_supplier");

        // ── dim_product ──────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY product_name, price) AS INT) AS product_id," +
            "  product_name, category_id, pet_category_id, brand_id, price, quantity, weight, color," +
            "  size, material, description, rating, reviews, release_date_id, expiry_date_id" +
            " FROM (" +
            "   SELECT DISTINCT m.product_name, dc.category_id, dpc.pet_category_id, db.brand_id," +
            "     m.product_price AS price, m.product_quantity AS quantity, m.product_weight AS weight," +
            "     m.product_color AS color, m.product_size AS size, m.product_material AS material," +
            "     m.product_description AS description, m.product_rating AS rating, m.product_reviews AS reviews," +
            "     ddr.date_id AS release_date_id, dde.date_id AS expiry_date_id" +
            "   FROM mock_data m" +
            "   JOIN dim_category dc ON m.product_category = dc.category_name" +
            "   JOIN dim_pet_category dpc ON m.pet_category = dpc.category_name" +
            "   JOIN dim_brand db ON m.product_brand = db.brand_name" +
            "   JOIN dim_date ddr ON ddr.full_date = to_date(m.product_release_date, 'M/d/yyyy')" +
            "   JOIN dim_date dde ON dde.full_date = to_date(m.product_expiry_date, 'M/d/yyyy')" +
            " ) s"
        ).write().mode("overwrite").jdbc(pgUrl, "dim_product", pgProps);
        readPg(spark, "dim_product").createOrReplaceTempView("dim_product");

        // ── fact_sales ───────────────────────────────────────────────────────
        spark.sql(
            "SELECT CAST(ROW_NUMBER() OVER (ORDER BY m.id, m.sale_date, m.sale_customer_id, m.sale_seller_id, m.product_name) AS BIGINT) AS sale_id," +
            "  dd.date_id, dc.customer_id, ds.seller_id, dp.product_id, dst.store_id, dsu.supplier_id," +
            "  CAST(m.sale_quantity AS INT) AS quantity, CAST(m.sale_total_price AS DOUBLE) AS total_price" +
            " FROM mock_data m" +
            " JOIN dim_date dd ON dd.full_date = to_date(m.sale_date, 'M/d/yyyy')" +
            " JOIN dim_customer dc ON dc.customer_id = m.sale_customer_id" +
            " JOIN dim_seller ds ON ds.seller_id = m.sale_seller_id" +
            " JOIN dim_product dp ON dp.product_name = m.product_name AND dp.price = m.product_price" +
            " JOIN dim_store dst ON dst.store_name = m.store_name AND dst.email = m.store_email" +
            " JOIN dim_supplier dsu ON dsu.name = m.supplier_name AND dsu.email = m.supplier_email"
        ).write().mode("overwrite").jdbc(pgUrl, "fact_sales", pgProps);

        System.out.println("Снежинка загружена в PostgreSQL");
    }

    private Dataset<Row> readPg(SparkSession spark, String table) {
        return spark.read().jdbc(pgUrl, table, pgProps);
    }
}
