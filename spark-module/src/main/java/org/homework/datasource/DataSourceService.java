package org.homework.datasource;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DataSourceService {

    private final String pgUrl;
    private final Properties pgProps;

    public DataSourceService(String pgUrl, Properties pgProps) {
        this.pgUrl = pgUrl;
        this.pgProps = pgProps;
    }

    /**
     * Загружает все таблицы снежинки из PostgreSQL
     */
    public void loadSnowflakeViews(SparkSession spark) {
        // Маленькие справочники — читаем в одну партицию
        String[] dimTables = {
            "dim_product", "dim_customer", "dim_seller", "dim_store",
            "dim_supplier", "dim_date", "dim_category", "dim_pet_category",
            "dim_brand", "dim_location", "dim_country", "dim_city",
            "dim_pet", "dim_pet_type", "dim_pet_breed"
        };
        for (String t : dimTables) {
            spark.read().jdbc(pgUrl, t, pgProps).createOrReplaceTempView(t);
        }

        // fact_sales — читаем с партиционированием по sale_id
        spark.read()
            .jdbc(pgUrl, "fact_sales", "sale_id", 1, 10000, 4, pgProps)
            .createOrReplaceTempView("fact_sales");
    }
}
