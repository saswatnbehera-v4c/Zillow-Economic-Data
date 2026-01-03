import dlt
from pyspark.sql.functions import *

# 1. Dimension Table: Location 
@dlt.table(name="dim_location", comment="Geographic dimension for real estate metrics")
@dlt.expect_or_drop("valid_region", "region_name IS NOT NULL")
@dlt.expect("valid_state", "state_name IS NOT NULL")
def dim_location():
    return spark.read.table("zillow.silver.county_crosswalk_metrics") \
        .select("region_name", "state_fips", "county_fips", "county_name", 
                "state_name", "city", "metro_name_zillow") \
        .distinct()

# 2. Fact Table: Real Estate Metrics
@dlt.table(name="fact_metrics", comment="Fact table containing housing market metrics")
@dlt.expect_or_fail("valid_date", "date IS NOT NULL")
@dlt.expect("positive_price", "zhvi_all_homes > 0")
def fact_metrics():
    return spark.read.table("zillow.silver.county_crosswalk_metrics") \
        .select("date", "region_name", "median_listing_price_all_homes", 
                "median_rental_price_all_homes", "median_rental_price_per_sqft_all_homes",
                "zhvi_all_homes", "zri_all_homes", "sale_counts")


# 3. Aggregation: Monthly Price Trend
@dlt.table(name="gold_monthly_price_trend")
def monthly_trend():
    return dlt.read("fact_metrics").alias("f") \
        .join(dlt.read("dim_location").alias("l"), "region_name") \
        .groupBy(month(col("date")).alias("month"), 
                 year(col("date")).alias("year"), 
                 "state_name") \
        .agg(avg("zhvi_all_homes").alias("avg_house_value"))


@dlt.table(name="gold_top_10_counties_by_value")
def top_10_counties():
    return dlt.read("fact_metrics").alias("f") \
        .join(dlt.read("dim_location").alias("l"), "region_name") \
        .groupBy("county_name", "state_name") \
        .agg(max("zhvi_all_homes").alias("max_zhvi")) \
        .orderBy(desc("max_zhvi")) \
        .limit(10)