                                                                       ***ECOMMERCE-360-DEGREE-VIEW-FABRIC-PROJECT***                                                     

***DATAPIPELINE***

<img width="461" height="252" alt="final pipeline" src="https://github.com/user-attachments/assets/e6cb4758-21c8-4f97-9edd-98aeb91605d0" />

***FINAL DASHBOARD***

<img width="876" height="502" alt="D1" src="https://github.com/user-attachments/assets/7531adfe-0144-48f7-a979-d345321ab844" />

<img width="889" height="496" alt="D2" src="https://github.com/user-attachments/assets/c22a9f96-694d-40e0-9db5-8e8240a8e88f" />



***PYSPARK Notebook 1 project1***

***loading Bronze data***

customers_raw=spark.read.parquet("abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/1a49c4d3-a3e2-4ff9-a637-24c27f990123/Files/BRONZE/customers.parquet")

orders_raw=spark.read.parquet("abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/1a49c4d3-a3e2-4ff9-a637-24c27f990123/Files/BRONZE/orders.parquet")

payments_raw=spark.read.parquet("abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/1a49c4d3-a3e2-4ff9-a637-24c27f990123/Files/BRONZE/payments.parquet")

support_raw=spark.read.parquet("abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/1a49c4d3-a3e2-4ff9-a637-24c27f990123/Files/BRONZE/support_tickets.parquet")

webactivity_raw=spark.read.parquet("abfss://e0cf1801-1533-430a-8805-b23abd9dafae@onelake.dfs.fabric.microsoft.com/1a49c4d3-a3e2-4ff9-a637-24c27f990123/Files/BRONZE/web_activities.parquet")

***DISPLAY DF***

display(customers_raw.limit(5))
display(orders_raw.limit(5))
display(payments_raw.limit(5))
display(support_raw.limit(5))
display(webactivity_raw.limit(5))

***SAVING TO BRONZE LAYER DELTA FILES*** 

customers_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_customer")
orders_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
payments_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_payments")
support_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_support")
webactivity_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_webactivity")

***DATA CLEANING FOR LAYER***

from pyspark.sql.functions import*
from pyspark.sql.types import*

***BRONZE CUSTOMER TO SILVER CUSTOMER***

%%sql
select * from bronze_customer limit 5

customer_raw=spark.table("bronze_customer")


customers_clean =

(

customer_raw
   
    .withColumn("Name", initcap(trim(col("name"))))
    
    .withColumn("email", lower(trim(col("EMAIL"))))
    
    .withColumn(
        "Gender",
        when(lower(col("gender")).isin("f", "female"), "Female")
        .when(lower(col("gender")).isin("m", "male"), "Male")
        .otherwise("Neutral")
    )
    
    .withColumn(
        "DOB",
        to_date(regexp_replace(col("dob"), "/", "-"))
    )
    .withColumn("Location", initcap(lower(col("location"))))
    .dropDuplicates(subset=["customer_id"])
    .dropna(subset=["customer_id", "email"]))

display(customers_clean.limit(6))

***BRONZE ORDER TO SILVER ORDER***

%%sql

select * from bronze_orders limit 5

orders_raw= spark.table("bronze_orders")

orders_clean = 

(
    orders_raw
    
    # Clean Order_date with multiple patterns
    
    .withColumn(
        "Order_date",
        when(
            col("order_date").rlike(r"^\d{4}/\d{2}/\d{2}$"),
            to_date(col("order_date"), "yyyy/MM/dd")
        )
        
        .when(
            col("order_date").rlike(r"^\d{2}-\d{2}-\d{4}$"),
            to_date(col("order_date"), "dd-MM-yyyy")
        )
        
        .when(
            col("order_date").rlike(r"^\d{8}$"),
            to_date(col("order_date"), "yyyyMMdd")
        )
        
        .otherwise(to_date(col("order_date"), "yyyy-MM-dd"))
    )

    # Cast Amount
    .withColumn("Amount", col("amount").cast(DoubleType()))

    # Fix invalid Amount: replace negative/zero with null
    .withColumn(
        "Amount",
        when(col("Amount") < 0, None).otherwise(col("Amount"))
    )

    # Clean Status column
    .withColumn("Status", initcap(lower(col("status"))))

    # Remove duplicate Orders
    .dropDuplicates(subset=["customer_id", "order_id"])

    # Drop rows with missing order_id
    .dropna(subset=["order_id"])
)

display(orders_clean.limit(5))

***BRONZE PAYYMENTS  TO SILVER PAYMENTS***

%%sql
select * from bronze_payments limit 5

payment_raw= spark.table("bronze_payments")

payments_clean = 

(
    payment_raw
    
    # Convert payment_date → replace "/" with "-" → to_date()
    
    .withColumn(
        "payment_date",
        to_date(regexp_replace(col("payment_date"), "/", "-"))
    )

    # Clean payment method
    .withColumn("payment_method", initcap(col("payment_method")))
    .replace({"Creditcard": "Credit Card"}, subset=["payment_method"])

    # Clean payment status
    .withColumn("payment_status", initcap(col("payment_status")))

    # Cast Amount to double
    .withColumn("Amount", col("amount").cast(DoubleType()))

    # Fix negative amount → replace with null
    .withColumn(
        "Amount",
        when(col("Amount") < 0, None).otherwise(col("Amount"))
    )

    # Drop rows with missing required fields
    .dropna(subset=["customer_id", "payment_date", "Amount"])
)

display(payments_clean.limit(5))

***BRONZE SUPPORT TO SILVER SUPPORT***

%%sql
select * from bronze_support limit 5

support_raw= spark.table("bronze_support")

support_clean = (
    support_raw
    
    .withColumn("issue_type", initcap(trim(col("issue_type"))))
    
    .withColumn("resolution_status", initcap(trim(col("resolution_status"))))

    # Clean ticket_date with multiple patterns
    .withColumn(
    
        "ticket_date",  
        
         to_date(regexp_replace(col("ticket_date"),"/","-"))  # fallback
    )

    # Replace NA/blank
    .replace({"NA": None, "": None}, subset=["issue_type", "resolution_status"])

    # Deduplicate
    .dropDuplicates(subset=["ticket_id"])

    # Remove rows with missing keys
    .dropna(subset=["customer_id", "ticket_date","resolution_status"])
)

display(support_clean.limit(5))

***BRONZE WEB ACTIVITY TO SILVER WEB ACTIVITY***

%%sql 
select * from bronze_webactivity limit 5

web_raw = spark.table("bronze_webactivity")

web_clean = (

    web_raw

    .withColumn("session_time", to_date(regexp_replace(col("session_time"), "/", "-")))
    
    .withColumn("page_viewed", lower(col("page_viewed")))
    
    .withColumn("device_type", initcap(col("device_type")))
    
    .dropDuplicates(["session_id"])
   
    .dropna(subset=["customer_id", "session_time", "page_viewed"])
    
)



display(web_clean.limit(5))

***BRONZE LAYER DELTA TO SILVER LAYER DELTA TABLES***

customers_clean.write.format("delta").mode("overwrite").saveAsTable("silver_customer")
orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver_order")
payments_clean.write.format("delta").mode("overwrite").saveAsTable("silver_payments")
support_clean.write.format("delta").mode("overwrite").saveAsTable("silver_support")
web_clean.write.format("delta").mode("overwrite").saveAsTable("silver_web_activity")

***GOLD LAYER***

cust=spark.table("silver_customer").alias("c")

ord=spark.table("silver_order").alias("o")

pay=spark.table("silver_payments").alias("p")

sup=spark.table("silver_support").alias("s")

web=spark.table("silver_web_activity").alias("w")

customerGOLD = (

    cust
    .join(ord, "customer_id", "left")
    .join(pay, "customer_id", "left")
    .join(sup, "customer_id", "left")
    .join(web, "customer_id", "left")
    .select(
        col("c.customer_id"),
        col("c.Name"),
        col("c.email"),
        col("c.Gender"),
        col("c.DOB"),
        col("c.Location"),

        col("o.order_id"),
        col("o.Order_date"),
        col("o.Amount").alias("order_amount"),
        col("o.Status").alias("order_status"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.Amount").alias("payment_amount"),

        col("s.ticket_id"),
        col("s.issue_type"),
        col("s.ticket_date"),
        col("s.resolution_status"),

        col("w.page_viewed"),
        col("w.device_type"),
        col("w.session_time")
    )
)

display(customerGOLD.limit(10))

customerGOLD.write.format("delta").mode("overwrite").saveAsTable("GOLD_customers")

