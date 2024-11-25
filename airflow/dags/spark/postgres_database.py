import psycopg2

DB_CONFIG = {
    'host': 'postgres1',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'UnigapPostgres@123'
}

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def process_partition(columns,insert_query,partition):
    conn =None
    try:
        conn = get_connection()       
        cursor = conn.cursor()             
        for row in partition:             
            try:    
                values = [getattr(row, col) for col in columns]
                cursor.execute(insert_query, values)
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        conn.commit()  
    except Exception as e:
        print(f"Error in partition: {e}")
        if conn:
            conn.rollback()
    finally:
        # Đóng cursor và connection khi hoàn thành
        if conn:
            cursor.close()
            conn.close()
def update_to_invalid_df(invalid_df):
    columns = ['time_stamp','ip','user_agent','store_id','local_time','current_url','referrer_url','product_id']
    column_names = ",".join(columns)
    placeholders = ','.join(['%s']*len(columns))
    insert_query = f"INSERT INTO invalid_table ({column_names}) VALUES ({placeholders})"
    invalid_df.foreachPartition(lambda partition:process_partition(columns,insert_query,partition))
def upsert_to_dim_browser(df_browser):
    columns = ['browser_id','browser_name']
    column_names = ",".join(columns)
    placeholders = ','.join(['%s']*len(columns))
    insert_query = f"INSERT INTO dim_browser ({column_names}) VALUES ({placeholders}) ON CONFLICT (browser_id) DO NOTHING"

    df_browser.foreachPartition(lambda partition:process_partition(columns,insert_query,partition))

def upsert_to_dim_os(df_os):
    columns = ['os_id','os_name']
    column_names = ",".join(columns)
    placeholders = ','.join(['%s']*len(columns))
    insert_query = f"INSERT INTO dim_os ({column_names}) VALUES ({placeholders}) ON CONFLICT (os_id) DO NOTHING"
    
    df_os.foreachPartition(lambda partition:process_partition(columns,insert_query,partition))
def upsert_to_fact_vew(df_fact):
    columns = ['id','product_id','territory_id','date_id','os_id','browser_id','current_url','referrer_url','store_id','total_view']
    column_names = ",".join(columns)
    placeholders = ','.join(['%s']*len(columns))
    insert_query = f"""
                    INSERT INTO fact_view ({column_names})
                    VALUES ({placeholders})
                    ON CONFLICT (id)
                    DO UPDATE SET total_view = fact_view.total_view + EXCLUDED.total_view;
                    """
    df_fact.foreachPartition(lambda partition:process_partition(columns,insert_query,partition))
def insert_to_dim_date(df_date):
    df_date.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres1:5432/postgres") \
        .option("dbtable", "dim_date") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()
def insert_to_dim_territory(df_territory):
    df_territory.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres1:5432/postgres") \
        .option("dbtable", "dim_territory") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()
def insert_to_dim_product(df_product):
    df_product.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres1:5432/postgres") \
        .option("dbtable", "dim_product") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()
def create_table():
    conn = get_connection()
    cur = conn.cursor()  
    query = '''
    DROP TABLE IF EXISTS dim_date CASCADE;
    CREATE TABLE dim_date(
        date_id BIGINT PRIMARY KEY,
        full_date DATE,
        day_of_week VARCHAR(10),
        day_of_week_short VARCHAR(10),
        day_of_month INT,
        year INT,
        month INT,
        hour INT
    );

    DROP TABLE IF EXISTS dim_product CASCADE;
    CREATE TABLE dim_product(
        product_id INT PRIMARY KEY,
        product_name VARCHAR(100)
    );

    DROP TABLE IF EXISTS dim_territory CASCADE;
    CREATE TABLE dim_territory(
        territory_id INT PRIMARY KEY,
        country_code VARCHAR(10),
        Country_name VARCHAR(100),         
        iso_3166_2 VARCHAR(30),          
        region VARCHAR(100),              
        sub_region VARCHAR(100),          
        intermediate_region VARCHAR(100)   
    );

    DROP TABLE IF EXISTS dim_os CASCADE;          
    CREATE TABLE dim_os(
        os_id INT PRIMARY KEY,
        os_name VARCHAR(100)
    );

    DROP TABLE IF EXISTS dim_browser CASCADE;
    CREATE TABLE dim_browser(
        browser_id INT PRIMARY KEY,
        browser_name VARCHAR(100)
    );
    DROP TABLE IF EXISTS fact_view CASCADE;
    CREATE TABLE Fact_View(
        id VARCHAR(100) PRIMARY KEY,      
        product_id INT NOT NULL,
        territory_id INT NOT NULL,
        date_id BIGINT NOT NULL,
        os_id INT NOT NULL,
        browser_id INT NOT NULL,
        current_url VARCHAR(255),         
        referrer_url VARCHAR(255),        
        store_id INT NOT NULL,
        total_view INT,
        FOREIGN KEY (product_id) REFERENCES Dim_Product(product_id),
        FOREIGN KEY (territory_id) REFERENCES Dim_Territory(territory_id),
        FOREIGN KEY (date_id) REFERENCES Dim_Date(date_id),
        FOREIGN KEY (os_id) REFERENCES Dim_Os(os_id),
        FOREIGN KEY (browser_id) REFERENCES Dim_Browser(browser_id)
    );
    DROP TABLE IF EXISTS invalid_table ;
    CREATE TABLE invalid_table(
        time_stamp BIGINT ,
        ip VARCHAR(250),
        user_agent VARCHAR(250),
        store_id INT,
        local_time TIMESTAMP,
        current_url VARCHAR(250),
        referrer_url VARCHAR(250),
        product_id INT
    );
    '''
    # Sử dụng self.cur và self.conn thay vì cur và conn
    cur.execute(query)
    conn.commit()

    # Đóng kết nối
    cur.close()
    conn.close()
    print("Table created successfully.")



# conf = Config()
# spark_conf = conf.spark_conf
# kafka_conf = conf.kafka_conf

# spark = SparkSession \
#         .builder \
#         .config(conf=spark_conf) \
#         .getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")


# # Test data
# data = [(1, "Chrome"), (2, "Firefox")]
# df = spark.createDataFrame(data, ["browser_id", "browser_name"])
# df.show()
# # Thực hiện upsert
# upsert_to_dim_browser(df)
# print("success !!")


    