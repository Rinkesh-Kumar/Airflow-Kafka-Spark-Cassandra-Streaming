import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import json
from cassandra.auth import PlainTextAuthProvider
import os


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Cassandra Table created successfully!")
    print("********************************************************")


def insert_data(session, **kwargs):

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        print(f"Data inserted for {first_name} {last_name}")
        print("********************************************************")

    except Exception as e:
        print(f'could not insert data due to {e}')
        print("********************************************************")


def create_spark_connection():
    s_conn = None
    conn_path = "Cred/secure-connect-user-data.zip"
    with open("Cred/user_data-token.json") as f:
        secrets = json.load(f)
    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    try:
        print("Creating spark session!")
        print("********************************************************")
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.files",conn_path)\
            .config("spark.cassandra.connection.config.cloud.path", "secure-connect-user-data.zip" )\
            .config("spark.cassandra.auth.username", CLIENT_ID) \
            .config("spark.cassandra.auth.password", CLIENT_SECRET) \
            .config("spark.dse.continuousPagingEnabled", "false")\
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
        print("********************************************************")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")
        print("********************************************************")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    kafka_config = {
    'bootstrap.servers':'pxxxxxx.xxxx.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username' : 'V2xxxxxxxxxxxxxxxxxxxx',
    'sasl.password' : '3w+T8TwoEJUxxxxxxxxxxxxxxxxxxxxxxxxxxx'
    }
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_config['bootstrap.servers']) \
            .option('subscribe', 'user_data') \
            .option('startingOffsets', 'latest') \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", 
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['sasl.username']}' password='{kafka_config['sasl.password']}';") \
            .load()

        print("kafka dataframe created successfully")
        print("********************************************************")
    except Exception as e:
        print(f"kafka dataframe could not be created because: {e}")
        print("********************************************************")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cloud_config= {'secure_connect_bundle': "Cred/secure-connect-user-data.zip"}
        with open("Cred/user_data-token.json") as f:
            secrets = json.load(f)
        CLIENT_ID = secrets["clientId"]
        CLIENT_SECRET = secrets["secret"]
        auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        cas_session = cluster.connect()
        print("Cassandra session created succesfully!")
        print("********************************************************")

        return cas_session
    except Exception as e:
        print(f"Could not create cassandra connection due to {e}")
        print("********************************************************")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return sel

def foreach_batch_function(df, epoch_id):
    # Print each micro-batch for debugging
    print("Records inserted..")
    print("\n")
    print(f"Batch {epoch_id}")
    df.show()
    print("********************************************************")

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_table(session)

            print("Streaming is being started...")
            print("********************************************************")

            streaming_query = (selection_df.writeStream.foreachBatch(foreach_batch_function)
                               .outputMode("append")
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
