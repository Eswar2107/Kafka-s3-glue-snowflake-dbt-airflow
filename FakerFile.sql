create database faker_store
create schema raw

CREATE OR REPLACE TABLE carts(
    id INT,
    userId INT,
    date TIMESTAMP,
    productId INT,
    quantity INT
);
CREATE OR REPLACE TABLE products (
    id INT,
    category STRING,
    description STRING,
    image STRING,
    price FLOAT,
    count INT,
    rate FLOAT,
    title STRING
);

CREATE OR REPLACE TABLE users (
    id INTEGER,
    email STRING,
    username STRING,
    password STRING,
    phone STRING,
    date date,
    firstname STRING,
    lastname STRING,
    city STRING,
    street STRING,
    street_number INTEGER,
    zipcode STRING,
    lat FLOAT,
    long FLOAT
);
----------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::476483571909:role/eswar_everything'
STORAGE_ALLOWED_LOCATIONS = ('s3://faker-store-data/');

desc INTEGRATION s3_int

create or replace stage s3_stage
url = 's3://faker-store-data/'
storage_integration = s3_int
file_format = parquet_type

create or replace file format parquet_type
type = parquet
---------------------------------------------------------------
LIST @s3_stage/delta_carts/data

SELECT *
FROM @s3_stage/delta_carts/data
(
  FILE_FORMAT => parquet_type,
  PATTERN => '.*\\.parquet'
);




create or replace pipe carts_pipe
auto_ingest = True
as
COPY INTO carts
FROM (
    SELECT 
        $1:id::int,
        $1:userId::int,
        $1:date::date,
        $1:productId::int,
        $1:quantity::int
    FROM @s3_stage/delta_carts/data
)
PATTERN='.*\\.parquet';

-------------------------------------------
CREATE OR REPLACE PIPE products_pipe
AUTO_INGEST = TRUE
AS
COPY INTO products
FROM (
    SELECT
        $1:id::INT,
        $1:category::STRING,
        $1:description::STRING,
        $1:image::STRING,
        $1:price::FLOAT,
        $1:count::INT,
        $1:rate::FLOAT,
        $1:title::STRING
    FROM @s3_stage/delta_products/data
)
PATTERN = '.*\\.parquet';
---------------------------------------------------------------------
CREATE OR REPLACE PIPE users_pipe
AUTO_INGEST = TRUE
AS
COPY INTO users
FROM (
    SELECT
        $1:id::INT,
        $1:email::STRING,
        $1:username::STRING,
        $1:password::STRING,
        $1:phone::STRING,
        $1:date::date,
        $1:firstname::STRING,
        $1:lastname::STRING,
        $1:city::STRING,
        $1:street::STRING,
        $1:street_number::INT,
        $1:zipcode::STRING,
        $1:lat::FLOAT,
        $1:long::FLOAT
    FROM @s3_stage/delta_users/data
)
PATTERN = '.*\\.parquet';

------------------------------------------------------------------


desc pipe carts_pipe;
desc pipe users_pipe;
desc pipe products_pipe

select * from carts
select * from users
select * from products