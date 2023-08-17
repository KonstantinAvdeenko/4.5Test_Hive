-- создадим сначала временные таблицы для корректной загрузки
-- по партициям и бакетам
CREATE TEMPORARY TABLE IF NOT EXISTS customers_temp (
    index INT, customer_id STRING, first_name STRING, last_name STRING,
    company STRING, city STRING, country STRING, phone_1 STRING,
    phone_2 STRING, email STRING, subscription_date DATE, website STRING,
    range_data INT, subscription_year INT
)
-- задаём параметры считывания файла
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
-- эта характеристика таблицы позволяет пропускать заголовки в источниках
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TEMPORARY TABLE IF NOT EXISTS people_temp (
    index INT, user_id STRING, first_name STRING, last_name STRING,
    sex STRING, email STRING, phone STRING, date_of_birth DATE,
    job_title STRING, range_data INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TEMPORARY TABLE IF NOT EXISTS organizations_temp (
    index INT, organization_id STRING, name STRING, website STRING,
    country STRING, description STRING, founded INT, industry STRING,
    number_of_employees INT, range_data INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

---------------------------------------------------------------------

-- загруза данных во временные таблицы
LOAD DATA INPATH '/user/hadoop/csv_files/customers.csv' OVERWRITE INTO TABLE customers_temp;
LOAD DATA INPATH '/user/hadoop/csv_files/people.csv' OVERWRITE INTO TABLE people_temp;
LOAD DATA INPATH '/user/hadoop/csv_files/organizations.csv' OVERWRITE INTO TABLE organizations_temp;

---------------------------------------------------------------------

-- далее создадим постоянные (в том числе партиционированные и бакетированные) таблицы
CREATE TABLE IF NOT EXISTS customers (
    index INT, customer_id STRING, first_name STRING, last_name STRING,
    company STRING, city STRING, country STRING, phone_1 STRING,
    phone_2 STRING, email STRING, subscription_date DATE, website STRING,
    range_data INT
)
-- партиционируем по году подписки
PARTITIONED BY (subscription_year int)
-- бакетируем по 3 столбцам, по которым будем джойнить
CLUSTERED BY(first_name, last_name, email) INTO 10 BUCKETS
-- файлы будем хранить в формате parquet
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS people (
    index INT, user_id STRING, first_name STRING, last_name STRING,
    sex STRING, email STRING, phone STRING, date_of_birth DATE,
    job_title STRING, range_data INT
)
-- здесь просто забакетируем по столбца для join-запросов
CLUSTERED BY(first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS organizations (
    index INT, organization_id STRING, name STRING, website STRING,
    country STRING, description STRING, founded INT, industry STRING,
    number_of_employees INT, range_data INT
)
STORED AS PARQUET;

---------------------------------------------------------------------

-- переливаем данные из временных таблиц в постоянные с учётом партиций
-- customer
INSERT INTO TABLE customers PARTITION(subscription_year=2020)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1,
       phone_2, email, subscription_date, website,
       range_data
FROM customers_temp WHERE subscription_year=2020;

INSERT INTO TABLE customers PARTITION(subscription_year=2021)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1,
       phone_2, email, subscription_date, website,
       range_data
FROM customers_temp WHERE subscription_year=2021;

INSERT INTO TABLE customers PARTITION(subscription_year=2022)
SELECT index, customer_id, first_name, last_name,
       company, city, country, phone_1,
       phone_2, email, subscription_date, website,
       range_data
FROM customers_temp WHERE subscription_year=2022;

-- people
INSERT INTO TABLE people
SELECT * FROM people_temp;

-- organizations
INSERT INTO TABLE organizations
SELECT * FROM organizations_temp;

---------------------------------------------------------------------

-- итоговая витрина, которая выгрузит данные в разрезе
-- Компания-Год_подписки-Возраст-Количество_подписок, по которой аналитики смогут
-- выделить для себя необходимые группы
CREATE TABLE subscribe_statistics AS

WITH
customers_union AS (
    -- в безопасном режиме партиционированные таблицы достаются
    -- только в разрезе отдельных партиций
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2020
    UNION
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2021
    UNION
    SELECT first_name, last_name, email, customer_id, company, subscription_date, subscription_year
    FROM customers
    WHERE subscription_year = 2022
)

SELECT s.company, s.subscription_year, s.age, COUNT(s.customer_id) AS cnt_subscribes
FROM (
    SELECT c.customer_id, c.company, c.subscription_year,
           round((datediff(c.subscription_date, p.date_of_birth) / 365.3), 0) AS age
    FROM customers_union c
    JOIN people p ON (
        c.first_name = p.first_name AND c.last_name = p.last_name AND c.email = p.email
    )
) AS s
GROUP BY s.subscription_year, s.company, s.age;