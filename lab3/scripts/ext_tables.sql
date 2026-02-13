DROP EXTERNAL TABLE IF EXISTS ext_data_dictionary;

CREATE EXTERNAL TABLE ext_data_dictionary (
    "Table_name" varchar(50),
    "Field_name" varchar(50),
    "Description" varchar(256)
)
LOCATION ('pxf://data_dictionary?PROFILE=JDBC&SERVER=mssql')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext_tdf_finishers;

CREATE EXTERNAL TABLE ext_tdf_finishers (
  "Year" int,
  "Rank" varchar(10),
  "Rider" varchar(50),
  "Time" varchar(50),
  "Team" varchar(50)
)
LOCATION ('pxf://tdf_finishers?PROFILE=JDBC&SERVER=mssql')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext_tdf_stages;

CREATE EXTERNAL TABLE ext_tdf_stages (
  "Year" int,
  "Date" varchar(50),
  "Stage" varchar(10),
  "Course" varchar(256),
  "Distance" varchar(50),
  "Type" varchar(50),
  "Winner" varchar(50)
)
LOCATION ('pxf://tdf_stages?PROFILE=JDBC&SERVER=mssql')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext_tdf_tours;

CREATE EXTERNAL TABLE ext_tdf_tours (
    "Year" INT,
    "Dates" varchar(50),
    "Stages" varchar(50),
    "Distance" varchar(50),
    "Starters" INT,
    "Finishers" INT
)
LOCATION ('pxf://tdf_tours?PROFILE=JDBC&SERVER=mssql')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext_tdf_winners;

CREATE EXTERNAL TABLE ext_tdf_winners (
  "Year" int,
  "Country" varchar(50),
  "Rider" varchar(50),
  "Team" varchar(50),
  "Time" varchar(50),
  "Margin" varchar(50),
  "Stages_Won" int,
  "Stages_Led" int,
  "Avg_Speed" varchar(50),
  "Height" varchar(50),
  "Weight" varchar(50),
  "Born" varchar(50),
  "Died" varchar(50)
)
LOCATION ('pxf://tdf_winners?PROFILE=JDBC&SERVER=mssql')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext_orders;

CREATE EXTERNAL TABLE ext_orders (
    "order_date" TIMESTAMP,
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "status" VARCHAR(100),
    "paid" VARCHAR(10),
    "declined" VARCHAR(10),
    "shipped" VARCHAR(10),
    "product_id" INTEGER,
    "quantity" INTEGER
)
LOCATION ('gpfdist://gpfdist-server:8080/external/orders.csv')
FORMAT 'CSV' (
    DELIMITER ';'
    NULL ''
)
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 2 ROWS;

