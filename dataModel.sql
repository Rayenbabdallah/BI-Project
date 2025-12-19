
SET GLOBAL local_infile = 1;

CREATE DATABASE IF NOT EXISTS stock_bi;

USE stock_bi;

CREATE TABLE staging_clean_data (
    symbol VARCHAR(10),
    company_name VARCHAR(200),
    sector VARCHAR(100),
    industry VARCHAR(150),
    market_cap BIGINT,
    beta DECIMAL(6,3),
    dividend_yield DECIMAL(6,2),
    pe_ratio DECIMAL(10,2),
    pb_ratio DECIMAL(10,2),
    ps_ratio DECIMAL(10,2),
    date DATE,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    adj_close DECIMAL(10,2)
);


LOAD DATA LOCAL INFILE 'C:/Users/rayen/Desktop/BI project/clean_data.csv'
INTO TABLE staging_clean_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(symbol, company_name, sector, industry, market_cap, beta, dividend_yield,
 pe_ratio, pb_ratio, ps_ratio, @date,
 open, high, low, close, volume, adj_close)
SET date = STR_TO_DATE(@date, '%d/%m/%Y');


CREATE TABLE Dim_Company (
    company_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    company_name VARCHAR(200),
    beta DECIMAL(6,3)
);

INSERT INTO Dim_Company (symbol, company_name, beta)
SELECT DISTINCT symbol, company_name, beta
FROM staging_clean_data;


CREATE TABLE Dim_Sector (
    sector_id INT AUTO_INCREMENT PRIMARY KEY,
    sector VARCHAR(100),
    industry VARCHAR(150)
);

INSERT INTO Dim_Sector (sector, industry)
SELECT DISTINCT sector, industry
FROM staging_clean_data;


CREATE TABLE Dim_Date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    weekday VARCHAR(20)
);

INSERT INTO Dim_Date (full_date, day, month, quarter, year, weekday)
SELECT DISTINCT
    date,
    DAY(date),
    MONTH(date),
    QUARTER(date),
    YEAR(date),
    DAYNAME(date)
FROM staging_clean_data;


CREATE TABLE Dim_MetricGroup (
    metric_group_id INT AUTO_INCREMENT PRIMARY KEY,
    metric_group_name VARCHAR(50)
);

INSERT INTO Dim_MetricGroup (metric_group_name) VALUES
('Price'),
('Valuation'),
('Volume'),
('Dividend');


CREATE TABLE Fact_StockPrices (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    company_id INT,
    sector_id INT,
    date_id INT,
    metric_group_id INT,

    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    adj_close DECIMAL(10,2),
    volume BIGINT,

    market_cap BIGINT,
    pe_ratio DECIMAL(10,2),
    pb_ratio DECIMAL(10,2),
    ps_ratio DECIMAL(10,2),
    dividend_yield DECIMAL(6,2),

    FOREIGN KEY (company_id) REFERENCES Dim_Company(company_id),
    FOREIGN KEY (sector_id) REFERENCES Dim_Sector(sector_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Date(date_id),
    FOREIGN KEY (metric_group_id) REFERENCES Dim_MetricGroup(metric_group_id)
);


INSERT INTO Fact_StockPrices (
    company_id,
    sector_id,
    date_id,
    metric_group_id,
    open, high, low, close, adj_close,
    volume,
    market_cap,
    pe_ratio,
    pb_ratio,
    ps_ratio,
    dividend_yield
)
SELECT
    c.company_id,
    s.sector_id,
    d.date_id,
    1 AS metric_group_id,   -- Price group (semantic)
    sc.open,
    sc.high,
    sc.low,
    sc.close,
    sc.adj_close,
    sc.volume,
    sc.market_cap,
    sc.pe_ratio,
    sc.pb_ratio,
    sc.ps_ratio,
    sc.dividend_yield
FROM staging_clean_data sc
JOIN Dim_Company c
    ON sc.symbol = c.symbol
JOIN Dim_Sector s
    ON sc.sector = s.sector
   AND sc.industry = s.industry
JOIN Dim_Date d
    ON sc.date = d.full_date;


DROP TABLE staging_clean_data;


SELECT * FROM dim_metricgroup ;