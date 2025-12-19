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



CREATE TABLE Dim_SectorIndustry (
    sector_id INT AUTO_INCREMENT PRIMARY KEY,
    sector VARCHAR(100),
    industry VARCHAR(150)
);

INSERT INTO Dim_SectorIndustry (sector, industry)
SELECT DISTINCT sector, industry
FROM staging_clean_data;

CREATE TABLE Dim_Company (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(200),
    sector_id INT,
    beta DECIMAL(6,3),
    FOREIGN KEY (sector_id) REFERENCES Dim_SectorIndustry(sector_id)
);

INSERT INTO Dim_Company (symbol, company_name, sector_id, beta)
SELECT DISTINCT
    s.symbol,
    s.company_name,
    d.sector_id,
    s.beta
FROM staging_clean_data s
JOIN Dim_SectorIndustry d
ON s.sector = d.sector
AND s.industry = d.industry;


CREATE TABLE Dim_Date (
    date DATE PRIMARY KEY,
    day INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT,
    weekday VARCHAR(20)
);

INSERT INTO Dim_Date
SELECT DISTINCT
    date,
    DAY(date),
    MONTH(date),
    MONTHNAME(date),
    QUARTER(date),
    YEAR(date),
    DAYNAME(date)
FROM staging_clean_data;

CREATE TABLE Dim_Metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_category VARCHAR(50),
    description VARCHAR(255)
);


INSERT INTO Dim_Metrics VALUES
(NULL,'Close Price','Price','Daily closing price'),
(NULL,'Adjusted Close','Price','Adjusted closing price'),
(NULL,'Volume','Volume','Daily traded volume'),
(NULL,'Market Cap','Valuation','Market capitalization'),
(NULL,'P/E Ratio','Valuation','Price-to-Earnings'),
(NULL,'P/B Ratio','Valuation','Price-to-Book'),
(NULL,'P/S Ratio','Valuation','Price-to-Sales'),
(NULL,'Dividend Yield','Valuation','Dividend return');


CREATE TABLE Fact_StockPrices (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    symbol VARCHAR(10),
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
    FOREIGN KEY (date) REFERENCES Dim_Date(date),
    FOREIGN KEY (symbol) REFERENCES Dim_Company(symbol)
);

INSERT INTO Fact_StockPrices (
    date,
    symbol,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    market_cap,
    pe_ratio,
    pb_ratio,
    ps_ratio,
    dividend_yield
)
SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    market_cap,
    pe_ratio,
    pb_ratio,
    ps_ratio,
    dividend_yield
FROM staging_clean_data;




DROP TABLE staging_clean_data;



SELECT COUNT(*) FROM Fact_StockPrices;
SELECT COUNT(*) FROM Dim_Company;
SELECT COUNT(*) FROM Dim_SectorIndustry;
SELECT COUNT(*) FROM Dim_Date;
SELECT COUNT(*) FROM Dim_Metrics;


SELECT f.symbol, c.company_name, d.year, f.close
FROM Fact_StockPrices f
JOIN Dim_Company c ON f.symbol = c.symbol
JOIN Dim_Date d ON f.date = d.date
LIMIT 10;
