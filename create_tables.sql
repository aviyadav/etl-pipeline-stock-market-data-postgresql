CREATE TABLE IF NOT EXISTS companies (
    company_symbol VARCHAR(10) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS daily_stock_prices (
    company_symbol varchar(10),
    date DATE,
    open_price DECIMAL(15, 4) NOT NULL,
    high_price DECIMAL(15, 4) NOT NULL,
    low_price DECIMAL(15, 4) NOT NULL,
    close_price DECIMAL(15, 4) NOT NULL,
    volume INT NOT NULL,
    PRIMARY KEY (company_symbol, date),
    FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
);

CREATE TABLE IF NOT EXISTS sma_indicators (
    company_symbol varchar(10),
    date_time TIMESTAMP,
    sma_value DECIMAL(15, 4) NOT NULL,
    PRIMARY KEY (company_symbol, date_time),
    FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
);
