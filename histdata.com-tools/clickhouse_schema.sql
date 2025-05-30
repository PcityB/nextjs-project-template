-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS histdata;

-- Switch to the database
USE histdata;

-- Create the exchange rates table
CREATE TABLE IF NOT EXISTS exchange_rates (
    -- Timestamp and metadata
    timestamp DateTime,
    source String,
    format String,
    timeframe String,
    fxpair String,
    
    -- M1 (1-minute) data fields
    openbid Nullable(Float64),
    highbid Nullable(Float64),
    lowbid Nullable(Float64),
    closebid Nullable(Float64),
    
    -- Tick data fields
    bidquote Nullable(Float64),
    askquote Nullable(Float64),
    
    -- Common fields
    vol Int64
) ENGINE = MergeTree()
ORDER BY (timestamp, fxpair, timeframe);

-- Create indices for common query patterns
ALTER TABLE exchange_rates ADD INDEX idx_fxpair fxpair TYPE minmax;
ALTER TABLE exchange_rates ADD INDEX idx_timeframe timeframe TYPE minmax;
