-- Connect to your database before running this
-- Create copies of existing tables
DROP TABLE IF EXISTS stocks1;
DROP TABLE IF EXISTS stocks2;

SELECT * INTO stocks1 FROM stock_prices_2;
SELECT * INTO stocks2 FROM stock_prices_4;

-- Perform updates
UPDATE stocks1 SET stock_symbol = 'TSLA' WHERE stock_symbol = 'ABC';
UPDATE stocks1 SET stock_symbol = 'AMZN' WHERE stock_symbol = 'LMN';
UPDATE stocks1 SET stock_symbol = 'HPE'  WHERE stock_symbol = 'XYZ';

UPDATE stocks2 SET stock_symbol = 'TSLA' WHERE stock_symbol = 'ABC';
UPDATE stocks2 SET stock_symbol = 'AMZN' WHERE stock_symbol = 'LMN';
UPDATE stocks2 SET stock_symbol = 'HPE'  WHERE stock_symbol = 'XYZ';
