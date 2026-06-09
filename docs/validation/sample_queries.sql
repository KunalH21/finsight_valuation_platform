-- Top valuation multiples

SELECT
ticker,
ev_to_ebitda
FROM MARTS.MART_VALUATION_MULTIPLES
ORDER BY ev_to_ebitda DESC;


-- Sector comparison

SELECT *
FROM MARTS.MART_SECTOR_COMPS;


-- Company trend

SELECT *
FROM MARTS.MART_COMPANY_TREND
WHERE ticker='AAPL';