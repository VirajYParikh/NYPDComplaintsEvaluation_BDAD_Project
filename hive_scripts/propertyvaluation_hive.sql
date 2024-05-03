-- Average property values per zip per year
CREATE TABLE average_property_values_per_year
AS
SELECT YEAR,
       ZIP_CODE,
       AVG(CURACTTOT) AS average_actual_total_value,
       AVG(CURACTLAND) AS average_actual_land_value
FROM property_valuation
GROUP BY YEAR, ZIP_CODE;

-- Total complaints per zip per year
CREATE TABLE total_complaints_by_zip_per_year
AS
SELECT YEAR(rpt_dt) AS YEAR,
       zipcode,
       COUNT(*) AS total_complaints
FROM complaints
GROUP BY YEAR(rpt_dt), zipcode;