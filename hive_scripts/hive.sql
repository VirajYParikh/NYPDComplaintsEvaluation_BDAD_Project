-- Businesses Table --
create external table businesses (
    avg_rating float, 
    category string, 
    gmap_id string, 
    name string, 
    num_of_reviews int, 
    zip int
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/reviews/nyc_businesses.csv'
tblproperties("skip.header.line.count"="1");
--------------------------

-- Reviews Table --
create external table reviews (
    gmap_id string,
    rating int,
    zip int,
    date_ date
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/reviews/nyc_reviews.csv'
tblproperties("skip.header.line.count"="1");
--------------------------

-- Reviews average Table --
create external table review_average_by_category (
    zip int,
    year int,
    category string,
    avg_rating float
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/reviews/review_average_by_category.csv'
tblproperties("skip.header.line.count"="1");
--------------------------

-- Property Valuation Table --
create external table property_valuation (
    BOROUGH string,
    BLOCK int,
    BORO int,
    LOT int,
    YEAR string,
    CURMKTLAND int,
    CURMKTTOT int,
    CURACTLAND int,
    CURACTTOT int,
    ZIP_CODE int,
    LAND_AREA int,
    NUM_BLDGS int,
    EXTRACRDT string,
    BLD_STORY int,
    UNITS double,
    GROSS_SQFT int
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/property_valuation'
tblproperties("skip.header.line.count"="1");
--------------------------

-- MTA Ridership Table --
create external table mta_ridership (
    transit_timestamp string,
    station_complex_id int,
    station_complex string,
    borough string,
    payment_method string,
    fare_class_category string,
    ridership int,
    transit_date date,
    transit_time string,
    zipcode int
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/MTARidership'
tblproperties("skip.header.line.count"="1");
--------------------------

-- Complaints Table --
create external table complaints (
    cmplnt_num string,
    cmplnt_fr_dt date,
    cmplnt_fr_tm string,
    cmplnt_to_dt date,
    cmplnt_to_tm string,
    rpt_dt date,
    ofns_desc string,
    law_cat_cd string,
    boro_nm string,
    susp_age_group string,
    susp_race string,
    susp_sex string,
    latitude float,
    longitude float,
    vic_age_group string,
    vic_race string,
    vic_sex string,
    zipcode int
)
row format delimited fields terminated by ','
location '/user/lsj3272_nyu_edu/shared/complaints_comma_escaped'
tblproperties("skip.header.line.count"="1");
--------------------------


-- Relevant Queries --
-- 1. Find Pearon's correlation between total complaints and another field of your choice
SELECT
    corr(a.avg_rating, b.total_complaints) AS correlation_avg_review_complaints
FROM
    total_review_by_zip a
JOIN
    total_complaints_by_zip b
ON
    a.zip = b.zipcode;

CREATE TABLE total_review_by_zip
AS
SELECT zip,
       AVG(avg_rating) AS avg_rating
FROM review_average_by_category
GROUP BY zip;