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
    UNITS int,
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

