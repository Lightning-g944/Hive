-- Creating Table 
-- Creating HVAC and Building Table
create table hvac
( H_date string,
time string,
target_temp int,
actual_temp int,
system int,
system_age int,
building_id string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
) 
tblproperties('serialization.null.format'='','skip.header.line.count'='1');

create table building
(building_id string,
building_mgr string,
building_age int,
hvac_product string,
country string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
) 
tblproperties('serialization.null.format'='','skip.header.line.count'='1');


-- Creating HVAC temperature Variables and merging every table

CREATE TABLE hvac_temperature as
select 
    *, 
    actual_temp - target_temp as temp_diff,
    case 
        when (actual_temp - target_temp) > 5 THEN 'HOT'
        when (actual_temp - target_temp) < -5 THEN 'COLD'
        else 'NORMAL'end
        as temp_range,
    case 
        when abs(actual_temp - target_temp) <5 THEN 0
        else 1 end
        AS extreme_temp
from hvac;

create table hvac_building as
select 
    ht.*, 
    b.building_mgr, 
    b.building_age, 
    b.hvac_product,
    b.country
from hvac_temperature ht inner join building b on ht.building_id = b.building_id;


-- Analysis

-- Q1. Data Visualization/analysis by mapping the buildings that are most frequently outside of the optimal
-- temperature range. Calculate count of extremetemp (i.e where the temperature was more than five degrees
-- or lower than the target temperature) by each country and temp range.

select
    building_id,
    count(temp_range) as optimal_count
from hvac_building
where temp_range !='NORMAL'
group by building_id
order by optimal_count desc;

select
    country,
    temp_range,
    sum(extreme_temp) as extreme_temperature
from hvac_building
group by country, temp_range
order by extreme_temperature;


-- Q2. Which country offices run hot (Hot offices can lead to employee complaints and reduced productivity) 
-- and which offices run cold (Cold offices cause elevated energy expenditures and employee discomfort). 
--  Calculate count of offices run in hot and count of office run in cold by country.

select
    country,
    temp_range,
    count(temp_range) Office_Count
from hvac_building
where temp_range != 'NORMAL'
group by country, temp_range;


-- Q3. Our data set includes information about the performance of five brands of HVAC equipment, distributed 
-- across many types of buildings in a wide variety of climates. We can use this data to assess the relative 
-- reliability of the different HVAC models(i.e We can see that the which model seems to regulate 
-- temperature most reliably and maintain the appropriate temperature range). Calculate count of 
-- extreamtemp by hvacproduct

select
    hvac_product,
    sum(cast(extreme_temp as int)) as extreme_temperature
from hvac_building
group by hvac_product;