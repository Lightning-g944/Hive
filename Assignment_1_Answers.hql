-- Changing from Master DB to User DB
use riazalab;

--=============================================================================

-- Create a table in Hive to access this data. Schema is mentioned above.

-- Droping Table If its already exists
drop table if exists logs_test;

-- Creating the Table
CREATE external table logs_test(
ip STRING,
dash string,
userid int,
reqdatetime string,
reqtime string,
pagereq string,
pagereq2 string,
code string,
statuscode int,
datasize int
)
row format delimited fields terminated by " ";

-- Loading the data into table.
load data inpath 'weblogs/' into table logs_test;


-- Creating a new table from the old table in 
-- addition to cleaning and converting into required format.
SELECT * from logs_test limit 5;

--Creating the Final table Weblogs which holds the data along with
--appropriate format and data.

create table weblogs as
select ip,
    userid,
    concat(replace(reqdatetime,"[",""),replace(reqtime,"]","")) ReqDateTime ,
    replace(pagereq,'"',"") PageReq,
    replace(pagereq2,"/","") PageName, 
    replace(code,'"',"") url, 
    statuscode, 
    datasize 
from logs_test;

--=============================================================================
-- How many users are there?

select count(distinct userid) as No_Of_Users from weblogs;

--=============================================================================
-- every user has made how many requests.

select userid, count(Pagereq) as No_Of_Requests from weblogs
group by userid;
--=============================================================================
-- Display total number of successful request.
select count(statuscode) as No_Of_Sucessful_Req from weblogs
where statuscode like '2%';