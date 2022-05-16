-- Creating Table Loan_stats
create table Loan_stats
(
    id int, 
    member_id int, 
    loan_amnt int, 
    funded_amnt int, 
    funded_amnt_inv int, 
    term string, 
    init_rate string, 
    installment float, 
    grade string, 
    subgrade string, 
    emp_title string, 
    emp_length string, 
    home_ownership string, 
    annual_inc bigint, 
    verification_status string, 
    issue_d string, 
    loan_status string, 
    pymt_plan string, 
    url string, 
    desc string,
    purpose string, 
    title string, 
    zip_code string, 
    addr_state string, 
    dti float, 
    delinq_2yrs int, 
    earliest_cr_line string, 
    inq_last_6mths int, 
    mths_since_last_delinq string, 
    mths_since_last_record string, 
    open_acc int, 
    pub_rec int, 
    revol_bal int, 
    revol_util string, 
    total_acc int, 
    initial_list_status string, 
    out_prncp float, 
    out_prncp_inv double, 
    total_pymnt float, 
    total_pymnt_inv float, 
    total_rec_int float, 
    total_rec_late_fee float, 
    recoveries float, 
    collection_recovery_fee float, 
    last_pymnt_d string, 
    last_pymnt_amnt float, 
    next_pymnt_d string, 
    last_credit_pull_d string, 
    collections_12_mths_ex_med float, 
    mths_since_last_major_derog string, 
    policy_code int, application_type string, 
    annual_inc_joint string, 
    dti_joint string, 
    verification_status_joint string, 
    acc_now_delinq int, 
    tot_coll_amt string, 
    tot_cur_bal string, 
    open_acc_6m string, 
    open_il_6m string, 
    open_il_12m string, 
    open_il_24m string, 
    mths_since_rcnt_il string, 
    total_bal_il string, 
    il_util string, 
    open_rv_12m string, 
    open_rv_24m string, 
    max_bal_bc string, 
    all_util string, 
    total_credit_rv string, 
    inq_fi string, 
    total_fi_tl string, 
    inq_last_12m string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
) tblproperties('serialization.null.format'='','skip.header.line.count'='2');

-- Creating Reject status
create external table reject_stats
(
    amount_requested string, 
    Application_date string, 
    loan_title string, 
    risk_score string, 
    Debt_To_Income string, 
    zipcode string, 
    state string, 
    employment_length string, 
    policy_code string
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES  
( 
"separatorChar" = ",", 
"quoteChar" = "\"" 
) STORED AS TEXTFILE  
tblproperties('serialization.null.format'='','skip.header.line.count'='2');


-- Q1. Total Loan issuance by yearly & Quarterly and calculate growth rate by quarter
-- on quarter and year on year.
create table quarter_table as 
select 
    id as id,
    case  
        when lower(month) in ('jan','feb','mar') then 1 
        when lower(month) in('apr','may','jun') then 2 
        when lower(month) in ('jul','aug','sep') then 3 
        when lower(month) in ('oct','nov','dec') then 4 
        else 0 end as year_quarter, 
    loan_amnt as total_amnt,
    loan_year as loan_year 
from loan_timeline;



select 
    sum(loan_amnt) total_loan_issued,
    substr(issue_d,5)  Year 
from Loan_stats
where isnotnull(year) and isnotnull(total_loan_issued)
group by substr(issue_d,5) ;

select 
    year_quarter, 
sum(total_amnt) quarter_wise_loan 
from quarter_table
where year_quarter > 0
group by year_quarter;


-- calculating growth rate by quarter on quarter and year on year

select 
    loan_year, 
    year_quarter, 
    round((diff/previous)*100) Percentage_Growth 
from
(
    select 
        loan_year ,
        year_quarter, 
        sum(total_amnt) - lag(sum(total_amnt)) over(order by loan_year,year_quarter) diff,
        lag(sum(total_amnt)) over(order by loan_year,year_quarter) previous 

    from quarter_table 
    group by loan_year, year_quarter 
) t2
where year_quarter <> 0 and not isnull((diff/previous)*100)
order by loan_year,year_quarter;


--Q2. Percentage of loans based on the reported loan purpose. (Note: Loan purpose describes
--  the reported intent of borrowers from the most recent completed quarter and may not 
--  reflect actual usage. Investors should rely on loan grades rather than loan purpose)

select 
    purpose , 
    100*(sum(loan_amnt)/sum(distinct total_amt)) Percentage_of_Loans
from (
        select 
            *,
            sum(loan_amnt) over() as total_amt 
            from loan_stats
    ) t1
group by purpose;



-- Q3. Loan Issuance by state – classify the states based on loan issuance 
--      by $50+ MM, $25-50 MM, $10-25 MM and $0-10 MM

Select addr_state, sum(loan_amnt) total_Amount
From Loan_stats 
Group by addr_state;


Select addr_state, case
When total_Amount <1000000 then 'LOW' 
When total_Amount <2500000 then 'MEDIUM'
When total_Amount <5000000 then 'HIGH'
When total_Amount >5000000 then 'VERY HIGH'
End as Category 
from state_class
WHERE isnotnull(addr_state)
Order by Category DESC;


-- Q4. Find the last quarter average interest rates by different term loans and overall

SELECT 
    avg(intrest_rate) as average_intrest_rate_byterm, 
    t_term as term 
FROM interest_term 
GROUP BY t_term; 

SELECT 
    avg(intrest_rate) as average_intrest_rate_byterm, 
    t_term as term 
FROM interest_term 
GROUP BY t_term;


SELECT 
    avg(intrest_rate) as average_intrest_rate_overall 
FROM interest_term;


-- Q5. Find the historical returns by loan grade (Historical performance by grade 
--      for all issued loans) and overall

select 
    grade, 
    sum(loan_amnt) as total_loan, 
    sum (total_pymnt) as total_pymnt
from loan_stats 
group by grade;


select 
    sum(total_pymnt - loan_amnt) as return 
from loan_stats;


-- Q6. Find the historical average interest rates by loan terms and loan grades (also for overall)

select 
    term, 
    grade, 
    avg(regexp_replace(int_rate,'[^0-9.]','')) avg_intrest_rate_byterm_grade 
from loan_stats
group by term, grade;


SELECT
     avg(regexp_replace(init_rate,'[^0-9.]','')) Avg_interest_rate
from loan_stats;


-- Q7. What is percentage of loans by different loan grades by each year 
--     and loan term level (also for overall

select 
    grade, 
    term, 
    substr(issue_d,5) as year , 
    100*(count(*)/sum(distinct ctr)) 
from (
    select 
        *, 
        count(*) over() as ctr
    from loan_stats
    ) t1
group by grade, term , substr(issue_d,5);

select  
    substr(issue_d,5) as year , 
    100*(count(*)/sum(distinct ctr)) 
from (
        select 
            *, 
                count(*) over() as ctr from loan_stats) t1
group by  substr(issue_d,5);


-- Q8. What is the loan performance details by different loan grades and overall


select  
    loan_status, 
    grade, 
    100*(count(*)/sum(distinct ctr)) 
from (
        select 
            *, 
            count(*) over() as ctr 
        from loan_stats
    ) t2
group by loan_status,grade;


-- Q9. Find Net Annualized returns by vintage by different loan grades and different loan terms (also for overall)

select 
    grade, 
    term, 
    sum((cast(regexp_replace(int_rate,'[^0-9.]','') as float)*loan_amnt)/100) annualized_return 
from loan_stats
group by grade ,term ;


--Q10 What is loan status migration over 9 months (Net Charge offs: 120+days delinquency)


create table loan_migrate_status as
select 
    loan_status as status, 
    count(loan_status) as cnt
from loan_stats
group by loan_status
having loan_status in ('In Grace Period', 'Late (16-30 days)', 'Late (31-120 days)', 'Default');

Create table charged_off as 
Select 
    count(loan_status) as loan_count 
from loan_stats
GROUP BY loan_status
Having loan_status='Charged Off';

Create table migrate_status as 
select 
    loan_migrate_status.status as status, 
    loan_migrate_status.cnt as count, 
    charged_off.loan_count as chargeed_count 
from loan_migrate_status 
cross join charged_off;

Select 
    status, 
    100*(count/chargeed_count) as Migrtn_Status 
from migrate_status;