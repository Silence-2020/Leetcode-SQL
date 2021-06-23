
# 1831
select
    transaction_id
from (
    select
        *,
        rank() over(partition by date_format(day, '%Y-%m-%d') order by amount desc) as 'ranking'
    from Transactions
) as Transactions
where ranking = 1
order by 1

# 1821
select
    customer_id
from Customers
where year = 2021
and revenue > 0

# 1811
with NewContests as (
    select
        contest_id,
        gold_medal as 'user_id'
    from Contests
    union all
    select
        contest_id,
        silver_medal as 'user_id'
    from Contests
    union all
    select
        contest_id,
        bronze_medal as 'user_id'
    from Contests
),
Base as (
    select
        Users.user_id,
        Users.mail,
        Users.name,
        NewContests.contest_id - row_number() over(partition by Users.user_id order by NewContests.contest_id) as 'continue_flag'
    from Users
    join NewContests
    on Users.user_id = NewContests.user_id
)
select
    name,
    mail
from Base
group by user_id, continue_flag
having count(*) >= 3

union

select
    Users.name,
    Users.mail
from Contests
join Users
on Contests.gold_medal = Users.user_id
group by Users.user_id
having count(*) >= 3

# 1809
select
    Playback.session_id
from Playback
left join Ads
on Playback.customer_id = Ads.customer_id
and (Ads.timestamp between Playback.start_time and Playback.end_time)
where Ads.ad_id is null

# 1795
select
    product_id,
    'store1' as 'store',
    store1 as 'price'
from Products
where store1 is not null

union

select
    product_id,
    'store2' as 'store',
    store2 as 'price'
from Products
where store2 is not null

union

select
    product_id,
    'store3' as 'store',
    store3 as 'price'
from Products
where store3 is not null

# 1789
select
    employee_id,
    department_id
from (
    select
        *,
        row_number() over(partition by employee_id order by primary_flag desc) as 'ranking'
    from Employee
) as Employee
where ranking = 1

# 1783
select
    Players.player_id,
    Players.player_name,
    count(*) as 'grand_slams_count'
from (
    select Wimbledon as 'player_id' from Championships
    union all
    select Fr_open from Championships
    union all
    select US_open from Championships
    union all
    select Au_open from Championships
) as Championships
join Players
on Championships.player_id = Players.player_id
group by Players.player_name

# 1777
select
    product_id,
    sum(if(store='store1', price, null)) as 'store1',
    sum(if(store='store2', price, null)) as 'store2',
    sum(if(store='store3', price, null)) as 'store3'
from Products
group by product_id

# 1767
with recursive MaxCounts as (
    select 1 as 'subtask_id'
    union all
    select subtask_id+1 from MaxCounts where subtask_id<(select max(subtasks_count) from Tasks)
)
select
    Tasks.task_id,
    MaxCounts.subtask_id
from Tasks
join MaxCounts
on Tasks.subtasks_count >= MaxCounts.subtask_id
left join Executed
on Tasks.task_id = Executed.task_id
and MaxCounts.subtask_id = Executed.subtask_id
where Executed.task_id is null

# 1757
select
    product_id
from Products
where low_fats = 'Y'
and recyclable = 'Y'

# 1747
select distinct
    LogInfo.account_id
from LogInfo
join LogInfo as BadInfo
on LogInfo.account_id = BadInfo.account_id
and LogInfo.ip_address != BadInfo.ip_address
and (LogInfo.login between BadInfo.login and BadInfo.logout or LogInfo.logout between BadInfo.login and BadInfo.logout)

# 1741
select
    event_day as 'day',
    emp_id,
    sum(out_time)-sum(in_time) as 'total_time'
from Employees
group by event_day, emp_id

# 1731
select
    Managers.employee_id,
    Managers.name,
    count(distinct Employees.employee_id) as 'reports_count',
    round(avg(Employees.age)) as 'average_age'
from Employees as Managers
join Employees
on Managers.employee_id = Employees.reports_to
group by Managers.employee_id

# 1729
select
    user_id,
    count(*) as 'followers_count'
from Followers
group by user_id
order by 1

# 1715
select
    sum(Boxes.apple_count) + ifnull(sum(Chests.apple_count), 0) as 'apple_count',
    sum(Boxes.orange_count) + ifnull(sum(Chests.orange_count), 0) as 'orange_count'
from Boxes
left join Chests
on Boxes.chest_id = Chests.chest_id

# 1709
select
    user_id,
    max(days) as 'biggest_window'
from (
    select
        user_id,
        datediff(lead(visit_date, 1, '2021-01-01') over(partition by user_id order by visit_date), visit_date) as 'days'
    from UserVisits
) as Base
group by user_id

# 1699
select
    from_id as 'person1',
    to_id as 'person2',
    count(*) as 'call_count',
    sum(duration) as 'total_duration'
from (
    select
        if(from_id<to_id, from_id, to_id) as 'from_id',
        if(from_id>to_id, from_id, to_id) as 'to_id',
        duration
    from Calls
) as Calls
group by from_id, to_id

# 1693
select
    date_id,
    make_name,
    count(distinct lead_id) as 'unique_leads',
    count(distinct partner_id) as 'unique_partners'
from DailySales
group by date_id, make_name

# 1683
select
    tweet_id
from Tweets
where length(content) > 15

# 1677
select
    Product.name,
    ifnull(sum(Invoice.rest), 0) as 'rest',
    ifnull(sum(Invoice.paid), 0) as 'paid',
    ifnull(sum(Invoice.canceled), 0) as 'canceled',
    ifnull(sum(Invoice.refunded), 0) as 'refunded'
from Product
left join Invoice
on Product.product_id = Invoice.product_id
group by Product.name
order by 1

# 1667
select
    user_id,
    concat(upper(left(name, 1)), lower(right(name, length(name)-1))) as 'name'
from Users
order by 1

# 1661
select
    machine_id,
    round((sum(if(activity_type='end', timestamp, 0))-sum(if(activity_type='start', timestamp, 0)))/count(distinct process_id), 3) as 'processing_time'
from Activity
group by machine_id

# 1651
with recursive monthlst as (
    select 1 as 'month'
    union all
    select month+1 from monthlst where month<12
)
select
    monthlst.month,
    round(avg(ifnull(ride_distance, 0)) over(order by monthlst.month rows between current row and 2 following), 2) as 'average_ride_distance',
    round(avg(ifnull(ride_duration, 0)) over(order by monthlst.month rows between current row and 2 following), 2) as 'average_ride_duration'
from monthlst
left join (
    select
        month(Rides.requested_at) as 'month',
        sum(AcceptedRides.ride_distance) as 'ride_distance',
        sum(AcceptedRides.ride_duration) as 'ride_duration'
    from Rides
    join AcceptedRides
    on Rides.ride_id = AcceptedRides.ride_id
    and year(Rides.requested_at) = 2020
    group by month(Rides.requested_at)
) as overall_rides
on monthlst.month = overall_rides.month
order by 1
limit 10

# 1645
with recursive monthlst as (
    select 1 as 'month'
    union all
    select month+1 from monthlst where month<12
),
monthly_drivers as (
    select
        monthlst.month,
        count(*) as 'drivers'
    from monthlst
    left join Drivers
    on 202000+monthlst.month >= date_format(Drivers.join_date, '%Y%m')
    group by monthlst.month
),
monthly_accepted as (
    select
        month(Rides.requested_at) as 'month',
        count(distinct AcceptedRides.driver_id) as 'accepted_drivers'
    from Rides
    join AcceptedRides
    on Rides.ride_id = AcceptedRides.ride_id
    and year(Rides.requested_at) = 2020
    group by month(Rides.requested_at)
)
select
    monthly_drivers.month,
    round(ifnull(monthly_accepted.accepted_drivers/monthly_drivers.drivers, 0)*100, 2) as 'working_percentage'
from monthly_drivers
left join monthly_accepted
on monthly_drivers.month = monthly_accepted.month

# 1635
with recursive Monthlst as (
    select 1 as 'month'
    union all
    select month+1 from Monthlst where month<12
),
MonthActive as (
    select
        Monthlst.month,
        count(distinct Drivers.driver_id) as 'active_drivers'
    from Monthlst
    left join Drivers
    on 202000+Monthlst.month >= Date_format(Drivers.join_date, '%Y%m')
    group by Monthlst.month
),
MonthRide as (
    select
        month(Rides.requested_at) as 'month',
        count(Rides.ride_id) as 'accepted_rides'
    from Rides
    join AcceptedRides
    on Rides.ride_id = AcceptedRides.ride_id
    and year(Rides.requested_at) = 2020
    group by month(Rides.requested_at)
)
select
    MonthActive.*,
    ifnull(MonthRide.accepted_rides, 0) as 'accepted_rides'
from MonthActive
left join MonthRide
on MonthActive.month = MonthRide.month

# 1633
select
    contest_id,
    round(count(distinct user_id)/(select count(*) from Users)*100, 2) as 'percentage'
from Register
group by contest_id
order by 2 desc, 1

# 1623
select
    SchoolA.student_name as 'member_A',
    SchoolB.student_name as 'member_B',
    SchoolC.student_name as 'member_C'
from SchoolA
join SchoolB
on SchoolA.student_id != SchoolB.student_id
and SchoolA.student_name != SchoolB.student_name
join SchoolC
on SchoolA.student_id != SchoolC.student_id
and SchoolA.student_name != SchoolC.student_name
and SchoolB.student_id != SchoolC.student_id
and SchoolB.student_name != SchoolC.student_name

# 1613
with recursive Num as (
    select 1 as 'ids'
    union
    select ids+1 from Num where ids<(select max(customer_id) from Customers)
)
select
    Num.ids
from Num
left join Customers
on Num.ids = Customers.customer_id
where Customers.customer_id is null

#1607
select
    Seller.seller_name
from Seller
left join Orders
on Seller.seller_id = Orders.seller_id
and year(Orders.sale_date) = 2020
where Orders.seller_id is null
order by 1

# 1596
select
    customer_id,
    product_id,
    product_name
from (
    select
        Orders.customer_id,
        Orders.product_id,
        Products.product_name,
        dense_rank() over(partition by Orders.customer_id order by count(*) desc) as 'ranking'
    from Orders
    join Products
    on Orders.product_id = Products.product_id
    group by Orders.customer_id, Orders.product_id
) as Base
where ranking = 1

# 1587
select
    Users.name,
    sum(Transactions.amount) as 'balance'
from Transactions
join Users
on Transactions.account = Users.account
group by Users.account
having 2 > 10000

# 1581
select
    Visits.customer_id,
    count(*) as 'count_no_trans'
from Visits
left join Transactions
on Visits.visit_id = Transactions.visit_id
where Transactions.visit_id is null
group by Visits.customer_id

# 1571
select
    Warehouse.name as 'WAREHOUSE_NAME',
    sum(Warehouse.units * Products.Width * Products.Length * Products.Height) as 'VOLUME'
from Warehouse
join Products
on Warehouse.product_id = Products.product_id
group by Warehouse.name

# 1565
select
    left(order_date, 7) as 'month',
    count(*) as 'order_count',
    count(distinct customer_id) as 'customer_count'
from Orders
where invoice > 20
group by left(order_date, 7)

# 1555
select
    Users.user_id,
    Users.user_name,
    Users.credit + ifnull(sum(if(Users.user_id=Transactions.paid_by, -Transactions.amount, Transactions.amount)), 0) as 'credit',
    if(Users.credit + ifnull(sum(if(Users.user_id=Transactions.paid_by, -Transactions.amount, Transactions.amount)), 0) < 0, 'Yes', 'No') as 'credit_limit_breached'
from Users
left join Transactions
on Users.user_id = Transactions.paid_by
or Users.user_id = Transactions.paid_to
group by Users.user_id

# 1549
select
    Products.product_name,
    Products.product_id,
    Orders.order_id,
    Orders.order_date
from (
    select
        *,
        dense_rank() over(partition by product_id order by order_date desc) as 'ranking'
    from Orders
) as Orders
join Products
on Orders.product_id = Products.product_id
and Orders.ranking = 1
order by 1, 2, 3

# 1543
select
    lower(replace(product_name, ' ', '')) as 'product_name',
    left(sale_date, 7) as 'sale_date',
    count(*) as 'total'
from Sales
group by 1, 2
order by 1, 2

# 1532
select
    customer_name,
    customer_id,
    order_id,
    order_date
from (
    select
        Customers.name as 'customer_name',
        Orders.customer_id,
        Orders.order_id,
        Orders.order_date,
        row_number() over(partition by Orders.customer_id order by Orders.order_date desc) as 'ranking'
    from Orders
    join Customers
    on Orders.customer_id = Customers.customer_id
) as Base
where ranking <= 3
order by 1, 2, 4 desc

# 1527
select
    *
from Patients
where conditions like 'DIAB1%'
or conditions like '% DIAB1%'

# 1517
select
    *
from Users
where mail regexp '^[a-zA-Z][a-zA-Z0-9\_\.\-]*@leetcode\.com$'

# 1511
select
    Customers.customer_id,
    Customers.name
from Orders
join Customers
on Orders.customer_id = Customers.customer_id
join Product
on Orders.product_id = Product.product_id
group by Customers.name
having sum(if(left(Orders.order_date, 7)='2020-06', Product.price*Orders.quantity, 0)) >= 100
and sum(if(left(Orders.order_date, 7)='2020-07', Product.price*Orders.quantity, 0)) >= 100

# 1501
select
    Country.name as 'country'
from Person
join Country
on left(Person.phone_number, 3) = Country.country_code
join (
    select caller_id as 'id', duration from Calls
    union all
    select callee_id as 'id', duration from Calls
) as Calls
on Person.id = Calls.id
group by Country.name
having avg(Calls.duration) > (select avg(duration) from Calls)

# 1495
select distinct
    Content.title
from Content
join TVProgram
on Content.content_id = TVProgram.content_id
and left(TVProgram.program_date, 7) = '2020-06'
and Content.Kids_content = 'Y'
and Content.content_type = 'Movies'

# 1485
select
    sell_date,
    count(distinct product) as 'num_sold',
    group_concat(distinct product) as 'products'
from Activities
group by sell_date

# 1479
select
    Items.item_category as 'Category',
    sum(if(dayofweek(Orders.order_date)=2, Orders.quantity, 0)) as 'Monday',
    sum(if(dayofweek(Orders.order_date)=3, Orders.quantity, 0)) as 'Tuesday',
    sum(if(dayofweek(Orders.order_date)=4, Orders.quantity, 0)) as 'Wednesday',
    sum(if(dayofweek(Orders.order_date)=5, Orders.quantity, 0)) as 'Thursday',
    sum(if(dayofweek(Orders.order_date)=6, Orders.quantity, 0)) as 'Friday',
    sum(if(dayofweek(Orders.order_date)=7, Orders.quantity, 0)) as 'Saturday',
    sum(if(dayofweek(Orders.order_date)=1, Orders.quantity, 0)) as 'Sunday'
from Items
left join Orders
on Items.item_id = Orders.item_id
group by Items.item_category
order by 1

# 1468
select
    Salaries.company_id,
    Salaries.employee_id,
    Salaries.employee_name,
    round(Salaries.salary*Tax.rate) as 'salary'
from Salaries
join (
    select
        company_id,
        case
        when max(salary)<1000 then 1
        when max(salary)<=10000 then 1-0.24
        else 1-0.49 end as rate
    from Salaries
    group by company_id
) as Tax
on Salaries.company_id = Tax.company_id

# 1459
select
    P1.id as 'p1',
    P2.id as 'p2',
    abs(P1.x_value-P2.x_value)*abs(P1.y_value-P2.y_value) as 'area'
from Points as P1
join Points as P2
on P1.id < P2.id
and P1.x_value != P2.x_value
and P1.y_value != P2.y_value
order by 3 desc, 1, 2

# 1454
select distinct
    Logins.id,
    Logins.name
from (
    select
        Accounts.id,
        Accounts.name,
        date_sub(login_date, interval row_number() over(partition by id order by login_date) day) as 'continue_flag'
    from Logins
    join Accounts
    on Logins.id = Accounts.id
    group by id, login_date
) as Logins
group by Logins.id, Logins.continue_flag
having count(*) >= 5

# 1445
select
    sale_date,
    max(if(fruit='apples', sold_num, 0))-max(if(fruit='oranges', sold_num, 0)) as 'diff'
from Sales
group by sale_date

# 1440
select
    Expressions.*,
    case Expressions.operator
    when '>' then if(left_var.value>right_var.value, 'true', 'false')
    when '<' then if(left_var.value<right_var.value, 'true', 'false')
    else if(left_var.value=right_var.value, 'true', 'false') end as 'value'
from Expressions
join Variables as left_var
on Expressions.left_operand = left_var.name
join Variables as right_var
on Expressions.right_operand = right_var.name

# 1435
select
    Base.bin,
    case Base.bin
    when '[0-5>' then sum(Sessions.duration/60<5)
    when '[5-10>' then sum(Sessions.duration/60>=5 and Sessions.duration/60<10)
    when '[10-15>' then sum(Sessions.duration/60>=10 and Sessions.duration/60<15)
    else sum(Sessions.duration/60>=15) end as 'total'
from (
    select '[0-5>' as 'bin'
    union
    select '[5-10>' as 'bin'
    union
    select '[10-15>' as 'bin'
    union
    select '15 or more' as 'bin'
) as Base
join Sessions
group by Base.bin

# 1421
select
    Queries.id,
    Queries.year,
    ifnull(NPV.npv, 0) as 'npv'
from Queries
left join NPV
on (Queries.id, Queries.year) = (NPV.id, NPV.year)

# 1412
select
    Student.student_id,
    Student.student_name
from (
    select distinct
        Student.student_id,
        Student.student_name
    from Student
    join Exam
    on Student.student_id = Exam.student_id
) as Student
left join (
    select
        *,
        dense_rank() over(partition by exam_id order by score) as 'ascending',
        dense_rank() over(partition by exam_id order by score desc) as 'descending'
    from Exam
) as Ranking
on Student.student_id = Ranking.student_id
and (Ranking.ascending=1 or Ranking.descending=1)
where Ranking.student_id is null
order by 1

# 1407
select
    Users.name,
    ifnull(sum(Rides.distance), 0) as 'travelled_distance'
from Users
left join Rides
on Users.id = Rides.user_id
group by Users.id
order by 2 desc, 1

# 1398
select
    Orders.customer_id,
    Customers.customer_name
from Orders
join Customers
on Orders.customer_id = Customers.customer_id
group by Orders.customer_id
having sum(Orders.product_name='A') >= 1
and sum(Orders.product_name='B') >= 1
and sum(Orders.product_name='C') = 0

# 1393
select
    stock_name,
    sum(if(operation='Buy', -price, price)) as 'capital_gain_loss'
from Stocks
group by stock_name

# 1384
with recursive Years as (
    select min(year(period_start)) as 'report_year' from Sales
    union
    select report_year+1 from Years where report_year < (
        select max(year(period_end)) from Sales
    )
),
Base as (
    select
        Sales.product_id,
        Product.product_name,
        Years.report_year,
        Sales.period_start,
        Sales.period_end,
        Sales.average_daily_sales
    from Sales
    join Product
    on Sales.product_id = Product.product_id
    join Years
    on Years.report_year between year(Sales.period_start) and year(Sales.period_end)
)
select
    Base.product_id,
    Base.product_name,
    cast(Base.report_year as char) as 'report_year',
    case
    when year(period_start)=year(period_end) then (datediff(period_end, period_start)+1)*average_daily_sales
    when year(period_start)!=year(period_end) and year(period_start)=report_year then (datediff(concat(report_year, '-12', '-31'), period_start)+1)*average_daily_sales
    when year(period_start)!=year(period_end) and year(period_end)=report_year then (datediff(period_end, concat(report_year, '-01', '-01'))+1)*average_daily_sales
    else (datediff(concat(report_year, '-12', '-31'), concat(report_year, '-01', '-01'))+1)*average_daily_sales end as 'total_amount'
from Base
order by 1, 3

# 1378
select
    EmployeeUNI.unique_id,
    Employees.name
from Employees
left join EmployeeUNI
on Employees.id = EmployeeUNI.id

# 1369
select
    username,
    max(if(counts>1 and ranking=2, activity, if(counts=1, activity, null))) as 'activity',
    max(if(counts>1 and ranking=2, startDate, if(counts=1, startDate, null))) as 'startDate',
    max(if(counts>1 and ranking=2, endDate, if(counts=1, endDate, null))) as 'endDate'
from (
    select
        *,
        row_number() over(partition by username order by startDate desc) as 'ranking',
        count(*) over(partition by username) as 'counts'
    from UserActivity
) as UserActivity
group by username

# 1364
select
    Invoices.invoice_id,
    Customers.customer_name,
    Invoices.price,
    count(distinct Contacts.contact_name) as 'contacts_cnt',
    count(distinct Temp.customer_name) as 'trusted_contacts_cnt'
from Invoices
join Customers
on Invoices.user_id = Customers.customer_id
left join Contacts
on Invoices.user_id = Contacts.user_id
left join Customers as Temp
on Contacts.contact_name = Temp.customer_name
group by Invoices.invoice_id

# 1355
select distinct
    activity
from (
    select
        activity,
        rank() over(order by count(distinct id) desc) as 'Descending',
        rank() over(order by count(distinct id)) as 'Ascending'
    from Friends
    group by activity
) as activity
where Descending != 1
and Ascending != 1

# 1350
select
    Students.id,
    Students.name
from Students
left join Departments
on Students.department_id = Departments.id
where Departments.id is null

# 1341
(
select
    Users.name as 'results'
from Movie_Rating
join Users
on Movie_Rating.user_id = Users.user_id
group by Movie_Rating.user_id
order by count(*) desc, Users.name
limit 1
)

union all

(
select
    Movies.title as 'results'
from Movie_Rating
join Movies
on Movie_Rating.movie_id = Movies.movie_id
and left(Movie_Rating.created_at, 7) = '2020-02'
group by Movie_Rating.movie_id
order by avg(Movie_Rating.rating) desc, Movies.title
limit 1
)

# 1336
with Base as(
    select
        Visits.user_id,
        Visits.visit_date,
        count(Transactions.user_id) as 'transactions_count'
    from Visits
    left join Transactions
    on (Visits.user_id, Visits.visit_date) = (Transactions.user_id, Transactions.transaction_date)
    group by Visits.user_id, Visits.visit_date
)
select
    Transactions.transactions_count,
    count(Base.user_id) as 'visits_count'
from (
    select
        row_number() over() as 'transactions_count'
    from Transactions

    union

    select 0 as 'transactions_count'
) as Transactions
left join Base
on Transactions.transactions_count = Base.transactions_count
where Transactions.transactions_count <= (
    select
        max(transactions_count)
    from Base
)
group by Transactions.transactions_count
order by 1

# 1327
select
    Products.product_name,
    sum(Orders.unit) as 'unit'
from Orders
join Products
on Orders.product_id = Products.product_id
and left(Orders.order_date, 7) = '2020-02'
group by Products.product_name
having sum(Orders.unit) >= 100

# 1322
select
    ad_id,
    ifnull(round(sum(action='Clicked')/sum(action!='Ignored')*100, 2), 0) as 'ctr'
from Ads
group by ad_id
order by 2 desc, 1

# 1321
select
    *
from (
    select
        visited_on,
        sum(amount) over(order by visited_on rows between 6 preceding and current row) as 'amount',
        round(avg(amount) over(order by visited_on rows between 6 preceding and current row), 2) as 'average_amount'
    from (
        select
            visited_on,
            sum(amount) as 'amount'
        from Customer
        group by visited_on
    ) as Customer
) as Base
where visited_on >= (
    select
        date_add(min(visited_on), interval +6 day)
    from Customer
)

# 1308
select
    gender,
    day,
    sum(score_points) over(partition by gender order by day) as 'total'
from Scores
order by 1, 2

# 1303
select
    employee_id,
    count(*) over(partition by team_id) as 'team_size'
from Employee

# 1294
select
    Countries.country_name,
    case
    when avg(Weather.weather_state) <= 15 then 'Cold'
    when avg(Weather.weather_state) >= 25 then 'Hot'
    else 'Warm' end as 'weather_type'
from Countries
join Weather
on Countries.country_id = Weather.country_id
and left(Weather.day, 7) = '2019-11'
group by Countries.country_id

# 1285
select
    min(log_id) as 'start_id',
    max(log_id) as 'end_id'
from (
    select
        *,
        log_id - row_number() over(order by log_id) as 'continue_flag'
    from Logs
) as Logs
group by continue_flag
order by 1

# 1280
select
    Students.student_id,
    Students.student_name,
    Subjects.subject_name,
    count(Examinations.subject_name) as 'attended_exams'
from Students
join Subjects
left join Examinations
on Students.student_id = Examinations.student_id
and Subjects.subject_name = Examinations.subject_name
group by Students.student_id, Subjects.subject_name
order by 1, 3

# 1270
select distinct
    Employees.employee_id
from Employees
join Employees as SecondLayer
on Employees.manager_id = SecondLayer.employee_id
join Employees as ThirdLayer
on SecondLayer.manager_id = ThirdLayer.employee_id
and ThirdLayer.manager_id = 1
where Employees.employee_id != 1

# 1264
select distinct
    Likes.page_id as 'recommended_page'
from (
    select
        user2_id as 'user_id'
    from Friendship
    where user1_id = 1

    union

    select
        user1_id as 'user_id'
    from Friendship
    where user2_id = 1
) as Friendship
join Likes
on Friendship.user_id = Likes.user_id
and Likes.page_id not in (
    select
        page_id
    from Likes
    where user_id = 1
)

# 1251
select
    UnitsSold.product_id,
    round(sum(Prices.price*UnitsSold.units)/sum(UnitsSold.units), 2) as 'average_price'
from UnitsSold
join Prices
on UnitsSold.product_id = Prices.product_id
and (UnitsSold.purchase_date between Prices.start_date and Prices.end_date)
group by UnitsSold.product_id

# 1241
select
    Submissions.sub_id as 'post_id',
    count(distinct Comments.sub_id) as 'number_of_comments'
from Submissions
left join Submissions as Comments
on Submissions.sub_id = Comments.parent_id
where Submissions.parent_id is null
group by Submissions.sub_id

# 1225
select
    *
from (
    select
        'succeeded' as 'period_state',
        min(success_date) as 'start_date',
        max(success_date) as 'end_date'
    from (
        select
            *,
            date_sub(success_date, interval row_number() over(order by success_date) day) as 'continue_flag'
        from Succeeded
        where success_date between '2019-01-01' and '2019-12-31'
    ) as Succeeded
    group by continue_flag

    union

    select
        'failed' as 'period_state',
        min(fail_date) as 'start_date',
        max(fail_date) as 'end_date'
    from (
        select
            *,
            date_sub(fail_date, interval row_number() over(order by fail_date) day) as 'continue_flag'
        from Failed
        where fail_date between '2019-01-01' and '2019-12-31'
    ) as Succeeded
    group by continue_flag
) as Base
order by start_date

# 1212
select
    Teams.team_id,
    Teams.team_name,
    ifnull(sum(Scores.num_points), 0) as 'num_points'
from Teams
left join (
    select
        host_team as 'team_id',
        case
        when host_goals > guest_goals then 3
        when host_goals = guest_goals then 1
        else 0 end as 'num_points'
    from Matches

    union all

    select
        guest_team as 'team_id',
        case
        when guest_goals > host_goals then 3
        when guest_goals = host_goals then 1
        else 0 end as 'num_points'
    from Matches
) as Scores
on Teams.team_id = Scores.team_id
group by Teams.team_id
order by 3 desc, 1


# 1211
select
    query_name,
    round(avg(rating/position), 2) as 'quality',
    round(sum(rating<3)/count(*)*100, 2) as 'poor_query_percentage'
from Queries
group by query_name

# 1205
select
    left(Chargebacks.trans_date, 7) as 'month',
    Transactions.country,
    sum(left(Chargebacks.trans_date, 7)=left(Transactions.trans_date, 7) and Transactions.state='approved') as 'approved_count',
    sum(if(left(Chargebacks.trans_date, 7)=left(Transactions.trans_date, 7) and Transactions.state='approved', Transactions.amount, 0)) as 'approved_amount',
    sum(Chargebacks.trans_id=Transactions.id) as 'chargeback_count',
    sum(if(Chargebacks.trans_id=Transactions.id, Transactions.amount, 0)) as 'chargeback_amount'
from Chargebacks
join Transactions
on Chargebacks.trans_id = Transactions.id
or (left(Chargebacks.trans_date, 7) = left(Transactions.trans_date, 7) and Transactions.state='approved')
group by left(Chargebacks.trans_date, 7), Transactions.country

select
    left(trans_date, 7) as 'trans_date',
    country,

from Transactions
group by left(trans_date, 7), country

select
    left(Chargebacks.trans_date, 7) as 'month',
    Transactions.country,
    sum(left(Chargebacks.trans_date, 7)=left(Transactions.trans_date, 7) and state='approved') as 'approved_count',
    sum(if(left(Chargebacks.trans_date, 7)=left(Transactions.trans_date, 7) and state='approved', amount, 0)) as 'approved_amount',
    count(distinct trans_id) as 'chargeback_count',
    sum(if(Chargebacks.trans_id=Transactions.id, Transactions.amount, 0)) as 'chargeback_amount'
from Chargebacks
join Transactions
group by left(Chargebacks.trans_date, 7), Transactions.country

from Transactions
join Chargebacks
on Transactions.id = Chargebacks.trans_id


# 1204
select
    person_name
from (
    select
        person_name,
        sum(weight) over(order by turn) as 'total_weight'
    from Queue
) as Queue
where total_weight <= 1000
order by total_weight desc
limit 1

# 1194
select
    group_id,
    player_id
from (
    select
        Players.player_id,
        Players.group_id,
        sum(score) as 'score'
    from (
        select
            first_player as 'player_id',
            sum(first_score) as 'score'
        from Matches
        group by first_player

        union all

        select
            second_player as 'player_id',
            sum(second_score) as 'score'
        from Matches
        group by second_player
    ) as Scores
    join Players
    on Scores.player_id = Players.player_id
    group by player_id
    order by score desc, player_id
) as Base
group by group_id
order by group_id

select
    group_id,
    player_id
from (
    select
        *,
        row_number() over(partition by group_id order by score desc, player_id) as 'ranking'
    from (
        select
            Players.player_id,
            Players.group_id,
            sum(if(Players.player_id=Matches.first_player, first_score, 0)) + sum(if(Players.player_id=Matches.second_player, second_score, 0)) as 'score'
        from Players
        join Matches
        on Players.player_id = Matches.first_player
        or Players.player_id = Matches.second_player
        group by Players.player_id
    ) as Base
) as Base
where ranking = 1

# 1193
select
    left(trans_date, 7) as 'month',
    country,
    count(*) as 'trans_count',
    sum(state='approved') as 'approved_count',
    sum(amount) as 'trans_total_amount',
    sum(if(state='approved', amount, 0)) as 'approved_total_amount'
from Transactions
group by left(trans_date, 7), country

# 1179
select
    id,
    max(if(month='Jan', revenue, null)) as 'Jan_Revenue',
    max(if(month='Feb', revenue, null)) as 'Feb_Revenue',
    max(if(month='Mar', revenue, null)) as 'Mar_Revenue',
    max(if(month='Apr', revenue, null)) as 'Apr_Revenue',
    max(if(month='May', revenue, null)) as 'May_Revenue',
    max(if(month='Jun', revenue, null)) as 'Jun_Revenue',
    max(if(month='Jul', revenue, null)) as 'Jul_Revenue',
    max(if(month='Aug', revenue, null)) as 'Aug_Revenue',
    max(if(month='Sep', revenue, null)) as 'Sep_Revenue',
    max(if(month='Oct', revenue, null)) as 'Oct_Revenue',
    max(if(month='Nov', revenue, null)) as 'Nov_Revenue',
    max(if(month='Dec', revenue, null)) as 'Dec_Revenue'
from Department
group by id

# 1174
select
    round(sum(order_date=customer_pref_delivery_date)/count(*)*100, 2) as 'immediate_percentage'
from (
    select
        *,
        row_number() over(partition by customer_id order by order_date) as 'ranking'
    from Delivery
) as Delivery
where ranking = 1

# 1173
select
    round(sum(order_date=customer_pref_delivery_date)/count(*)*100, 2) as 'immediate_percentage'
from Delivery

# 1164
select
    Base.product_id,
    ifnull(Products.new_price, 10) as 'price'
from (
    select distinct
        product_id
    from Products
) as Base
left join (
    select
        *,
        row_number() over(partition by product_id order by change_date desc) as 'ranking'
    from Products
    where change_date <= '2019-08-16'
) as Products
on Base.product_id = Products.product_id
and Products.ranking = 1

# 1159
select
    Users.user_id as 'seller_id',
    if(sum(Items.item_id=Orders.item_id)=1, 'yes', 'no') as '2nd_item_fav_brand'
from Users
join Items
on Users.favorite_brand = Items.item_brand
left join (
    select
        *,
        row_number() over(partition by seller_id order by order_date) as 'ranking'
    from Orders
) as Orders
on Users.user_id = Orders.seller_id
and Orders.ranking = 2
group by Users.user_id

# 1158
select
    Users.user_id as 'buyer_id',
    Users.join_date,
    count(Orders.order_id) as 'orders_in_2019'
from Users
left join Orders
on Users.user_id = Orders.buyer_id
and year(order_date) = '2019'
group by Users.user_id

# 1149
select distinct
    viewer_id as 'id'
from Views
group by view_date, viewer_id
having count(distinct article_id) >= 2
order by 1

# 1148
select distinct
    author_id as 'id'
from Views
where author_id = viewer_id
order by 1

# 1142
select
    ifnull(round(count(distinct session_id)/count(distinct user_id), 2), 0) as 'average_sessions_per_user'
from Activity
where datediff('2019-07-27', activity_date) < 30

# 1141
select
    activity_date as 'day',
    count(distinct user_id) as 'active_users'
from Activity
where (activity_date between date_add('2019-07-27', interval -29 day) and '2019-07-27')
group by activity_date

# 1132
select
    round(avg(remove_rate), 2) as 'average_daily_percent'
from (
    select
        round(count(distinct Removals.post_id)/count(distinct Actions.post_id)*100, 2) as 'remove_rate'
    from Actions
    left join Removals
    on Actions.post_id = Removals.post_id
    where Actions.extra = 'spam'
    group by Actions.action_date
) as Base

# 1127
select
    Spending.spend_date,
    Platforms.platform,
    sum(if(Spending.platform=Platforms.platform, amount, 0)) as 'total_amount',
    count(if(Spending.platform=Platforms.platform, 1, null)) as 'total_users'
from (
    select
        user_id,
        spend_date,
        if(count(distinct platform)=2, 'both', platform) as 'platform',
        sum(amount) as 'amount'
    from Spending
    group by user_id, spend_date
) as Spending
join (
    select 'desktop' as 'platform'
    union
    select 'mobile' as 'platform'
    union
    select 'both' as 'platform'
) as Platforms
group by Spending.spend_date, Platforms.platform

# Write your MySQL query statement below
select spend_date, 
    b.platform,
    sum(if (a.platform = b.platform, amount, 0)) as total_amount,
    count(if (a.platform = b.platform, 1, null)) as total_users
from (
    select spend_date, 
        user_id,
        if (count(distinct platform) = 2, 'both', platform) as platform,
        sum(amount) as amount
    from spending
    group by user_id, spend_date
) a,
(
    select 'desktop' as platform union
    select 'mobile' as platform union
    select 'both' as platform
) b
group by spend_date, platform;

# 1126
select
    business_id
from (
    select
        *,
        occurences > avg(occurences) over(partition by event_type) as 'greater_than'
    from Events
) as Events
group by business_id
having sum(greater_than) >= 2

# 1113
select
    extra as 'report_reason',
    count(distinct post_id) as 'report_count'
from Actions
where action_date = '2019-07-04'
and action = 'report'
group by extra

# 1112
select
    student_id,
    course_id,
    grade
from (
    select
        *,
        row_number() over(partition by student_id order by grade desc, course_id) as 'ranking'
    from Enrollments
) as Enrollments
where ranking = 1

# 1107
select
    first_login as 'login_date',
    count(*) as 'user_count'
from (
    select
        user_id,
        min(activity_date) as 'first_login'
    from Traffic
    where activity = 'login'
    group by user_id
) as Traffic
where first_login >= date_add('2019-06-30', interval -90 day)
group by first_login
order by 1

# 1098
select
    Books.book_id,
    Books.name
from Books
left join Orders
on Books.book_id = Orders.book_id
and Orders.dispatch_date > date_add('2019-06-23', interval -1 year)
where Books.available_from < date_add('2019-06-23', interval -1 month)
group by Books.book_id
having sum(ifnull(Orders.quantity, 0)) < 10

# 1097
select
    Activity.event_date as 'install_dt',
    count(Activity.player_id) as 'installs',
    round(count(SecondDay.player_id)/count(Activity.player_id), 2) as 'Day1_retention'
from Activity
left join Activity as SecondDay
on Activity.player_id = SecondDay.player_id
and datediff(SecondDay.event_date, Activity.event_date) = 1
where (Activity.player_id, Activity.event_date) in (
    select
        player_id,
        min(event_date)
    from Activity
    group by player_id
)
group by Activity.event_date

# 1084
select
    Sales.product_id,
    Product.product_name
from Sales
join Product
on Sales.product_id = Product.product_id
group by Sales.product_id
having sum(sale_date between '2019-01-02' and '2019-03-31') > 0
and sum(sale_date < '2019-01-01' or sale_date > '2019-03-31') = 0

# 1083
with Base as(
    select
        Sales.product_id,
        Sales.buyer_id,
        Product.product_name
    from Sales
    join Product
    on Sales.product_id = Product.product_id
)
select distinct
    buyer_id
from Base
where product_name = 'S8'
and buyer_id not in (
    select
        buyer_id
    from Base
    where product_name = 'iPhone'
)

# 1082
select
    seller_id
from (
    select
        seller_id,
        rank() over(order by sum(price) desc) as 'ranking'
    from Sales
    group by seller_id
) as Sales
where ranking = 1

# 1077
select
    project_id,
    employee_id
from (
    select
        Project.*,
        rank() over(partition by Project.project_id order by Employee.experience_years desc) as 'ranking'
    from Project
    join Employee
    on Project.employee_id = Employee.employee_id
) as Project
where ranking = 1

# 1076
select distinct
    project_id
from (
    select
        project_id,
        rank() over(order by count(*) desc) as 'ranking'
    from Project
    group by project_id
) as Project
where ranking = 1

# 1075
select
    Project.project_id,
    round(avg(Employee.experience_years), 2) as 'average_years'
from Project
join Employee
on Project.employee_id = Employee.employee_id
group by Project.project_id

# 1070
select
    product_id,
    year as 'first_year',
    quantity,
    price
from Sales
where (product_id, year) in (
    select
        product_id,
        min(year)
    from Sales
    group by product_id
)

# 1069
select
    product_id,
    sum(quantity) as 'total_quantity'
from Sales
group by product_id

# 1068
select
    Product.product_name,
    Sales.year,
    Sales.price
from Sales
join Product
on Sales.product_id = Product.product_id

# 1050
select
    actor_id,
    director_id
from ActorDirector
group by actor_id, director_id
having count(*) >= 3

# 1045
select
    customer_id
from Customer
group by customer_id
having count(distinct product_key) = (
    select
        count(distinct product_key)
    from Product
)

# 627
update salary set sex = case sex when 'm' then 'f' else 'm' end

# 626
select
    row_number() over(order by (id-1)^1) as 'id',
    student
from seat

# 620
select
    *
from cinema
where description != 'boring'
and id%2 = 1
order by rating desc

# 619
select (
    select
        num
    from my_numbers
    group by num
    having count(*) = 1
    order by 1 desc
    limit 1
) as 'num'

# 618
select
    max(if(continent='America', name, null)) as 'America',
    max(if(continent='Asia', name, null)) as 'Asia',
    max(if(continent='Europe', name, null)) as 'Europe'
from (
    select
        *,
        row_number() over(partition by continent order by name) as 'ranking'
    from student
) as student
group by ranking

# 615
select
    department.pay_month,
    department.department_id,
    case
    when department.avg_department_salary > company.avg_company_salary then 'higher'
    when department.avg_department_salary < company.avg_company_salary then 'lower'
    else 'same' end as 'comparison'
from (
    select
        left(salary.pay_date, 7) as 'pay_month',
        employee.department_id,
        avg(salary.amount) as 'avg_department_salary'
    from salary
    join employee
    on salary.employee_id = employee.employee_id
    group by left(salary.pay_date, 7), employee.department_id
) as department
join (
    select
        left(salary.pay_date, 7) as 'pay_month',
        avg(amount) as 'avg_company_salary'
    from salary
    group by left(salary.pay_date, 7)
) as company
on department.pay_month = company.pay_month

# 614
select
    followee as 'follower',
    count(distinct follower) as 'num'
from follow
where followee in (
    select
        follower
    from follow
)
group by followee

# 613
select
    min(abs(first_point.x - second_point.x)) as 'shortest'
from point as first_point
join point as second_point
on first_point.x != second_point.x

# 612
select
    round(sqrt(pow(first_point.x - second_point.x, 2) + pow(first_point.y - second_point.y, 2)), 2) as 'shortest'
from point_2d as first_point
join point_2d as second_point
on (first_point.x, first_point.y) != (second_point.x, second_point.y)
order by 1
limit 1

# 610
select
    *,
    if(x+y>z and abs(x-y)<z, 'Yes', 'No') as 'triangle'
from triangle

# 608
select
    id,
    case
    when p_id is null then 'Root'
    when id in (select p_id from tree) then 'Inner'
    else 'Leaf' end as 'Type'
from tree

# 607
select
    name
from salesperson
where sales_id not in (
    select
        sales_id
    from orders
    join company
    on orders.com_id = company.com_id
    and company.name = 'RED'
)

select
from orders
join salesperson
on orders.sales_id = salesperson.sales_id
and 

# 603
select
    seat_id
from (
    select
        *,
        count(*) over(partition by repeat_flag) as 'continue_num'
    from (
        select
            seat_id,
            seat_id - row_number() over(order by seat_id) as 'repeat_flag'
        from cinema
        where free = 1
    ) as cinema
) as cinema
where continue_num > 1

# 602
select
    id,
    count(*) as 'num'
from (
    select
        requester_id as 'id'
    from request_accepted

    union all

    select
        accepter_id as 'id'
    from request_accepted
) as base
group by id
order by 2 desc
limit 1

# 601
select
    id,
    visit_date,
    people
from (
    select
        *,
        count(*) over(partition by repeat_flag) as 'continue_num'
    from (
        select
            *,
            id - row_number() over(order by id) as 'repeat_flag'
        from Stadium
        where people >= 100
    ) as Stadium
) as Stadium
where continue_num >= 3

# 597
select
    ifnull(
    round(
        (
            select
                count(distinct requester_id, accepter_id)
            from RequestAccepted
        )
        /
        (
            select
                count(distinct sender_id, send_to_id)
            from FriendRequest
        ), 2), 0) as 'accept_rate'

# 596
select
    class
from courses
group by class
having count(distinct student) >= 5

# 595
select
    name,
    population,
    area
from World
where area > 3000000
or population > 25000000

# 586
select
    customer_number
from orders
group by customer_number
order by count(*) desc
limit 1

# 585
select
    sum(TIV_2016) as 'TIV_2016'
from (
    select
        *,
        count(*) over(partition by TIV_2015) as 'unique_2015',
        count(*) over(partition by LAT, LON) as 'unique_location'
    from insurance
) as insurance
where unique_2015 > 1
and unique_location = 1

# 584
select
    name
from customer
where referee_id != 2
or referee_id is null

# 580
select
    department.dept_name,
    count(student.student_id) as 'student_number'
from department
left join student
on department.dept_id = student.dept_id
group by department.dept_name
order by 2 desc, 1

# 579
select
    Id,
    Month,
    sum(Salary) over(partition by Id order by Month rows between 2 preceding and current row) as 'Salary'
from Employee
where (Id, Month) not in (
    select
        Id,
        max(Month)
    from Employee
    group by Id
)
order by 1, 2 desc

# 578
select
    question_id as 'survey_log'
from survey_log
where action != 'skip'
group by question_id
order by count(answer_id)/sum(answer_id is null) desc
limit 1

# 577
select
    Employee.name,
    Bonus.bonus
from Employee
left join Bonus
on Employee.empId = Bonus.empId
where Bonus.bonus is null
or Bonus.bonus < 1000

# 574
select
    Candidate.Name
from Vote
join Candidate
on Vote.CandidateId = Candidate.id
group by Candidate.Name
order by count(*) desc
limit 1

# 571
select
    avg(Number) as 'median'
from (
    select
        *,
        sum(Frequency) over(order by Number) as 'Ascending',
        sum(Frequency) over(order by Number desc) as 'Descending',
        sum(Frequency) over()/2 as 'Median'
    from Numbers
) as Numbers
where Ascending >= Median
and Descending >= Median

# 570
select distinct
    Manager.Name
from Employee
join Employee as Manager
on Employee.ManagerId = Manager.Id
group by Manager.Name
having count(distinct Employee.Id) >= 5

# 569
select
    Id,
    Company,
    Salary
from (
    select
        *,
        cast(row_number() over(partition by Company order by Salary, Id) as signed) as 'Ascending',
        cast(row_number() over(partition by Company order by Salary desc, Id desc) as signed) as 'Descending'
    from Employee
) as Employee
where abs(Ascending - Descending) <= 1
order by 1

# 550
select
    round(count(SecondDay.player_id)/count(FirstDay.player_id),2) as 'fraction'
from (
    select
        player_id,
        min(event_date) as 'first_login'
    from Activity
    group by player_id
) as FirstDay
left join Activity as SecondDay
on FirstDay.player_id = SecondDay.player_id
and datediff(SecondDay.event_date, FirstDay.first_login) = 1


# 534
select
    player_id,
    event_date,
    sum(games_played) over(partition by player_id order by event_date) as 'games_played_so_far'
from Activity

# 512
select
    player_id,
    device_id
from Activity
where (player_id, event_date) in (
    select
        player_id,
        min(event_date)
    from Activity
    group by player_id
)

# 511
select
    player_id,
    min(event_date) as 'first_login'
from Activity
group by player_id

# 262
select
    Trips.Request_at as 'Day',
    round(count(if(Trips.Status!='completed',1,null))/count(*),2) as 'Cancellation Rate'
from Trips
join Users as Clients
on Trips.Client_Id = Clients.Users_Id
and Clients.Banned = 'No'
and (Trips.Request_at between '2013-10-01' and '2013-10-03')
join Users as Drivers
on Trips.Driver_Id = Drivers.Users_Id
and Drivers.Banned = 'No'
group by Trips.Request_at

# 197
select
    Weather.id
from Weather
join Weather as Yesterday
on datediff(Weather.recordDate, Yesterday.recordDate) = 1
and Weather.Temperature > Yesterday.Temperature

# 196
delete from Person where Id not in (
    select
        Id
    from (
        select
            min(Id) as 'Id'
        from Person
        group by Email
    ) as tmp
)

# 185
select
    Department,
    Employee,
    Salary
from (
    select
        Department.Name as 'Department',
        Employee.Name as 'Employee',
        Employee.Salary,
        dense_rank() over(partition by Employee.DepartmentId order by Employee.Salary desc) as 'Ranking'
    from Employee
    join Department
    on Employee.DepartmentId = Department.Id
) as Base
where Ranking <= 3


# 184
select
    Department,
    Employee,
    Salary
from (
    select
        Department.Name as 'Department',
        Employee.Name as 'Employee',
        Employee.Salary,
        rank() over(partition by Employee.DepartmentId order by Employee.Salary desc) as 'Ranking'
    from Employee
    join Department
    on Employee.DepartmentId = Department.Id
) as Base
where Ranking = 1

# 183
select
    Customers.Name as 'Customers'
from Customers
left join Orders
on Customers.Id = Orders.CustomerId
where Orders.Id is null

# 182
select
    Email
from Person
group by Email
having count(*) > 1

# 181
select
    Employee.Name as 'Employee'
from Employee
join Employee as Manager
on Employee.ManagerId = Manager.Id
and Employee.Salary > Manager.Salary

# 180
select distinct
    Num as 'ConsecutiveNums'
from (
    select
        *,
        cast(Id as signed) - cast(row_number() over(partition by Num order by Id) as signed) as 'Diff'
    from Logs
) as Logs
group by Num, Diff
having count(*) >= 3

# 178
select
    Score,
    dense_rank() over(order by Score desc) as 'Rank'
from Scores

# 177
select
    Salary
from (
    select distinct
        Salary,
        dense_rank() over(order by Salary desc) as 'Ranking'
    from Employee
) as Employee
where Ranking = n

# 176
select (
    select distinct
        Salary
    from Employee
    order by Salary desc
    limit 1, 1
) as 'SecondHighestSalary'


# 175
select
    Person.FirstName,
    Person.LastName,
    Address.City,
    Address.State
from Person
left join Address
on Person.PersonId = Address.PersonId