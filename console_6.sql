select *
from userbehavior
limit 10;

alter table userbehavior
    change C1 user_id int,
    change C2 item_id int,
    change C3 category_id int,
    change c4 behavior_type varchar(5),
    change C5 time_stamp int;

select *
from userbehavior
limit 10;

#空值
select *
from userbehavior
where user_id is null
   or item_id is null
   or category_id is null
   or behavior_type is null
   or time_stamp is null;
#重复值
select user_id, item_id, time_stamp
from userbehavior
group by user_id, item_id, time_stamp
having count(*) > 1;
#去重
alter table userbehavior
    add id int first;
alter table userbehavior
    modify id int primary key auto_increment;
select *
from userbehavior
limit 10;

delete userbehavior
from userbehavior,
     (select user_id,
             item_id,
             time_stamp,
             max(id) as max_id
      from userbehavior
      group by user_id,
               item_id,
               time_stamp
      having count(*) > 1) as df1
where userbehavior.user_id = df1.user_id
  and userbehavior.item_id = df1.item_id
  and userbehavior.time_stamp = df1.time_stamp
  and userbehavior.id < df1.max_id;

#异常值处理
alter table userbehavior
    add datetimes timestamp(0);

update userbehavior
set datetimes=from_unixtime(time_stamp);

select *
from userbehavior
limit 10;

delete
from userbehavior
where datetimes > '2017-12-03 23:59:59'
   or datetimes < '2017-11-25 00:00:00'
   or datetimes is null;
#增加日期 小时 列
alter table userbehavior
    add dates char(10);
alter table userbehavior
    add hours char(2);

update userbehavior
set dates=substring(datetimes, 1, 10),
    hours=substring(datetimes, 12, 2);

select *
from userbehavior
limit 10;

###数据分析

#用户获取分析
create table df_pv_uv
(
    dates char(10),
    PV    int(9),
    UV    int(9),
    PVUV  decimal(10, 2)
);

insert into df_pv_uv
select dates,
       count(if(behavior_type = 'pv', 1, null))                                       as PV,
       count(distinct user_id)                                                        as UV,
       round((count(if(behavior_type = 'pv', 1, null)) / count(distinct user_id)), 2) as 'PVUV'
from userbehavior
group by dates;

select dates, count(*)
from userbehavior
where behavior_type = 'pv'
group by dates;
select count(distinct user_id)
from userbehavior
group by dates;

#用户留存
select user_id, dates, lag(dates, 1, 0) over (partition by user_id order by dates)
from userbehavior
group by dates, user_id;

create table df_retention_1
(
    dates       char(10),
    retention_1 float
);
#次日留存率
insert into df_retention_1
select a.dates as dates, count(b.dates) / count(a.dates) as retention_1
from (select user_id, dates
      from userbehavior
      group by dates, user_id) a
         left join (select user_id, dates
                    from userbehavior
                    group by dates, user_id) b
                   on a.user_id = b.user_id
                       and a.dates = date_sub(b.dates, interval 1 day)
group by a.dates;
#三日留存率

select a.dates, count(b.dates) / count(a.dates)
from (select user_id, dates from userbehavior group by user_id, dates) a
         left join (select user_id, dates from userbehavior group by user_id, dates) b
                   on a.user_id = b.user_id
                       and date_add(a.dates, interval 3 day) = b.dates
group by a.dates;

#用户行为
create table df_timeseries
(
    dates char(10),
    hours int(9),
    PV    int(9),
    CART  int(9),
    FAV   int(9),
    BUY   int(9)
);

select *
from userbehavior
limit 10;

insert into df_timeseries
select dates,
       hours,
       count(if(behavior_type = 'pv', 1, null))   PV,
       count(if(behavior_type = 'cart', 1, null)) CART,
       count(if(behavior_type = 'fav', 1, null))  FAV,
       count(if(behavior_type = 'buy', 1, null))  BUY
from userbehavior
group by dates, hours
order by dates, hours;

#用户转化率
create view user_behavior_total as
select user_id,
       item_id,
       count(if(behavior_type = 'pv', 1, null))   as PV,
       count(if(behavior_type = 'fav', 1, null))  as FAV,
       count(if(behavior_type = 'cart', 1, null)) as CART,
       count(if(behavior_type = 'buy', 1, null))  as BUY
from userbehavior
group by user_id,
         item_id;

create view user_behavior_total_standard as
select user_id,
       item_id,
       if(PV > 0, 1, 0)   as ifpv,
       if(FAV > 0, 1, 0)  as iffav,
       if(CART > 0, 1, 0) as ifcart,
       if(BUY > 0, 1, 0)  as ifbuy
from user_behavior_total
group by user_id,
         item_id;

create view user_path as
select user_id, item_id, concat(ifpv, iffav, ifcart, ifbuy) path
from user_behavior_total_standard;


create view user_path_num as
select path,
       case
           when path = 1101 then '浏览-收藏-/-购买'
           when path = 1011 then '浏览-/-加购-购买'
           when path = 1111 then '浏览-收藏-加购-购买'
           when path = 1001 then '浏览-/-/-购买'
           when path = 1010 then '浏览-/-加购-/'
           when path = 1100 then '浏览-收藏-/-/'
           when path = 1110 then '浏览-收藏-加购-/'
           else '浏览-/-/-/'
           end  as description,
       count(*) as path_num
from user_path
where path regexp '^1'
group by path;

create table df_buy_path
(
    buy_path     varchar(55),
    buy_path_num int(9)
);

insert into df_buy_path
select '浏览',
       sum(path_num) as buy_path_num
from user_path_num;

insert into df_buy_path
select '浏览后收藏加购',
       sum(if(path = 1101 or
              path = 1100 or
              path = 1010 or
              path = 1011 or
              path = 1110 or
              path = 1111, path_num, null)) as buy_path_num
from user_path_num;


insert into df_buy_path
select '浏览后收藏加购后购买',
       sum(if(path = 1101 or
              path = 1011 or
              path = 1111, path_num, null)) as buy_path_num
from user_path_num;

#用户定位分析
#R计算
create view c as
select user_id,
       max(dates) as 'last_buy_date'
from userbehavior
where behavior_type = 'buy'
group by user_id;
#F计算
create view d as
select user_id,
       count(user_id) as 'buy_times'
from userbehavior
where behavior_type = 'buy'
group by user_id;

create table df_rfm_model
(
    user_id   int(9),
    recency   char(10),
    frequency int(9)
);
insert into df_rfm_model
select user_id,
       last_buy_date,
       buy_times
from c
         join
     d using (user_id);

-- 量化R
alter table df_rfm_model
    add r_score int(9);

update df_rfm_model
set r_score =
        case
            when recency = '2017-12-03' then 100
            when recency = '2017-12-02' or recency = '2017-12-01' then 80
            when recency = '2017-11-30' or recency = '2017-11-29' then 60
            when recency = '2017-11-28' or recency = '2017-11-27' then 40
            else 20
            end;

-- 量化F
alter table df_rfm_model
    add f_score int(9);

update df_rfm_model
set f_score =
        case
            when frequency > 15 then 100
            when frequency between 12 and 14 then 90
            when frequency between 9 and 11 then 70
            when frequency between 6 and 8 then 50
            when frequency between 3 and 5 then 30
            else 10
            end;

create view f as
select e.user_id,
       recency,
       r_score,
       avg_r,
       frequency,
       f_score,
       avg_f
from (select user_id,
             avg(r_score) over () as avg_r,
             avg(f_score) over () as avg_f
      from df_rfm_model) as e
         join
     df_rfm_model using (user_id);

create table df_rfm_result
(
    user_class     varchar(5),
    user_class_num int(9)
);

insert into df_rfm_result;

select user_class,
       count(*) as user_class_num
from (select *,
             case
                 when (f_score >= avg_f and r_score >= avg_r) then '价值用户'
                 when (f_score >= avg_f and r_score < avg_r) then '保持用户'
                 when (f_score < avg_f and r_score >= avg_r) then '发展用户'
                 else '挽留用户'
                 end as user_class
      from f) as g
group by user_class;

-- 热门品类

create table df_popular_category
(
    category_id int(9),
    category_pv int(9)
);
insert into df_popular_category
select category_id,
       count(if(behavior_type = 'pv', 1, null)) as category_pv
from userbehavior
group by category_id
order by count(if(behavior_type = 'pv', 1, null)) desc
limit 10;


-- 热门商品
create table df_popular_item
(
    item_id int(9),
    item_pv int(9)
);
insert into df_popular_item;
select item_id,
       count(if(behavior_type = 'pv', 1, null)) item_pv
from userbehavior
group by item_id
order by item_pv desc
limit 10;

#商品特征
create table df_category_conv_rate
(
    category_id        int(9),
    PV                 int(9),
    FAV                int(9),
    CART               int(9),
    BUY                int(9),
    category_conv_rate float
);
insert into df_category_conv_rate
select category_id,
       count(if(behavior_type = 'pv', 1, null))                                           as PV,
       count(if(behavior_type = 'fav', 1, null))                                          as FAV,
       count(if(behavior_type = 'cart', 1, null))                                         as CART,
       count(if(behavior_type = 'buy', 1, null))                                          as BUY,
       count(distinct if(behavior_type = 'buy', user_id, null)) / count(distinct user_id) as category_conv_rate
from userbehavior
group by category_id
order by category_conv_rate desc;

#用户留存率分析
#1.每日用户次日留存率（包括老用户）
select a.dates,count(b.dates)/count(a.dates)
from
(select user_id,dates from userbehavior group by user_id,dates) a
left join
(select user_id,dates from userbehavior group by user_id,dates) b
on a.user_id=b.user_id
and date_add(a.dates,interval 1 day)=b.dates
group by a.dates;
#2.新用户次日留存率
select count(b.dates)/count(a.dates)
from (select user_id,date_add(min(dates),interval 1 day) dates from userbehavior group by user_id) a
left join (select user_id,dates from userbehavior group by user_id,dates) b
on a.user_id=b.user_id and a.dates=b.dates;

#3.每日新用户次日留存率
select a.dates,count(b.dates)/count(a.dates)
from
(select user_id,min(dates) dates from userbehavior group by user_id) a
left join
(select user_id,dates from userbehavior group by user_id,dates) b
on a.user_id=b.user_id and date_add(a.dates,interval 1 day)=b.dates
group by a.dates;
