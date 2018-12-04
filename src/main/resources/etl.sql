-- 商品文本数据
SELECT
pd.products_id AS product_id,
pd.products_name AS product_name,
ptc.cat_path AS cat_path
FROM newchic.products_description pd
JOIN newchic.products_to_categories ptc
ON pd.products_id = ptc.products_id

-- 商品相似表
create table if not exists ai.nc_item_similarity (
    product_id      string,
    sim_payload     string,
    category        string
);

-- 商品热度表
create table if not exists ai.nc_item_popularity (
    product_id      string,
    pop_order       float,
    pop_cart        float,
    pop_wish        float,
    pop_click       float,

    trend_order       float,
    trend_cart        float,
    trend_wish        float,
    trend_click       float,

    hot_order       float,
    hot_cart        float,
    hot_wish        float,
    hot_click       float
);

-- 商品uv统计
SELECT
event,
product_id,
sum(event_uv) AS event_uv
FROM ai.nc_customer_daily_events
WHERE event_time >= "2018-11-01" AND event_time <= "2018-11-10"
GROUP BY event, product_id

-- user
select distinct
sess_id,
customers_id as user_id
from recommendation.rec_session
where dctime = ""
and customers_id > 0

-- click
select distinct
sess_id,
products_id as product_id
from recommendation.rec_session_action
where dctime = ""
and site in (
  "m.newchic.com", "www.newchic.com", "android.newchic.com",
  "ios.newchic.com", "ar-ios.newchic.com", "ar-android.newchic.com",
  "ar-m.newchic.com", "m.newchic.in", "android.newchic.in",
  "ios.newchic.in", "www.newchic.in"
)
and com = 'view'
and products_id != 0
and r_position rlike '.*(buytogether-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'

-- wish
select distinct
sess_id,
products_id as product_id
from recommendation.rec_session_operate
where dctime = ""
and site in (
  "m.newchic.com", "www.newchic.com", "android.newchic.com",
  "ios.newchic.com", "ar-ios.newchic.com", "ar-android.newchic.com",
  "ar-m.newchic.com", "m.newchic.in", "android.newchic.in",
  "ios.newchic.in", "www.newchic.in"
)
and operating = 'wish'
and products_id != 0
and r_position rlike '.*(buytogether-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'

-- cart
select distinct
sess_id,
products_id as product_id
from recommendation.rec_session_basket
where dctime = ""
and site in (
  "m.newchic.com", "www.newchic.com", "android.newchic.com",
  "ios.newchic.com", "ar-ios.newchic.com", "ar-android.newchic.com",
  "ar-m.newchic.com", "m.newchic.in", "android.newchic.in",
  "ios.newchic.in", "www.newchic.in"
)
and operating in ('cart', 'buynow')
and products_id != 0
and r_position rlike '.*(buytogether-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'

-- order
select distinct
sess_id,
products_id as product_id
from recommendation.rec_order_products
where from_unixtime(add_time, 'yyyy-MM-dd') = ""
and site in (
  "m.newchic.com", "www.newchic.com", "android.newchic.com",
  "ios.newchic.com", "ar-ios.newchic.com", "ar-android.newchic.com",
  "ar-m.newchic.com", "m.newchic.in", "android.newchic.in",
  "ios.newchic.in", "www.newchic.in"
)
and orders_status not in (1,4,6,12,17,20,21,22,23,27)
and products_id != 0
and r_position rlike '.*(buytogether-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'

-- 综合表
create table if not exists ai.nc_customer_daily_events (
    sess_id       bigint,
    user_id       bigint,
    product_id    bigint,
    event         string,
    event_uv      int
) partitioned by (event_time string)
row format delimited fields terminated by ',';

-- 得分表
create table if not exists ai.nc_item_event_corr (
    product_id       string,
    corr_payload     string
) partitioned by (event string)
row format delimited fields terminated by ',';

-- reading training data
select
event,
user_id,
product_id,
count(event_uv) as event_uvs
from ai.nc_customer_daily_events
where event_time >= "" and event_time <= ""
group by user_id, product_id, event
order by event, user_id, product_id

-- reading outcome data
select
  pt.products_id as product_id,
  COALESCE(order_corr_payload, "") as order_corr_payload,
  COALESCE(cart_corr_payload, "") as cart_corr_payload,
  COALESCE(wish_corr_payload, "") as wish_corr_payload,
  COALESCE(click_corr_payload, "") as click_corr_payload,
  "2018-11-20" as update_time
from newchic.products pt
left join
  (select
   product_id,
   corr_payload as order_corr_payload
   from ai.nc_item_event_corr
   where event="order"
  ) as order_tb
on pt.products_id=order_tb.product_id
left join
  (select
   product_id,
   corr_payload as cart_corr_payload
   from ai.nc_item_event_corr
   where event="cart"
  ) as cart_tb
on pt.products_id = cart_tb.product_id
left join
  (select
   product_id,
   corr_payload as wish_corr_payload
   from ai.nc_item_event_corr
   where event="wish"
  ) as wish_tb
on pt.products_id = wish_tb.product_id
left join
  (select
   product_id,
   corr_payload as click_corr_payload
   from ai.nc_item_event_corr
   where event="click"
  ) as click_tb
on pt.products_id = click_tb.product_id

-- indexed table
CREATE TABLE `rec_item_event_corr_nc` (
`id`  bigint(20) NOT NULL AUTO_INCREMENT COMMENT '唯一索引' ,
`product_id`  bigint(15) UNSIGNED NOT NULL DEFAULT 0 COMMENT '商品id' ,
`cat_path`  varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT '' COMMENT '商品类目路径' ,
`price`  float(10,4) UNSIGNED ZEROFILL NULL DEFAULT 00000.0000 COMMENT '商品价格' ,
`pop_order`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '下单pop分' ,
`pop_cart`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '加购pop分' ,
`pop_wish`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '收藏pop分' ,
`pop_click`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '点击pop分' ,
`trend_order`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '下单trend分' ,
`trend_cart`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '加购trend分' ,
`trend_wish`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '收藏trend分' ,
`trend_click`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '点击trend分' ,
`hot_order`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '下单hot分' ,
`hot_cart`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '加购hot分' ,
`hot_wish`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '收藏hot分' ,
`hot_click`  float(7,2) NOT NULL DEFAULT 0.00 COMMENT '点击hot分' ,
`popular_score`  float(10,5) UNSIGNED ZEROFILL NULL DEFAULT 0000.00000 COMMENT 'popular分数' ,
`hot_score`  float(10,5) UNSIGNED ZEROFILL NULL DEFAULT 0000.00000 COMMENT 'hot分数' ,
`trend_score`  float(10,5) UNSIGNED ZEROFILL NULL DEFAULT 0000.00000 COMMENT 'trend分数' ,
`order_corr_payload`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '下单关联商品及分数' ,
`cart_corr_payload`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '加购关联商品及分数' ,
`wish_corr_payload`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '收藏关联商品及分数' ,
`click_corr_payload`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '点击关联商品及分数' ,
`text_corr_payload`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '文本关联商品及分数' ,
`update_time`  varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '插入时间' ,
PRIMARY KEY (`id`, `update_time`),
INDEX `pt_ix` (`product_id`) USING BTREE COMMENT '商品id索引',
INDEX `time_ix` (`update_time`) USING BTREE COMMENT '时间索引'
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci
COMMENT='商品协同分数'
AUTO_INCREMENT=127134
ROW_FORMAT=DYNAMIC
;



