

--------------------------------------------------------------------------------
----------------------------------基础实验底表-----------------------------------
----每天任务更新时间5:00，未更新预警通知时间 10:00
insert overwrite table bigdata_ds_temp.app_lc_waitpage_tianye_base_0651

SELECT  distinct 
        a1.group_name           AS group_name
       ,a1.shunt_id             AS user_id
       ,a2.order_id             AS order_id
       ,a2.is_respond           AS is_respond
       ,a2.is_match             AS is_match
       ,a2.total_amt/100        AS total_amt_yuan
       ,a2.order_gmv/100        AS order_gmv_yuan
       ,a2.cancel_ab_cnt        AS cancel_ab_cnt
       ,a2.respond_ab_cnt       AS respond_ab_cnt
       ,a2.is_tips_order        AS is_tips_order
       ,a2.cancel_type          AS cance_type
       ,a2.order_status         AS order_status
       ,to_date(a2.create_time) AS dt
       ,a2.is_final_order       AS is_final_order
       ,a2.order_display_id     AS order_display_id
       ,a2.order_uuid           AS order_uuid
       ,a2.cancel_duration      AS cancel_duration
       ,(case when a3.is_a_of_ab_order=0 and a3.order_subset=2 then 2 else 1 end ) as is_a_of_ab_order
       ,a3.is_cancel         AS is_cancel
       ,a3.is_prepay         AS is_prepay
       ,cast(unix_timestamp(a3.cancel_time)-unix_timestamp(a3.pre_pay_time) as bigint) AS cancel_pay_duration
       ,a3.respond_duration   AS respond_duration
       ,a3.order_subset       AS order_subset
       ,a4.apply_status      AS apply_status
       ,a4.driver_id         AS driver_id
       ,a4.result_status     AS result_status
FROM hll_dwd.dwd_abtest_obj_shunt_d_in a1
LEFT JOIN hll_dwb.dwb_order_base_1d_tm a2
ON a1.shunt_id = a2.user_id 
AND a2.dt = '2022-09-14'     ------底表时间修改（增量）
AND to_date(a2.create_time) between '2022-09-07' and '2022-09-14'   ------底表时间修改（期间）
AND a2.is_bus_ib = 1 
AND a2.is_bus_lc = 1 
AND ((a2.app_revision >= 6685 AND a2.client_type = 1) or (a2.app_revision >= 6684 AND a2.client_type = 2))
LEFT JOIN hll_dwb.dwb_order_ab_base_1d_tm a3
ON a2.order_id = a3.order_id 
AND a3.dt= '2022-09-14'       -------底表时间修改（增量）
AND to_date(a3.create_time) between '2022-09-07' and '2022-09-14'   ------底表时间修改（期间）
AND a3.is_bus_lc =1
LEFT JOIN hll_dwd.dwd_driver_service_markup_record_1d_tm a4
ON a2.order_uuid = a4.order_uuid
AND a4.dt= '2022-09-14'    -------底表时间修改（增量）  -----底表时间修改（期间）
WHERE a1.dt between '2022-09-07' and '2022-09-14'  ------底表时间修改（区间）
AND a1.test_id IN (10651)
AND ((cast(concat(split(a1.app_version, '\\.')[0], split(a1.app_version, '\\.')[1], split(a1.app_version, '\\.')[2]) AS bigint) >= 6685 
AND a1.client_id = 1) 
or (cast(concat(split(a1.app_version, '\\.')[0], split(a1.app_version, '\\.')[1], split(a1.app_version, '\\.')[2])as bigint) >= 6684 
AND a1.client_id = 2))

--------------------------------------------------------------------------------------------
---------------------------------埋点底表----------------------------------------------------
----已测试
---用户埋点底表
insert overwrite table bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651

select to_date(datetime),user_id,event,attribute from  hll_dwd.dwd_user_sensors_event_1d_in
where dt between '2022-09-07' and '2022-09-14'
and event in ('waitACK_report_click','raise_fee_popup_expo','fee_popup_click','fee_popup_click','waitACK_show','button_click_event','wait_show_click',
'raise_fee_module_expo','raise_fee_driver_expo','raise_fee_module_click','raise_fee_driver_click','waitpage_popup_click','waitpage_popup_expo')

---------------------------------------------------------------------------------------------
-----司机埋点底表
-------已测试 表变量为datetime，driver_id,event,attribution（注意）
insert overwrite table bigdata_ds_temp.app_lc_waitpage_wty_driver_base_0616 ----0651

select datetime,driver_id,event,attribute from  hll_dwd.dwd_driver_sensors_event_1d_in
where dt between '2022-09-07' and '2022-09-14'
and event in ('order_details_element_expo','order_details_page_click','order_extra_price_popup_click')



------------------------------------------------------------------------------------------------
------------------------------------整体数据表+用户加价表----------------------------------------------
-- 已测试

SELECT * from (
(SELECT 
        group_name
        ,count(distinct case when order_id is not null then user_id end)      AS order_user_num      ---实验用户总数
        ,count(distinct order_id)                                                   AS order_num           ---实验订单总数
        ,count(distinct case when is_respond=1 then order_id end)             AS respond_order_num   ---响应订单总数
        ,count(distinct case when is_match=1 then order_id end)               AS match_order_num     ---匹配订单总数
        ,count(distinct case when is_respond=1 then order_id end) / count(distinct order_id)  AS respond_rate        ---响应率
        ,count(distinct case when is_match=1 then order_id end) / count(distinct order_id)   AS match_rate          ---配对率
        ,count(distinct case when cancel_type in (31,32) and order_status =3 and is_respond=0 then order_id end)/count(distinct order_id)  AS cancelled_rate --响应前取消，取消类型用户取消
        ,count(distinct case when is_tips_order=1 then user_id end )                             AS tips_user_num       ---用户加价用户数
        ,count(distinct case when is_tips_order=1 then order_id end )                            AS tips_order_num      ---用户加价执行单量
        ,count(distinct case when is_tips_order=1 then order_id end ) / count(distinct order_id) AS tips_rate --用户加价订单占比
FROM bigdata_ds_temp.app_lc_waitpage_tianye_base_0651 
group by group_name) a1
FULL JOIN
(SELECT group_name
        ,sum(case when is_match=1 then order_gmv_yuan else 0 end)  as match_gtv    ----匹配订单总金额
FROM(SELECT distinct order_id,is_match,order_gmv_yuan,group_name
FROM bigdata_ds_temp.app_lc_waitpage_tianye_base_0651) a
group by group_name) a2
ON a1.group_name = a2.group_name
FULL JOIN 
(SELECT group_name,sum(case when is_respond=1 then cancel_ab_cnt else 0 end) / sum(case when is_respond=1 then respond_ab_cnt else 0 end ) as cancel_rate  ------取消率
FROM (
    SELECT            distinct order_id
                     ,group_name
                     ,is_respond
                     ,cancel_ab_cnt
                     ,respond_ab_cnt
    FROM bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
) b
group by group_name
) a3
ON a1.group_name = a3.group_name
) 
----------------------------------------------------------------------------------------------------
--------------------------------司机加价订单表--------------------------------------------------------
-----已测试
SELECT * from (
(SELECT   
        group_name                                                                             AS group_name       --------对照组，实验组名称
       ,count(distinct order_id)                                                               AS chujia_num       --------满足司机出价条件的订单量
       ,count(distinct case when apply_status = 1 then order_id end)                           AS apply_num        --------司机出价执行单量
       ,count(distinct case when result_status =1 then order_id end)                           AS result_num       --------用户同意司机出价订单数
       ,count(distinct case when result_status =1 and is_match=1 then order_id end)            AS match_result_num --------用户同意司机出价配对成功订单数
       ,count(distinct case when is_respond=1 then order_id end) / count(distinct order_id)    AS driver_respond_rate  ----司机出价订单响应率？？？？
       ,count(distinct case when is_match=1 then order_id end) / count(distinct order_id)      AS driver_match_rate -------司机出价订单配对率？？？？
FROM bigdata_ds_temp.app_lc_waitpage_tianye_base_0651 
where (is_a_of_ab_order =2 and is_prepay =0 and (cancel_duration >= 60 or respond_duration >= 60)) 
or (is_a_of_ab_order =2 and is_prepay =1 and (cancel_pay_duration >= 60 or respond_duration >= 60)) 
or (is_a_of_ab_order = 1 and (cancel_duration >= 60 or respond_duration >= 60))  -------------满足司机出价的订单条件
GROUP BY group_name) a1
FULL JOIN 
(select
    group_name, sum(driver_num) as driver_sum                                    ---------每个订单司机数加总
 from
    (
    select order_uuid,group_name,count(distinct case when apply_status = 1 then driver_id end) as driver_num
    from
    (select group_name,apply_status,driver_id ,order_uuid
     from bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
     where (is_a_of_ab_order =2 and is_prepay =0 and (cancel_duration >= 60 or respond_duration >= 60)) 
     or (is_a_of_ab_order =2 and is_prepay =1 and (cancel_pay_duration >= 60 or respond_duration >= 60)) 
     or (is_a_of_ab_order = 1 and (cancel_duration >= 60 or respond_duration >= 60))   
     ) a
     group by order_uuid,group_name
    ) b
 group by group_name
) a2
ON a1.group_name = a2.group_name
)

-----------------------------------------------------------------------------------------------------
---------------------------------响应前取消表----------------------------------------------------------
----已测试
SELECT
        group_name,
        count(distinct order_id) as cancel_order_cnt,     ----------响应前取消订单量
        percentile_approx(cancel_duration,0.5) as middle_cancel_time,   ------------响应前取消等待时长中位数
        avg(cancel_duration) as avg_cancel_time           ----------响应前取消等待时长平均数
FROM (
    select distinct order_id,group_name,cancel_duration 
    from bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
    WHERE is_respond=0
        ---and is_cancel=1 ) a2
    AND order_status=3
    AND cancel_type in (31,32)
) a
GROUP BY group_name


-------------------------------------------------------------------------------------------------------
--------------------------------用户加价埋点表-------------------------------------------
----已测试
select
  *
from
  (
    select
      group_name,    -------------实验组对照组
      count(*) as cnt_1,   ------------去加价点击次数
      count(distinct user_id) as user_cnt_1   -------去加价点击人数
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          user_id,
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitACK_report_click'
          and attribute ['module_name'] in ('去加价', '继续加价')
        ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r1 
  full join (
    select
      group_name,
      count(*) as cnt_3,     ----------默认拒绝司机确认弹窗次数
      count(distinct user_id) as user_cnt_3     ------默认拒绝司机确认弹窗人数
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          user_id,
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'raise_fee_popup_expo'
          and attribute ['popup_name'] = '用户加价大于司机报价弹窗'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r3 on r1.group_name = r3.group_name 
  full join (
    select
      group_name,
      count(*) as cnt_4,  --------继续加价次数
      count(distinct user_id) as user_cnt_4 -------继续加价人数
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          user_id,
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitACK_report_click'
          and attribute ['module_name'] = '继续加价'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r4 on r1.group_name = r4.group_name 
  full join (
    select
      group_name,
      count(*) as cnt_5,   -----------去选低报价次数
      count(distinct user_id) as user_cnt_5  ------去选低报价人数
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          user_id,
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'fee_popup_click'
          and attribute ['module_name'] = '去选低报价'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r5 on r1.group_name = r5.group_name
  -----------------------------------
  --去加价点击次数老版
select
  group_name,
  count(*) as cnt_2,
  count(distinct user_id) as user_cnt_2
from
  (
    select distinct 
      group_name,
      order_uuid
    from
      bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
  ) a1
  inner join (
    select distinct
      user_id,
      attribute ['order_uuid'] as order_uuid -- 安卓
    from
      bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
    where
        event = 'waitACK_show'
        and attribute ['add_tips'] = '点击加小费入口'
        and attribute ['button_type'] = '加小费'
        and attribute ['button_source'] = '配对中页面'
        or event = 'button_click_event'
        and attribute ['add_tips'] = '点击加小费入口'
        and attribute ['button_type'] = '加小费'
        and attribute ['button_source'] = '配对中页面'
        or event = 'wait_show_click'
        and attribute ['button_type'] = '加小费入口'
    union
    select distinct
      user_id,
      attribute ['order_uuid'] as order_uuid -- ios
    from
      bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
    where
        event = 'waitACK_show'
        and attribute ['add_tips'] = '点击加小费'
        or event = 'button_click_event'
        and attribute ['button_type'] = '加小费'
        and attribute ['button_source'] = '配对中页面'
        and attribute ['order_status'] = '配对中'
  ) a2 
on a1.order_uuid = a2.order_uuid
group by
  group_name
-------------------------------------------------------------------------------------------
------------------------司机加价埋点表(司机侧）------------------------------------
-----已测试
select
  *
from
  (
    select
      group_name,
      count(distinct concat(driver_id, a2.order_display_id)) as expo_cnt_1,  -------出价按钮曝光次数
      count(distinct a2.order_display_id) as expo_order_cnt_1           -------出价按钮曝光订单数
    from
      (
        select
          distinct group_name,
          order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          driver_id,
          attribution ['order_display_id'] as order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_wty_driver_base_0616
        where
          event = 'order_details_element_expo'
          and attribution ['element_name'] = '我要出价'
      ) a2 on a1.order_display_id = a2.order_display_id
    group by
      group_name
  ) r1 
  full join --点击出价按钮
  (
    select
      group_name,
      count(distinct concat(driver_id, a2.order_display_id)) as expo_cnt_2, ------点击出价按钮次数
      count(distinct a2.order_display_id) as expo_order_cnt_2     ------------点击出价按钮订单数
    from
      (
        select distinct
          distinct group_name,
          order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          driver_id,
          attribution ['order_display_id'] as order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_wty_driver_base_0616
        where
          event = 'order_details_page_click'
          and attribution ['page_click'] = '我要出价'
      ) a2 
      on a1.order_display_id = a2.order_display_id
    group by
      group_name
  ) r2 on r1.group_name = r2.group_name 
  full join --申请司机加价
  (
    select
      group_name,
      count(distinct concat(driver_id, a2.order_display_id)) as expo_cnt_3,   -------申请司机加价次数
      count(distinct a2.order_display_id) as expo_order_cn_3    ----------申请司机加价订单数
    from  
      (
        select distinct
          group_name,
          order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          driver_id,
          attribution ['order_display_id'] as order_display_id
        from
          bigdata_ds_temp.app_lc_waitpage_wty_driver_base_0616
        where
          event = 'order_extra_price_popup_click'
          and attribution ['popup_name'] = '我要出价'
          and attribution ['button_name'] = '确认出价'
      ) a2 
      on a1.order_display_id = a2.order_display_id
    group by
      group_name
  ) r3 on r1.group_name = r3.group_name

----------------------------------------------------------------------------------------------------
----------------------------------------------司机加价埋点表（用户侧）-----------------------------------
----未测试
select
  *
from
  (
    select
      group_name,
      count(*) as expo_cnt_1,         -----------------加价司机曝光次数 一级曝光
      count(distinct a2.order_uuid) as expo_order_cnt_1  --------加价司机曝光订单数 一级曝光
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'raise_fee_module_expo'
          and attribute ['driver_amount'] > 0
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r1 
  full join 
  (
    select
      group_name,
      count(*) as expo_cnt_2,    ---------------加价司机曝光次数 二级曝光
      count(distinct a2.order_uuid) as expo_order_cnt_2  --------加价司机曝光订单数 二级曝光
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'raise_fee_driver_expo'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r2 on r1.group_name = r2.group_name 
  full join
  (
    select
      group_name,
      count(*) as agr_cnt_1,     ----------用户点击列表同意按钮次数 一级同意
      count(distinct a2.order_uuid) as agr_order_cnt_1  --------用户点击列表同意按钮订单数 一级同意
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'raise_fee_module_click'
          and attribute ['module_name'] = '同意加价'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r3 on r1.group_name = r3.group_name 
  full join 
  (
    select
      group_name,
      count(*) as agr_cnt_2,    --------------用户点击列表同意按钮次数 二级同意
      count(distinct a2.order_uuid) as agr_order_cnt_2   -------------用户点击列表同意按钮订单数 二级同意
    from     
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'raise_fee_driver_click'
      ) a2 
      on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r4 
  on r1.group_name = r4.group_name 
  full join 
  (
    select
      group_name,
      count(*) as 2nd_agr_cnt,     ----------------二次确认弹窗同意次数
      count(distinct a2.order_uuid) as 2nd_agr_order_cnt   --------------二次确认弹窗同意订单数
    from
      (
        select
          distinct group_name,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select 
          attribute ['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'fee_popup_click'
          and attribute ['module_name'] = '确定'
          and attribute ['popup_name'] in ('同意加价确认弹窗','加价二次确认弹窗')
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r5 
  on r1.group_name = r5.group_name

--------------------------------------------------------------------------------
--------------------------修改车型埋点表-----------------------------------------

select
  *
from
  (
    select
      group_name,
      count(*) as type_expo_cnt,   --------修改车型弹窗曝光次数
      count(distinct a1.user_id) as type_expo_order   --------修改车型弹窗曝光人数
    from
      (
        select distinct 
        group_name,
        order_uuid,
        user_id
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitpage_popup_expo'
          and attribute ['popup_name'] = '呼叫更多车型弹窗'
      ) a2 
      on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r1 
  full join 
  (
    select
      group_name,
      count(*) as type_cfm_cnt,  -------修改车型次数
      count(distinct a1.user_id) as type_cfm_order -----修改车型人数
    from
      (
        select distinct 
        group_name,
        user_id,
        order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select distinct
          attribute['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitpage_popup_click'
          and attribute ['popup_name'] = '呼叫更多车型弹窗'
          and attribute['module_name'] = '确认'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r2 on r1.group_name = r2.group_name 
  full join
  (
    select
      group_name,
      count(*) as time_expo_cnt, -------修改用车时间弹窗曝光次数
      count(distinct a1.user_id) as time_expo_order -----修改用车时间弹窗曝光人数
    from
      (
        select
          distinct group_name,
          user_id,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select
          attribute['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitpage_popup_expo'
          and attribute ['popup_name'] = '修改用车时间弹窗'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r3 on r1.group_name = r3.group_name 
  full join -- 修改备注弹窗曝光
  (
    select
      group_name,
      count(*) as cmt_expo_cnt, ---------修改备注弹窗曝光次数
      count(distinct a1.user_id) as cmt_expo_order  ---------修改备注弹窗曝光人数
    from
      (
        select
          distinct group_name,
          user_id,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select
          attribute['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitpage_popup_expo'
          and attribute ['popup_name'] = '修改订单备注弹窗'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by
      group_name
  ) r4 on r1.group_name = r4.group_name full
  join -- 修改备注
  (
    select
      group_name,
      count(*) as cmt_cfm_cnt, --------修改备注次数
      count(distinct a1.user_id) as cmt_cfm_order ------修改备注人数
    from
      (
        select
          distinct group_name,
          user_id,
          order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_tianye_base_0651
      ) a1
      inner join (
        select
          attribute['order_uuid'] as order_uuid
        from
          bigdata_ds_temp.app_lc_waitpage_wty_user_base_0651
        where
          event = 'waitpage_popup_click'
          and attribute ['popup_name'] = '修改订单备注弹窗'
          and attribute ['module_name'] = '确认'
      ) a2 on a1.order_uuid = a2.order_uuid
    group by group_name
  ) r5 on r1.group_name = r5.group_name

-------------------------------------------------------------------------------
---------------------------------显著性检测基本表-----------------------------------

select
  group_name,
  user_id,
  sum(if(is_match = 1, 1, 0)) as match_order,
  sum(if(is_match = 1, order_gmv_yuan, 0)) as match_gtv,
  sum(if(is_respond = 1, 1, 0)) as respond_order,
  sum(if(cancel_type in (31, 32) and order_status = 3 and is_respond = 0,1,0)) as cancelled_order,
  sum(case when is_respond = 1 then cancel_ab_cnt else 0 end ) as cancel_order
from
  (
    select
      distinct group_name,
      order_id
      user_id,
      is_match,
      is_respond,
      cancel_type,
      order_status,
      cancel_ab_cnt,
      respond_ab_cnt,
      order_gmv_yuan
    from
      bigdata_ds_temp.app_lc_waitpage_tianye_base_0651) a 
group by
  group_name,
  user_id


