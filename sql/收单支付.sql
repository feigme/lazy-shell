--# 支付渠道
select * from yt_pay.t_pay_channel order by id desc;
--# 支付渠道配置的分组查询商户号
select * from yt_pay.t_pay_merchants where merchants_no_group = 'Group_5939_hz-ytwl_weixin' order by id desc;
--# 用户
select * from yt_ustone.t_user where user_id = 'e27e44f96b6d4a769fd5e39832eb29c4'
--1# 收单
select * from yt_pay.t_acquire_order where is_deleted = 0
and out_biz_no = '31512408294395112849' 
-- and acquire_no = 'AC2024080100154236000009594'
order by id desc limit 10;
--2# 收银台token
select * from yt_pay.t_cashier_token order by id desc;
--3# 保存支付单
select * from yt_pay.t_acquire_pay_relate where acquire_no in ('AC2024082911020259300000311') order by id desc;
select * from yt_pay.t_pay where pay_num in ('Hi2024082911140828400007329') order by id desc;
select * from yt_pay.t_pay_sub where pay_num in ('Hi2024083015014699400008772') order by id desc;
select * from yt_pay.t_pay_history where pay_num = 'Hi2024082911140828400007329' order by id desc;
--4# 网关支付日志, biz_no = pay_num
select * from yt_pay.t_pay_gw_log where biz_no = 'Hi2024082911140828400007329' order by id desc;
select * from yt_pay.t_pay_ext order by id desc limit 10;

