--# hi卡模版
select * from yt_trade.t_hi_card_template where is_deleted = 0 order by id desc;
--# 品牌跟卡模版绑定
select * from yt_trade.t_hi_card_template_search where is_deleted = 0 order by id desc;
--# 用户hi卡
select * from yt_trade.t_user_card where is_deleted = 0 order by id desc;
--# hi卡余额
select * from yt_trade.t_card_fund where is_deleted = 0 order by id desc;
--# hi卡流水
select * from yt_trade.t_card_fund_serial where is_deleted = 0 order by id desc;
--# hi卡流水详情
select * from yt_trade.t_card_fund_serial_details where is_deleted = 0 order by id desc;
--# hi卡支出业务流水
select * from yt_trade.t_card_expenditure_serial where is_deleted = 0 order by id desc;
--# hi卡收入业务流水
select * from yt_trade.t_card_income_serial where is_deleted = 0 order by id desc;
