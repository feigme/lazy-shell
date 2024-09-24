#!/bin/sh

table=(pt_order_logistics_n pt_order_shop_n pt_order_status_n pt_order_supply_n pt_trade_shop_n pt_trade_supply_n t_card_expenditure_serial t_card_fund t_card_fund_serial t_card_fund_serial_details t_card_income_serial t_hi_account_balance_detail t_hi_account_balance_detail_unique t_idempotent t_limit_buy_record_month t_limit_buy_record_week t_order_pay_fail t_order_rate t_order_sub_refund t_payment_details t_pay_channel t_shop_payment_config t_user_card)

for t in ${table[@]};
do
  echo "--------------------"
  echo "table: $t"
  find . -path "*/mapper/*.xml" -exec grep -l "$t" {} \; | grep -v "\./master/.*" | sed 's/\.\///g' | sed 's/\/.*//g' | uniq  
done
