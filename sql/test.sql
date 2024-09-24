WITH trd_list AS
    (
        SELECT data_id
             , wms_name
             , content
             , create_time                                                                               AS api_created_time
             , edit_time                                                                                 AS api_edit_time
             , GET_JSON_OBJECT(content, '$.is_cod')                                                      AS is_cod
             , GET_JSON_OBJECT(content, '$.l_id')                                                        AS l_id
             , REPLACE(GET_JSON_OBJECT(content, '$.send_date'), '/', '-')                                AS send_date
             , REPLACE(GET_JSON_OBJECT(content, '$.pay_date'), '/', '-')                                 AS pay_date
             , GET_JSON_OBJECT(content, '$.freight')                                                     AS freight
             , GET_JSON_OBJECT(content, '$.receiver_city')                                               AS receiver_city
             , GET_JSON_OBJECT(content, '$.receiver_state')                                              AS receiver_state
             , GET_JSON_OBJECT(content, '$.receiver_town')                                               AS receiver_town
             , GET_JSON_OBJECT(content, '$.receiver_name')                                               AS receiver_name
             , GET_JSON_OBJECT(content, '$.receiver_mobile')                                             AS receiver_mobile
             , GET_JSON_OBJECT(content, '$.wms_co_id')                                                   AS wms_co_id
             , GET_JSON_OBJECT(content, '$.logistics_company')                                           AS logistics_company
             , GET_JSON_OBJECT(content, '$.free_amount')                                                 AS free_amount
             , GET_JSON_OBJECT(content, '$.shop_id')                                                     AS shop_id
             , GET_JSON_OBJECT(content, '$.shop_name')                                                   AS shop_name
             , GET_JSON_OBJECT(content, '$.question_type')                                               AS question_type
             , GET_JSON_OBJECT(content, '$.outer_pay_id')                                                AS outer_pay_id
             , GET_JSON_OBJECT(content, '$.so_id')                                                       AS so_id
             , GET_JSON_OBJECT(content, '$.type')                                                        AS type
             , GET_JSON_OBJECT(content, '$.order_from')                                                  AS order_from
             , GET_JSON_OBJECT(content, '$.status')                                                      AS status
             , GET_JSON_OBJECT(content, '$.pay_amount')                                                  AS pay_amount
             , GET_JSON_OBJECT(content, '$.shop_buyer_id')                                               AS shop_buyer_id
             , GET_JSON_OBJECT(content, '$.open_id')                                                     AS open_id
             , GET_JSON_OBJECT(content, '$.shop_status')                                                 AS shop_status
             , REPLACE(GET_JSON_OBJECT(content, '$.order_date'), '/', '-')                               AS order_date
             , GET_JSON_OBJECT(content, '$.question_desc')                                               AS question_desc
             , GET_JSON_OBJECT(content, '$.o_id')                                                        AS o_id
             , GET_JSON_OBJECT(content, '$.co_id')                                                       AS co_id
             , GET_JSON_OBJECT(content, '$.drp_co_id_from')                                              AS drp_co_id_from
             , GET_JSON_OBJECT(content, '$.labels')                                                      AS labels
             , GET_JSON_OBJECT(content, '$.paid_amount')                                                 AS paid_amount
             , GET_JSON_OBJECT(content, '$.currency')                                                    AS currency
             , GET_JSON_OBJECT(content, '$.buyer_message')                                               AS buyer_message
             , GET_JSON_OBJECT(content, '$.lc_id')                                                       AS lc_id
             , GET_JSON_OBJECT(content, '$.invoice_title')                                               AS invoice_title
             , GET_JSON_OBJECT(content, '$.invoice_type')                                                AS invoice_type
             , GET_JSON_OBJECT(content, '$.buyer_tax_no')                                                AS buyer_tax_no
             , GET_JSON_OBJECT(content, '$.creator_name')                                                AS creator_name
             , replace(GET_JSON_OBJECT(content, '$.plan_delivery_date'), '/', '-')                       AS plan_delivery_date
             , GET_JSON_OBJECT(content, '$.node')                                                        AS node
             , GET_JSON_OBJECT(content, '$.drp_co_id_to')                                                AS drp_co_id_to
             , GET_JSON_OBJECT(content, '$.f_freight	')                                               AS f_freight
             , GET_JSON_OBJECT(content, '$.shop_site')                                                   AS shop_site
             , GET_JSON_OBJECT(content, '$.un_lid')                                                      AS un_lid
             , replace(GET_JSON_OBJECT(content, '$.end_time'), '/', '-')                                 AS end_time
             , GET_JSON_OBJECT(content, '$.receiver_country')                                            AS receiver_country
             , GET_JSON_OBJECT(content, '$.receiver_zip')                                                AS receiver_zip
             , GET_JSON_OBJECT(content, '$.seller_flag')                                                 AS seller_flag
             , GET_JSON_OBJECT(content, '$.receiver_email')                                              AS receiver_email
             , GET_JSON_OBJECT(content, '$.referrer_id')                                                 AS referrer_id
             , GET_JSON_OBJECT(content, '$.referrer_name')                                               AS referrer_name
             , GET_JSON_OBJECT(content, '$.items')                                                       AS items
             , REGEXP_REPLACE(REGEXP_REPLACE(GET_JSON_OBJECT(content, '$.items'), '\\[', ''), '\\]', '') AS itemlist
             , GET_JSON_OBJECT(content, '$.pays')                                                        AS pays
             , REGEXP_REPLACE(REGEXP_REPLACE(GET_JSON_OBJECT(content, '$.pays'), '\\[', ''), '\\]', '')  AS payslist
             , GET_JSON_OBJECT(content, '$.skus')                                                        AS skus
             , GET_JSON_OBJECT(content, '$.f_weight')                                                    AS f_weight
             , GET_JSON_OBJECT(content, '$.weight')                                                      AS weight
             , GET_JSON_OBJECT(content, '$.buyer_id')                                                    AS buyer_id
             , GET_JSON_OBJECT(content, '$.buyer_paid_amount')                                           AS buyer_paid_amount
             , GET_JSON_OBJECT(content, '$.seller_income_amount')                                        AS seller_income_amount
             , GET_JSON_OBJECT(content, '$.chosen_channel')                                              AS chosen_channel
             , replace(GET_JSON_OBJECT(content, '$.created'), '/', '-')                                  AS created
             , replace(GET_JSON_OBJECT(content, '$.modified'), '/', '-')                                 AS modified
        FROM ytdw.dwd_wms_data_d
        WHERE dayid = '${v_date}'
          AND data_type = 'saleOrder'
    )
   , trd_items AS
    (
        SELECT trds.*
             , STR_TO_MAP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(item_json, '\\{', ''), '\\}', ''), '\\"', ''),
                          '\\,', '\\:') AS item_json
        FROM trd_list AS trds
                 LATERAL VIEW EXPLODE(SPLIT(itemlist, '\\},\\{', false)) item AS item_json
    )
   , trd_pays AS
    (
        SELECT trds.*
             , STR_TO_MAP(
                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(pay_json_list, '\\{', ''), '\\}', ''), '\\"', ''), '\\,',
                '\\:') AS pay_json
        FROM trd_list AS trds
                 LATERAL VIEW EXPLODE(SPLIT(payslist, '\\},\\{', false)) pay AS pay_json_list
    )
   , trd_ord_amt AS
    (
        SELECT data_id
             , wms_name
             , SUM(trd_pays.pay_json['amount']) AS paid_amt
        FROM trd_pays
        GROUP BY data_id
               , wms_name
    )
   , sku_map AS
    (
        select DISTINCT sku_id, sku_type
        from (
                 select sku_id, 'combine' as sku_type
                 from ytdw.dwd_ytj_erp_itm_combline_item_d
                 where dayid = '$v_date'
                 union all
                 select sku_id, sku_type
                 from ytdw.dwd_ytj_erp_itm_item_d
                 where dayid = '$v_date'
             ) a
    )

INSERT
OVERWRITE
TABLE
dwd_ytj_erp_trd_com_ord_d
PARTITION
(
dayid = '${v_date}'
)
SELECT trd_item_list.data_id
     , trd_item_list.wms_name
     , CAST(CASE
                WHEN TOLOWER(trd_item_list.is_cod) = 'true' THEN 1
                ELSE 0
    END AS INT)                                                                               AS is_cod
     , trd_item_list.l_id
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.send_date), 'yyyyMMddHHmmss')               AS send_date
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.pay_date), 'yyyyMMddHHmmss')                AS pay_date
     , trd_item_list.freight
     , trd_item_list.receiver_city
     , trd_item_list.receiver_state
     , trd_item_list.receiver_town
     , trd_item_list.receiver_name
     , trd_item_list.receiver_mobile
     , trd_item_list.wms_co_id
     , trd_item_list.logistics_company
     , trd_item_list.free_amount
     , trd_item_list.shop_id
     , trd_item_list.shop_name
     , trd_item_list.question_type
     , trd_item_list.outer_pay_id
     , trd_item_list.so_id
     , trd_item_list.type
     , trd_item_list.order_from
     , CASE
           WHEN TOLOWER(trd_item_list.status) = 'waitpay' THEN '待付款'
           WHEN TOLOWER(trd_item_list.status) = 'delivering' THEN '发货中'
           WHEN TOLOWER(trd_item_list.status) = 'merged' THEN '被合并'
           WHEN TOLOWER(trd_item_list.status) = 'question' THEN '异常'
           WHEN TOLOWER(trd_item_list.status) = 'split' THEN '被拆分'
           WHEN TOLOWER(trd_item_list.status) = 'waitoutersent' THEN '等供销商|外仓发货'
           WHEN TOLOWER(trd_item_list.status) = 'waitconfirm' THEN '已付款待审核'
           WHEN TOLOWER(trd_item_list.status) = 'waitfconfirm' THEN '已客审待财审'
           WHEN TOLOWER(trd_item_list.status) = 'sent' THEN '已发货'
           WHEN TOLOWER(trd_item_list.status) = 'cancelled' THEN '取消'
           ELSE trd_item_list.status
    END                                                                                       as status
     , trd_item_list.pay_amount
     , trd_item_list.shop_buyer_id
     , trd_item_list.open_id
     , CASE
           WHEN TOLOWER(trd_item_list.shop_status) = 'wait_buyer_pay' THEN '等待买家付款'
           WHEN TOLOWER(trd_item_list.shop_status) = 'wait_seller_send_goods' THEN '等待卖家发货'
           WHEN TOLOWER(trd_item_list.shop_status) = 'wait_buyer_confirm_goods' THEN '等待买家确认收货'
           WHEN TOLOWER(trd_item_list.shop_status) = 'trade_finished' THEN '交易成功'
           WHEN TOLOWER(trd_item_list.shop_status) = 'trade_closed' THEN '付款后交易关闭'
           WHEN TOLOWER(trd_item_list.shop_status) = 'trade_closed_by_taobao' THEN '付款前交易关闭'
           ELSE trd_item_list.shop_status
    END                                                                                       AS shop_status
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.order_date), 'yyyyMMddHHmmss')              AS order_date
     , trd_item_list.question_desc
     , trd_item_list.o_id
     , trd_item_list.co_id
     , trd_item_list.drp_co_id_from
     , trd_item_list.labels
     , trd_item_list.paid_amount
     , trd_item_list.currency
     , trd_item_list.buyer_message
     , trd_item_list.lc_id
     , trd_item_list.invoice_title
     , trd_item_list.invoice_type
     , trd_item_list.buyer_tax_no
     , trd_item_list.creator_name
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.plan_delivery_date), 'yyyy-MM-dd HH:mm:ss') AS plan_delivery_date
     , trd_item_list.node
     , trd_item_list.drp_co_id_to
     , trd_item_list.f_freight
     , trd_item_list.shop_site
     , trd_item_list.un_lid
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.end_time), 'yyyy-MM-dd HH:mm:ss')           AS end_time
     , trd_item_list.receiver_country
     , trd_item_list.receiver_zip
     , trd_item_list.seller_flag
     , trd_item_list.receiver_email
     , trd_item_list.referrer_id
     , trd_item_list.referrer_name
     , trd_item_list.skus
     , trd_item_list.f_weight
     , trd_item_list.weight
     , trd_item_list.buyer_id
     , trd_item_list.buyer_paid_amount
     , trd_item_list.seller_income_amount
     , trd_item_list.chosen_channel
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.created), 'yyyyMMddHHmmss')                 AS created
     , FROM_UNIXTIME(UNIX_TIMESTAMP(trd_item_list.modified), 'yyyyMMddHHmmss')                AS modified
     , CASE
           WHEN TOLOWER(trd_item_list.item_json['is_gift']) = 'true' THEN 1
           WHEN TOLOWER(trd_item_list.item_json['is_gift']) = '是' THEN 1
           ELSE 0
    END                                                                                       AS is_gift
     , trd_item_list.item_json['sku_id']                                                      AS sku_id
     , trd_item_list.item_json['name']                                                        AS name
     , CASE
           WHEN TOLOWER(trd_item_list.item_json['refund_status']) = 'none' THEN '未申请'
           WHEN TOLOWER(trd_item_list.item_json['refund_status']) = 'waiting' THEN '退款中'
           WHEN TOLOWER(trd_item_list.item_json['refund_status']) = 'success' THEN '退款成功'
           WHEN TOLOWER(trd_item_list.item_json['refund_status']) = 'closed' THEN '退款关闭'
           ELSE trd_item_list.item_json['refund_status']
    END                                                                                       AS refund_status
     , trd_item_list.item_json['refund_id']                                                   AS refund_id
     , trd_item_list.item_json['price']                                                       AS price
     , trd_item_list.item_json['outer_oi_id']                                                 AS outer_oi_id
     , trd_item_list.item_json['item_status']                                                 AS item_status
     , trd_item_list.item_json['i_id']                                                        AS i_id
     , trd_item_list.item_json['properties_value']                                            AS properties_value
     , trd_item_list.item_json['oi_id']                                                       AS oi_id
     , trd_item_list.item_json['amount']                                                      AS amount
     , trd_item_list.item_json['shop_sku_id']                                                 AS shop_sku_id
     , trd_item_list.item_json['raw_so_id']                                                   AS raw_so_id
     , trd_item_list.item_json['qty']                                                         AS qty
     , CAST(CASE
                WHEN TOLOWER(trd_item_list.item_json['is_presale']) = 'true' THEN 1
                WHEN TOLOWER(trd_item_list.item_json['is_presale']) = 'false' THEN 0
                ELSE trd_item_list.item_json['is_presale']
    END AS INT)                                                                               AS is_presale
     , trd_item_list.item_json['base_price']                                                  AS base_price
     , trd_item_list.item_json['pic']                                                         AS pic
     , COALESCE(trd_item_list.item_json['sku_type'], sp.sku_type)                             AS sku_type
     , trd_item_list.item_json['shop_i_id']                                                   AS shop_i_id
     , trd_item_list.item_json['buyer_paid_amount']                                           AS sub_buyer_paid_amount
     , trd_item_list.item_json['seller_income_amount']                                        AS sub_seller_income_amount
     , trd_item_list.item_json['referrer_id']                                                 AS sub_referrer_id
     , trd_ord_amt.paid_amt
     , trd_item_list.api_created_time
     , trd_item_list.api_edit_time
FROM trd_items AS trd_item_list
         LEFT JOIN trd_ord_amt
                   ON trd_item_list.data_id = trd_ord_amt.data_id AND trd_item_list.wms_name = trd_ord_amt.wms_name
         LEFT JOIN sku_map sp ON trd_item_list.item_json['sku_id'] = sp.sku_id