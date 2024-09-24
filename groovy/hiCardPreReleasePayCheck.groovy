def serialDirection =#serial_direction#;
def cardId = #card_id#;
def serialId = #id#;

return ['code':200,'cardId':cardId,'serialDirection':serialDirection,'serialId':serialId]


def serialDirection = #source.serialDirection#;
def cardId = #source.cardId#;
def serialId =#source.serialId#
if(serialDirection.toInteger() != 0){
    // 非使用hi卡支付，直接返回
    return ['code':200,'desc':'过滤，不是hi卡支付流水']
}
// 查询预发放hi卡
List<Map<String,String>> userCardList = runSqlList('select * from yt_trade.t_user_card where is_deleted = 0 and is_pre_released = 1 and id = \''+cardId+'\'');
if(userCardList.isEmpty()){
    // 非预发放hi卡，直接返回
    return ['code':200,'desc':'过滤，支付使用的hi卡不是预发放卡']
}

def outBizId = userCardList.get(0).get('out_biz_id')
def shopId = userCardList.get(0).get('shop_id')


def tb = Math.abs(shopId.hashCode()%64)

List<Map<String,String>> sendPreReleaseHiCardTradeList = runSqlList('select * from yt_trade.pt_trade_shop_'+tb+' where trade_id = \''+outBizId+'\'');
if(sendPreReleaseHiCardTradeList.isEmpty()){
    return ['code':500,'desc':'预发放hi卡发卡订单未查询到, tradeId='+outBizId]
}
def tags = sendPreReleaseHiCardTradeList.get(0).get('tags')
def sendPreReleaseHiCardParentTradeId = sendPreReleaseHiCardTradeList.get(0).get('parent_trade_id')

def hasTag = tags.split(',').any { it.toInteger() == 118 }
if(!hasTag) {
    return ['code':500,'desc':'预发放hi卡订单没有tag=118']
}

List<Map<String,String>> serialDetailList = runSqlList('select * from yt_trade.t_card_fund_serial_details where is_deleted = 0 and card_fund_serial_id = ' + serialId);
if(serialDetailList.isEmpty()){
    return ['code':500,'desc':'预发放hi卡使用的流水详情未查询到']
}
def useCardTradeId = serialDetailList.get(0).get('out_biz_id')
List<Map<String,String>> usePreReleaseHiCardTradeList = runSqlList('select * from yt_trade.pt_trade_shop_'+tb+' where trade_id = \''+useCardTradeId+'\'');
if(usePreReleaseHiCardTradeList.isEmpty()){
    return ['code':500,'desc':'使用预发放hi卡的订单未查询到, tradeId='+outBizId]
}
def usePreReleaseHiCardParentTradeId = usePreReleaseHiCardTradeList.get(0).get('parent_trade_id')
if(usePreReleaseHiCardParentTradeId == sendPreReleaseHiCardParentTradeId) {
    return ['code':200,'desc':'买预发放卡并同时使用']
} else {
    return ['code':500,'desc':'支付使用的预发放hi卡, 不是本次交易购买的预发放hi卡','发卡tradeId':sendPreReleaseHiCardParentTradeId,'使用预发放hi卡tradeId':usePreReleaseHiCardParentTradeId]
}