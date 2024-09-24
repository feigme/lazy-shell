package message.parser.weixin02;

import com.ytgw.common.util.StringUtil;
import com.ytgw.common.util.DateUtil;
import com.ytgw.core.shared.exception.YtgwException;
import com.ytgw.core.shared.exception.code.ParserErrorCode;
import com.ytgw.core.shared.message.domain.SupergwMessage;
import com.ytgw.core.shared.service.message.parser.TextMessageParser;
import com.ytgw.security.bcm.util.BcmUtil;
import com.ytgw.common.util.JsonUtil

/**
 * 微信-app支付
 */
public class weixin02010106 extends TextMessageParser {

    public SupergwMessage parse(String message) {
        SupergwMessage gw = new SupergwMessage();
        if (StringUtil.isBlank(message)) {
            throw new YtgwException(ParserErrorCode.PARSE_ERROR, "返回报文信息为空");
        }
        def response = new XmlSlurper().parseText(message)
        String return_code = response.return_code.toString();
        String return_msg = response.return_msg.toString();
        String appid = response.appid.toString();
        String mch_id = response.mch_id.toString();
        String device_info = response.device_info.toString();
        String nonce_str = response.nonce_str.toString();
        String sign = response.sign.toString();
        String result_code = response.result_code.toString();
        String prepay_id = response.prepay_id.toString();
        String trade_type = response.trade_type.toString();
        String code_url = response.code_url.toString();
        gw.addField("code_url", code_url);
        gw.addField("prepay_id", prepay_id);
        gw.addField("mchId", mch_id);
        gw.setCommunicationResultCode(return_code);
        gw.setBusinessResultCode(result_code);
        gw.setChannelResponseCode(return_code + "_" + result_code);
        gw.setChannelResponseMessage(return_msg);
        return gw;
    }

    public void share(SupergwMessage inMessage, SupergwMessage outMessage) {
	    String MD5_KEY = outMessage.g("MD5_KEY")
                                 inMessage.addField("initalRequest", outMessage.g("initalRequest"));
		String noncestr = "R"+DateUtil.getRandomNumberCode(20)
		String timestamp = StringUtil.substring(DateUtil.currentTimeMillis()+"",0,10)
		String prepay_id = inMessage.g("prepay_id")
		String appid = outMessage.g("appId")
		String partnerid = outMessage.g("mchId")
		//String weixinpackage = "Sign=WXPay"
		String weixinpackage = "prepay_id="+prepay_id
		String total_fee = outMessage.g("total_fee")
		
		Map<String, String> sParaTemp = new HashMap<String, String>();


		sParaTemp.put("MD5_KEY", MD5_KEY);
		sParaTemp.put("nonceStr", noncestr);
		sParaTemp.put("timeStamp", timestamp);
		sParaTemp.put("appId", appid);
		sParaTemp.put("package", weixinpackage);
		sParaTemp.put("signType", "MD5");

		
		BcmUtil bcmUtil = new BcmUtil();
		String unsigned = JsonUtil.toJSONString(sParaTemp);
		String signed = bcmUtil.sign("weixin", unsigned);
		
        inMessage.addField("outOrderNo", outMessage.g("orderNo"));
		inMessage.addField("appId", appid);
		inMessage.addField("partnerId", partnerid);
		inMessage.addField("package", weixinpackage);
		inMessage.addField("nonceStr", noncestr);
		inMessage.addField("timeStamp", timestamp);
		inMessage.addField("paySign", signed);
		inMessage.addField("signType", "MD5");
    }
}