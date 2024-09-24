package com.yangt.reconciliation.application.actuator.gw.script;

import com.yangt.reconciliation.application.actuator.gw.AbstractGwResultParser;
import com.yangt.reconciliation.common.constants.Constants;
import com.yangt.reconciliation.common.exception.biz.BizException;
import com.yangt.reconciliation.common.util.CSVUtil;
import com.yangt.reconciliation.common.util.DateUtil;
import com.yangt.reconciliation.common.util.FileUtil;
import com.yangt.reconciliation.common.util.FtpUtil;
import com.yangt.reconciliation.common.util.StringUtils;
import com.yangt.reconciliation.common.util.ZipUtil;
import com.ytgw.facade.message.SupergwMessage;
import com.ytgw.facade.message.xml.MessageXmlParser;
import com.ytgw.facade.result.TransResult;
import org.apache.commons.net.ftp.FTPClient;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSON;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class YiLingGwAsync extends AbstractGwResultParser {

    public Object execute(Map<String, Object> params) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        if (params == null) {
            return null;
        }
        Object merchantNum = params.get("merchantNum");
        if (merchantNum == null) {
            return null;
        }
        Object md5Key = params.get("md5Key");
        if (md5Key == null) {
            return null;
        }
        Object orderType = params.get("orderType");
        if (orderType == null) {
            orderType = "all";
        }
        Object pullDate = params.get("pullDate");
        String from = "";
        String end = "";
        if (null != pullDate) {
            Date formatDate = DateUtil.getDateByString(String.valueOf(pullDate), DateUtil.DATA_FORMANT2);
            from = DateUtil.getFormatDate(DateUtil.getDateStart(DateUtil.addDays(formatDate, -1)),
                    DateUtil.DATA_FORMANT);
            end = DateUtil.getFormatDate(DateUtil.getDateStart(DateUtil.addDays(formatDate, -0)),
                    DateUtil.DATA_FORMANT);
        } else {
            from = DateUtil.getFormatDate(DateUtil.getDateStart(DateUtil.addDays(new Date(), -1)),
                    DateUtil.DATA_FORMANT);
            end = DateUtil.getFormatDate(DateUtil.getDateStart(DateUtil.addDays(new Date(), -0)),
                    DateUtil.DATA_FORMANT);
        }

        SupergwMessage supergwMessage = new SupergwMessage();
        supergwMessage.addField("orderType", orderType.toString());
        supergwMessage.addField("payClient", "all");
        supergwMessage.addField("endTime", end);
        supergwMessage.addField("startTime", from);
        supergwMessage.addField("merchantNum", merchantNum.toString());
        supergwMessage.addField("version", "1.0");
        supergwMessage.addField("md5Key", md5Key.toString());

        // 发起异步申请
        TransResult transResult = gwFacade.callGwService("LEADERPAY02020202", "LEADERPAY02", supergwMessage.toXML(), 1);
        if (transResult == null || !transResult.isSuccess() || StringUtils.isEmpty(transResult.getContext())) {
            throw new BizException("移领账单发起异步申请失败");
        }

        SupergwMessage result = MessageXmlParser.toObject(transResult.getContext());
        String uuid = result.g("uuid");
        if (uuid == null) {
            throw new BizException("移领账单获取任务UUID失败");
        }

        // 轮询查询任务执行状态
        supergwMessage = new SupergwMessage();
        supergwMessage.addField("uuid", uuid);
        for (int i = 0; i < 20; i++) { // 最多重试检查20次，每次30s，共600s/10min
            transResult = gwFacade.callGwService("LEADERPAY02020203", "LEADERPAY02", supergwMessage.toXML(), 1);
            if (transResult == null || !transResult.isSuccess() || StringUtils.isEmpty(transResult.getContext())) {
                throw new BizException("移领账单轮询任务状态失败 by i=" + i);
            }

            result = MessageXmlParser.toObject(transResult.getContext());
            JSONObject jobj = JSON.parseObject(result.g("resultData"));
            String status = jobj.getString("status");
            if ("1".equals(status)) { // 任务已完成、可以下载文件了
                String downloadUrl = jobj.getString("ossFileUrl");
                if (StringUtils.isEmpty(downloadUrl)) {
                    throw new BizException("移领账单已完成，但下载Url为空");
                }
                String fileName = getFileName(downloadUrl);
                String descDir = Constants.DOWNLOAD_PATH + "yiling" + Constants.PATH_SPLIT;
                StringBuilder sb = new StringBuilder(descDir);
                sb.append(fileName);
                sb.append(".csv");
                try {
                    // 文件下载
                    FileUtil.downFile(downloadUrl, sb.toString());
                    resultData = CSVUtil.readCSV(sb.toString(), Constants.GBK_CHARSET);
                    resultData.remove(0);
                } catch (Exception e) {
                    throw new BizException("yiling error", e);
                }
                break;
            } else { // 任务未完成，等待30s
                try {
                    Thread.currentThread().sleep(30 * 1000);
                } catch (InterruptedException ex) {
                    throw ex;
                }
            }
        }

        return resultData;
    }

    public String getFileName(String url) {
        String[] split1 = url.split("/");
        String zipName = "";
        if (split1.length > 0) {
            zipName = split1[split1.length - 1];
        }
        String fileName = "";
        if (StringUtils.isNotEmpty(zipName)) {
            String[] s1 = zipName.split("[.]");
            if (s1.length > 0) {
                fileName = s1[0];
            }
        }
        return fileName;
    }
}