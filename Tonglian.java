package com.yangt.reconciliation.application.actuator.gw.script;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yangt.reconciliation.application.actuator.gw.AbstractGwResultParser;
import com.yangt.reconciliation.application.domain.entity.data.DataSourceResultEntity;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TongLian9ndcGw extends AbstractGwResultParser {

    public List<Map<String, Object>> execute(Map<String, Object> params) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        if (params == null) {
            return null;
        }
        Object cusId = params.get("cusId");
        if (cusId == null) {
            return null;
        }
        Object appId = params.get("appId");
        Object upload = params.get("upload");
        SupergwMessage supergwMessage = new SupergwMessage();
        String formatDate = DateUtil.getFormatDate(DateUtil.addDays(new Date(), -1), DateUtil.DATA_FORMANT2);
        Object pullDate = params.get("pullDate");
        if (pullDate != null) {
            Date pullDate1 = DateUtil.getDateByString(String.valueOf(pullDate), DateUtil.DATA_FORMANT2);
            formatDate = DateUtil.getFormatDate(DateUtil.addDays(pullDate1, -1), DateUtil.DATA_FORMANT2);
        }
        supergwMessage.addField("date", formatDate);
        supergwMessage.addField("appId", appId.toString());
        supergwMessage.addField("cusId", cusId.toString());
        final TransResult transResult = gwFacade.callGwService("ALLINPAY02010202", "ALLINPAY02", supergwMessage.toXML(),
                1);
        if (transResult == null || !transResult.isSuccess() || StringUtils.isEmpty(transResult.getContext())) {
            throw new BizException("通联账户");
        }
        final String context = transResult.getContext();

        SupergwMessage result = MessageXmlParser.toObject(context);
        String downloadUrl = result.g("url");
        if (StringUtils.isEmpty(downloadUrl)) {
            return null;
        }
        String fileName = getFileName(downloadUrl);
        String descDir = Constants.DOWNLOAD_PATH + "tonglian" + Constants.PATH_SPLIT;
        StringBuilder sb = new StringBuilder(descDir);
        sb.append(fileName);
        sb.append(".zip");
        InputStream inputStream = null;
        try {
            // 文件下载
            File zipFile = FileUtil.downFile(downloadUrl, sb.toString());
            if (upload != null && "true".equals(upload.toString())) {
                FTPClient ftpClient = FtpUtil.getFTPClient("172.16.222.75", "yangtuo", "IblUB1AfJ6uV6d", 2211);
                ftpClient.makeDirectory(formatDate);
                FtpUtil.uploads(ftpClient, formatDate, new File(zipFile.getPath()));
            }
            // 文件解压
            ZipUtil.unZipFiles(sb.toString(), descDir);
            File file = new File(descDir + cusId + ".xlsx");
            inputStream = new FileInputStream(file);
            resultData = read(inputStream);
        } catch (Exception e) {
            throw new BizException("aliPay error", e);
        }

        return resultData;
    }

    public List<Map<String, Object>> read(InputStream inputStream) {
        List<Map<String, Object>> results = new ArrayList<>();
        EasyExcelFactory.read(inputStream, new AnalysisEventListener() {
            @Override
            public void invoke(Object data, AnalysisContext context) {
                Map<Integer, Object> result = (Map<Integer, Object>) data;
                if (result.size() > 15 && !"终端号".equals(result.get(0))) {
                    Set<Integer> integers = result.keySet();
                    Map<String, Object> dataMap = new HashMap<>();
                    for (Integer key : integers) {
                        if (key == null) {
                            continue;
                        }
                        dataMap.put(key.toString(), result.get(key));
                    }
                    if (dataMap.size() > 0) {
                        results.add(dataMap);
                    }
                }
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {

            }
        }).sheet().doRead();
        return results;
    }

    /**
     *
     * @return
     */
    public String getFileName(String url) {
        String[] split = url.trim().split("[?]");
        if (split.length < 1) {
            return StringUtils.EMPTY;
        }
        String s = split[0];
        String[] split1 = s.split("/");
        String zipName = "";
        if (split1.length > 0) {
            zipName = split1[split1.length - 1];
        }
        String fileName = "";
        if (StringUtils.isNotEmpty(zipName)) {
            String[] s1 = zipName.split("_");
            if (s1.length > 0) {
                fileName = s1[0];
            }
        }
        return fileName;
    }
}