package com.yangt.reconciliation.application.actuator.gw.script;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.yangt.reconciliation.application.db.DBQuery;
import com.yangt.reconciliation.application.domain.aggregate.CheckingAggregate;
import com.yangt.reconciliation.application.service.CheckingQueryService;
import com.yangt.reconciliation.common.constants.Constants;
import com.yangt.reconciliation.dao.mapper.DataSourceResultsMapper;
import com.yangt.reconciliation.dao.model.CheckingResults;
import com.yangt.reconciliation.dao.model.DataSourceResults;
import org.apache.commons.collections4.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralChecking {
    @Resource
    private CheckingQueryService checkingQueryService;
    @Resource
    private DataSourceResultsMapper dataSourceResultsMapper;
    private static final Logger log = LoggerFactory.getLogger(GeneralChecking.class);

    public Object execute(Map<String, Object> params) {
        if (params == null) {
            return null;
        }
        Object o = params.get(Constants.CHECK_AGGREGATE);
        Object executeDate = params.get("executeDate");
        if (o == null) {
            return null;
        }
        log.info("[GeneralChecking-execute] checkingId={}", o.toString());
        CheckingAggregate checkingAggregate = checkingQueryService.selectById(Long.valueOf(o.toString()));
        final Long dataSourceId = checkingAggregate.getDataSourceId();
        Map<String, List<Long>> reusltMap = new HashMap<>();
        List<Long> successIds = new ArrayList<>();
        List<Long> errorIds = new ArrayList<>();
        for(int page = 0; page< 10000; page++) {
            List<DataSourceResults> dataSourceResults = getDataSourceResults(dataSourceId, executeDate, page);
            if(CollectionUtils.isEmpty(dataSourceResults)) {
                break;
            }
            log.info("[GeneralChecking-execute] dataSourceResults size={}", dataSourceResults.size());
            List<String> matchKeyValues = new ArrayList<>();
            List<Long> ids = new ArrayList<>();
            for (DataSourceResults results : dataSourceResults) {
                matchKeyValues.add(results.getMatchKeyValue());
                ids.add(results.getId());
            }
            if (CollectionUtils.isEmpty(matchKeyValues)) {
                errorIds.addAll(ids);
                continue;
            }
            String sql = getSql(matchKeyValues);
            List<JSONObject> query = DBQuery.query(sql);
            if (CollectionUtils.isEmpty(query)) {
                log.info("[GeneralChecking-execute] query is empty");
                errorIds.addAll(ids);
                continue;
            }
            Map<String, JSONObject> targetDataMap = new HashMap<>();
            for (JSONObject data : query) {
                targetDataMap.put(data.getString("third_serial"), data);
            }
            List<CheckingResults> checkingResults = new ArrayList<>();
            for (DataSourceResults result : dataSourceResults) {
                String matchKeyValue = result.getMatchKeyValue();
                String data = result.getData();
                boolean checkResult = false;
                String error = null;
                JSONObject jsonObject = null;
                JSONObject targetData = null;
                try{
                    jsonObject = JSONObject.parseObject(data);
                    Double amount = jsonObject.getDouble("amount");
                    targetData = targetDataMap.get(matchKeyValue);
                    if (amount == null) {
                        errorIds.add(result.getId());
                        error = "三方渠道金额为空";
                        continue;
                    }
                    if (targetData == null) {
                        errorIds.add(result.getId());
                        error = "我方数据为空";
                        continue;
                    }
                    Object o1 = targetData.getLong("amount");
                    final BigDecimal tem = new BigDecimal(o1.toString());
                    o1 = tem.divide(new BigDecimal(100)).doubleValue();
                    targetData.put("amount", o1);
                    if (amount.equals(o1)) {
                        checkResult = true;
                        successIds.add(result.getId());
                    } else {
                        errorIds.add(result.getId());
                        error = "金额不等";
                    }
                } catch (Exception e) {
                    errorIds.add(result.getId());
                    error = e.getMessage();
                }
                jsonObject = jsonObject == null ? new JSONObject() : jsonObject;
                targetData = targetData == null ? new JSONObject() : targetData;
                checkingResults.add(build(dataSourceId, checkingAggregate.getId(), checkResult, error, jsonObject, targetData));
            }
//            if (CollectionUtils.isNotEmpty(checkingResults)) {
//                checkingResultsMapper.insertBatch(checkingResults);
//            }
        }
        reusltMap.put("successIds", successIds);
        reusltMap.put("errorIds", errorIds);
        return reusltMap;
    }

    public List<DataSourceResults> getDataSourceResults(Long dataSourceId, Object executeDate, Integer page) {
        List<DataSourceResults> dataSourceResults = new ArrayList<>();
        if(executeDate != null) {
            log.info("[GeneralChecking-execute] dataSourceId={}, executeDate={}", dataSourceId, executeDate.toString());
            dataSourceResults = dataSourceResultsMapper.pageNoCheckResultByDate(dataSourceId, page * 1000, 1000, Long.valueOf(executeDate.toString()));
        } else {
            log.info("[GeneralChecking-execute] dataSourceId={}, executeDate=null", dataSourceId);
            dataSourceResults = dataSourceResultsMapper.pageNoCheckResult(dataSourceId, page * 1000, 1000);
        }
        return dataSourceResults;
    }

    public CheckingResults build(Long dataSourceId, Long checkingId, boolean check, String error, JSONObject data, JSONObject data1) {
        final CheckingResults checkingResults = new CheckingResults();
        checkingResults.setCheckingId(checkingId);
        checkingResults.setDataSourceData(JSON.toJSONString(Lists.newArrayList(data)));
        checkingResults.setDataSourceId(dataSourceId);
        checkingResults.setResult(check ? 1 : 0);
        checkingResults.setError(error);
        checkingResults.setTargetDataSourceData(JSON.toJSONString(Lists.newArrayList(data1)));
        checkingResults.setTargetDataSourceId(dataSourceId);
        checkingResults.setCreateTime(new Date());
        checkingResults.setEditTime(new Date());
        checkingResults.setIsFixed(false);
        checkingResults.setIsDeleted((byte)0);
        return checkingResults;
    }

    public static String getSql(List<String> matchKeyValues) {
        String sql = "select refund_amount as amount, refund_channel_settle_no as orderNo, refund_sub_no as third_serial from yt_pay.t_refund_sub where refund_sub_no in(";
        StringBuilder sb = new StringBuilder();
        for (String key : matchKeyValues) {
            sb.append("'").append(key).append("',");
        }
        sql = sql + sb.substring(0, sb.length() - 1) + ")";
        return sql;
    }
}