#!/bin/bash

pods=$(sudo kubectl get pod  | grep 'deploy-' | awk '{print $1}')
for pod in $pods; do
    echo -e "\033[36m${pod}\033[0m"
    appCode=$(echo $pod | sed 's/-k8sytj.*//g')
    # 检查是否有dubbo服务
    # 特殊的名称处理
    if [ "$appCode" = "ytrtcreco" ]; then
        appCode="yt_rtc_reco"
    elif [ "$appCode" = "ytwatcher" ]; then
        appCode="yt_watcher"
    elif [ "$appCode" = "ytsgc" ]; then
        appCode="yt_sgc"
    elif [ "$appCode" = "ytlibra" ]; then
        appCode="yt_libra"
    elif [ "$appCode" = "ytselect" ]; then
        appCode="yt_select"
    elif [ "$appCode" = "ytrecort" ]; then
        appCode="yt_reco_rt"
    elif [ "$appCode" = "ytdataxserver" ]; then
        appCode="yt_datax_server"
    fi

    echo "appCode: $appCode"
    sudo kubectl exec -it $pod -- find /alidata/log -type f | grep -v "\.log" | grep -v "catalina.out" | grep -v "\.txt" | grep -v "app.err"
done