#!/bin/bash

# cat 0616netstat.log | grep -v "0.0.0.0:" | awk '{if($7 ~ /\.*java/){print $0}}' | awk '{if($5 !~ /172\.17\.\.*/ && $5 !~ /\.*:3306/){print $0}}:q
targetApp=$1
podInfos=$(sudo kubectl get pod -o wide | awk '{print $1":"$6}')
for podInfo in $podInfos; do
    pod=${podInfo%%:*}
    ip=${podInfo##*:}
    appCode=$(echo $pod | sed 's/-k8sytj.*//g')

    # pod名称必定大于38
    if [ ${#pod} -lt 38 ]; then
        continue
    fi  

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

    if [ -n "$targetApp" ] && [ "$targetApp" != "$appCode" ];then
        continue
    fi

    echo -e "\033[36m${pod}\033[0m"

    sudo kubectl exec -it $pod -- netstat -naopt >temp.log

    # 检查开放端口情况
    ports=$(cat temp.log | grep "java" | grep "LISTEN" | awk '{if($4 ~ /0\.0\.0\.0:.*/){print $4}}' | sed 's/0.0.0.0://g' | sort -n | awk '{ for(i=1;i<=NF;i++){ if(NR==1){ arr[i]=$i; }else{ arr[i]=arr[i]"\t"$i; } } } END{ for(i=1;i<=NF;i++){ print arr[i]; } }')
    echo -e "\033[32m> 应用占用端口  >> ${ports}\033[0m"

    # 检查数据库链接情况
    for db in $(cat temp.log | grep "java" | awk '{if($5 ~ /.*:3306$/){print $5,$6}}' | awk '{ar[$1]=$0}; END{for (i in ar) print ar[i];}' | awk '{print $1"="$2}'); do
        if [ "${db##*=}" = "ESTABLISHED" ]; then
            echo -e "\033[32m> 数据库链接    >> ${db%%=*}   DB链接正常\033[0m"
        elif [ "${db##*=}" = "SYN_SENT" ]; then
            echo -e "\033[31m> 数据库链接    >> ${db%%=*}   DB网络不通\033[0m"
        else
            echo -e "\033[31m> 数据库链接    >> ${db%%=*}   ${db##*=}\033[0m"
        fi
    done

    # 检查mq链接情况
    for db in $(cat temp.log | grep "java" | awk '{if($5 ~ /.*:9876$/ || $5 ~ /.*:40911$/ || $5 ~ /.*:30909$/ || $5 ~ /.*:30911$/){print $5,$6}}' | awk '{ar[$1]=$0}; END{for (i in ar) print ar[i];}' | awk '{print $1"="$2}'); do
        if [ "${db##*=}" = "ESTABLISHED" ]; then
            echo -e "\033[32m> MQ链接        >> ${db%%=*}   MQ链接正常\033[0m"
        elif [ "${db##*=}" = "SYN_SENT" ]; then
            echo -e "\033[31m> MQ链接        >> ${db%%=*}   MQ网络不通\033[0m"
        else
            echo -e "\033[31m> MQ链接        >> ${db%%=*}   ${db##*=}\033[0m"
        fi
    done

    # 检查kafka链接情况
    for db in $(cat temp.log | grep "java" | awk '{if($5 ~ /.*:9092$/){print $5,$6}}' | awk '{ar[$1]=$0}; END{for (i in ar) print ar[i];}' | awk '{print $1"="$2}'); do
        if [ "${db##*=}" = "ESTABLISHED" ]; then
            echo -e "\033[32m> Kafka链接      >> ${db%%=*}   Kafka链接正常\033[0m"
        elif [ "${db##*=}" = "SYN_SENT" ]; then
            echo -e "\033[31m> Kafka链接      >> ${db%%=*}   Kafka网络不通\033[0m"
        else
            echo -e "\033[31m> Kafka链接      >> ${db%%=*}   ${db##*=}\033[0m"
        fi
    done


    # 1. 查询dubbo name
    dubboNameJson=$(curl -s "http://es.yangtuojia.com/getDubboAppName?appCmdbName=${appCode}")
    dubboName=$(echo $dubboNameJson | sed 's/.*data"://g' | sed 's/,.*//g' | sed 's/"//g')
    # 2. 是否提供dubbo
    if [ ! "$dubboName" = "null" ]; then
        # 3. 查询是否注册到zk
        zkStr=$(curl -s "http://register-ops.yangtuojia.com/providers?cluster=k8s&app=${dubboName}&host=${ip}")
        if [ ${#zkStr} -lt 100 ]; then
            echo -e "\033[33m> dubbo服务检查 >> dubboName: $dubboName    ip: $ip   >>>>    k8s zk 上没有发现 dubbo \033[0m"
        else
            echo -e "\033[32m> dubbo服务检查 >> dubboName: $dubboName    ip: $ip   >>>>    dubbo服务正常 \033[0m"
        fi
    fi

    cat temp.log | while read line; do

        # --
        appIp=$(echo $line | awk '{print $4}')
        targetIpPort=$(echo $line | awk '{print $5}')
        netState=$(echo $line | awk '{print $6}')
        targetType=$(echo $line | awk '{print $7}')

        ip=${appIp%%:*}
        if [[ ! "$ip" =~ [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+ ]]; then
            continue
        fi

        # 过滤自身
        if [[ "$targetIpPort" =~ 0\.0\.0\.0:.* ]]; then
            continue
        fi

        # 过滤非java进程的
        if [[ ! $targetType =~ .*java$ ]]; then
            continue
        fi

        # 过滤链接的ip是172.17开头的
        if [[ "$targetIpPort" =~ 172\.17\..* ]]; then
            continue
        fi

        # 过滤slb过来的链接
        if [[ "$targetIpPort" =~ 100\.100\..* ]]; then
            continue
        fi

        # 链接到测试k8s的
        if [[ "$targetIpPort" =~ 10\.100\..*:20880 ]]; then
            echo "> $appIp    $targetIpPort   >>>>   链接测试 dubbo"
            continue
        fi

        # 过滤es的链接
        if [[ "$targetIpPort" =~ .*:9201$ ]]; then
            continue
        fi

        # 过滤链接数据
        if [[ "$targetIpPort" =~ .*:3306$ ]]; then
            continue
        fi

        # 过滤链接 443 的
        if [[ "$targetIpPort" =~ .*:443$ ]]; then
            continue
        fi

        # 过滤链接zk
        if [[ "$targetIpPort" =~ .*:2181$ ]]; then
            continue
        fi
        if [[ "$targetIpPort" =~ 172\.16\.120\.140:.* ]] || [[ "$targetIpPort" =~ 172\.16\.121\.134:.* ]]; then
            echo "> $appIp    $targetIpPort   >>>>   链接测试环境zk"
            continue
        fi

        # 过滤mq的链接
        if [[ "$targetIpPort" =~ .*:9876$ ]] || [[ "$targetIpPort" =~ .*:40911 ]] || [[ "$targetIpPort" =~ .*:30909 ]] || [[ "$targetIpPort" =~ .*:30911 ]]; then
            continue
        fi

        # 过滤kafka
        if [[ "$targetIpPort" =~ .*:9092$ ]]; then
            continue
        fi

        # 过滤redis
        if [[ "$targetIpPort" =~ .*:6379$ ]]; then
            continue
        fi

        # 过滤hop的请求
        if [[ "$targetIpPort" =~ 172\.16\.21\.111:.* ]] || [[ "$targetIpPort" =~ 172\.16\.21\.151:.* ]] || [[ "$targetIpPort" =~ 172\.16\.21\.150:.* ]] || [[ "$targetIpPort" =~ 172\.16\.60\.44:.* ]] || [[ "$targetIpPort" =~ 172\.16\.60\.43:.* ]]; then
            continue
        fi

        # 过滤disconf链接
        if [[ "$targetIpPort" =~ 172\.16\.18\.125:80$ ]]; then
            continue
        fi

        # 过滤pinpoint slb的ip
        if [[ "$targetIpPort" =~ 172\.16\.16\.31:.* ]]; then
            continue
        fi
        if [[ "$targetIpPort" =~ 172\.16\.120\.85:.* ]]; then
            echo "> $appIp    $targetIpPort   >>>> 链接到测试的 pinpoint"
            continue
        fi

        # 自己http链接自己的
        if [[ "$targetIpPort" =~ 127\.0\.0\.1:.* ]]; then
            continue
        fi

        # rlp待下线
        if [[ "$targetIpPort" =~ 172\.16\.12\.63:44444 ]]; then
            echo "> $appIp    $targetIpPort   >>>> rlp 待下线"
            continue
        fi

        # http访问80端口的，没法限制，过滤掉
        # if [[ "$targetIpPort" =~ .*:80$ ]]; then
        #     continue
        # fi

        if [ "$targetIpPort" = "172.16.22.228:28845" ] || [ "$targetIpPort" = "172.16.22.227:28845" ]; then
            echo "> $appIp    $targetIpPort   >>>>   alita-doc富客户端, zk地址的key: alita.doc.client.dubbo-consumer-app-owner"
            continue
        fi
        if [ "$targetIpPort" = "172.16.18.11:28888" ] || [ "$targetIpPort" = "172.16.18.10:28888" ]; then
            echo "> $appIp    $targetIpPort   >>>>   toc富客户端, zk地址的key: toc.dubbo-register-address"
            continue
        fi
        if [ "$targetIpPort" = "172.16.18.126:28888" ] || [ "$targetIpPort" = "172.16.18.133:28888" ] || [ "$targetIpPort" = "172.16.18.155:28888" ] || [ "$targetIpPort" = "172.16.18.116:28888" ] || [ "$targetIpPort" = "172.16.18.127:28888" ] || [ "$targetIpPort" = "172.16.18.117:28888" ] || [ "$targetIpPort" = "172.16.18.153:28888" ] || [ "$targetIpPort" = "172.16.18.119:28888" ] || [ "$targetIpPort" = "172.16.18.118:28888" ] || [ "$targetIpPort" = "172.16.18.128:28888" ] || [ "$targetIpPort" = "172.16.18.131:28888" ] || [ "$targetIpPort" = "172.16.18.132:28888" ] || [ "$targetIpPort" = "172.16.18.152:28888" ]; then
            echo "> $appIp    $targetIpPort   >>>>   icp富客户端, zk地址的key: icp.dubbo.register"
            continue
        fi
        if [ "$targetIpPort" = "172.16.18.114:28888" ] || [ "$targetIpPort" = "172.16.18.115:28888" ]; then
            echo "> $appIp    $targetIpPort   >>>>   泛化调用，写死的re的ip"
            continue
        fi

        echo "> $appIp    $targetIpPort"

        # ---
    done

done
