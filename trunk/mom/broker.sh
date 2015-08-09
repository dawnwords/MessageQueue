#!/usr/bin/env bash
export CLASSPATH=.:./target/lib/*:${CLASSPATH}
JAVA_OPT="-server -Xms4g -Xmx4g -Xmn512m -XX:PermSize=512m -XX:MaxPermSize=512m -XX:+DisableExplicitGC -XX:+UseParNewGC -XX:CMSFullGCsBeforeCompaction=0 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=85 -XX:SoftRefLRUPolicyMSPerMB=0 -cp ${CLASSPATH}"
java ${JAVA_OPT} com.alibaba.middleware.race.mom.Broker