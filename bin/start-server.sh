bin=`cd "$( dirname "$0" )"; pwd`
echo $bin
java -cp $bin/../target/OSEProxy-0.0.1-SNAPSHOT-jar-with-dependencies.jar ucb.oseproxy.rpc.ProxyServer 
