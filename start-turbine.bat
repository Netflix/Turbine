echo off
cd %~dp0
java -cp .\turbine-ext\turbine-discovery-consul\build\libs\turbine-discovery-consul-executable-2.0.0-DP.3-SNAPSHOT.jar com.netflix.turbine.discovery.consul.StartConsulTurbine %*