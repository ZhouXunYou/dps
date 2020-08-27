# 启动脚本
spark-submit --class dps.mission.Launcher /home/spark/dps-mission.jar --missionName emmcCable --driver org.postgresql.Driver --ip 192.168.11.200 --port 5432 --user postgres --password postgres --dbType postgres --dbName dps

# 后台启动
nohup spark-submit --class dps.mission.Launcher /home/spark/dps-mission.jar --missionName emmcCable --driver org.postgresql.Driver --ip 192.168.11.200 --port 5432 --user postgres --password postgres --dbType postgres --dbName dps &