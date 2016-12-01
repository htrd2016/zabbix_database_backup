nohup python /root/htrd/zabbix_database_backup/historybackup.py 192.168.103.112 zabbix zabbix 3306 zabbix 127.0.0.1 htrd htrd 3306 htrd_bak history 2 /root/htrd/this_file_exist_stop_backup_history > /dev/null 2>log &

nohup python /root/htrd/zabbix_database_backup/historybackup.py 192.168.103.112 zabbix zabbix 3306 zabbix 127.0.0.1 htrd htrd 3306 htrd_bak history_uint 2 /root/htrd/this_file_exist_stop_backup_history_uint > /dev/null 2>log &

nohup python /root/htrd/zabbix_database_backup/historybackup.py 192.168.103.112 zabbix zabbix 3306 zabbix 127.0.0.1 htrd htrd 3306 htrd_bak history_text 2 /root/htrd/this_file_exist_stop_backup_history_text > /dev/null 2>log &

nohup python /root/htrd/zabbix_database_backup/historybackup.py 192.168.103.112 zabbix zabbix 3306 zabbix 127.0.0.1 htrd htrd 3306 htrd_bak history_str 2 /root/htrd/this_file_exist_stop_backup_history_str > /dev/null 2>log &

nohup python /root/htrd/zabbix_database_backup/historybackup.py 192.168.103.112 zabbix zabbix 3306 zabbix 127.0.0.1 htrd htrd 3306 htrd_bak history_log 2 /root/htrd/this_file_exist_stop_backup_history_log > /dev/null 2>log &
