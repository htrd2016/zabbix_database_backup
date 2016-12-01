import MySQLdb   #yum install MySQL-python
import datetime
import time
import sys
import os
import fcntl
import logging

mysql_ip_src = sys.argv[1]
mysql_username_src = sys.argv[2]
mysql_pass_src = sys.argv[3]
mysql_port_src = int(sys.argv[4])
mysql_dbname_src = sys.argv[5]

mysql_ip_des = sys.argv[6]
mysql_username_des = sys.argv[7]
mysql_pass_des = sys.argv[8]
mysql_port_des = int(sys.argv[9])
mysql_dbname_des = sys.argv[10]
mysql_table_name_to_backup = sys.argv[11]

days_before_to_backup = int(sys.argv[12])
stop_file = sys.argv[13]

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=os.path.dirname(sys.argv[0])+"/"+mysql_table_name_to_backup+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'.log',
                filemode='w')


print mysql_ip_src,mysql_username_src,mysql_pass_src,mysql_port_src,mysql_dbname_src, mysql_ip_des,mysql_username_des,mysql_pass_des,mysql_port_des,mysql_dbname_des

def get_days_ago(d):
#    return int((time.mktime((datetime.datetime.now() - datetime.timedelta(days=d)).timetuple())))  
    return int((time.mktime((datetime.datetime.now() - datetime.timedelta(hours=d)).timetuple())))

def remove_stop_backingup_file():
    if(os.path.exists(stop_file) is True):
       os.remove(stop_file);

def is_stop_backingup():
    return os.path.exists(stop_file)

def get_max_clock_from_des(cur, conn, tablename):
    sql = 'select max(clock) from '+ tablename
    logging.debug(sql)

    count = cur.execute(sql)
    #print '['+tablename+']', 'row count =', count
    result = cur.fetchone()
    logging.debug("min click:"+str(result))

    if result[0] is None:
       result = 0
    else:
       result = result[0]

    return (int)(result);

def get_min_clock_from_src(cur, conn, tablename):
    sql = 'select min(clock) from '+ tablename
    logging.debug(sql)

    count = cur.execute(sql)
    #print '['+tablename+']', 'row count =', count
    result = cur.fetchone()
    logging.debug("min click:"+str(result))

    if result[0] is None:
       result = 0
    else:
       result = result[0]

    return (int)(result);


def get_min_clock_in_range_from_src_table(cur, conn, tablename, min, max):
    sql = 'select min(clock) from '+ tablename + " where clock > "+str(min)+" and clock<=" + str(max)  
    logging.debug(sql)

    count = cur.execute(sql)
    #print '['+tablename+']', 'row count =', count
    result = cur.fetchone()
    logging.debug("min clock:"+str(result))

    if result[0] is None:
       result = 0
    else:
       result = result[0]

    return (int)(result);


def backuptable(srccur, srcconn, descur, desconn, tablename, days):
    logging.debug("-----------start backup table "+tablename+" at"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"----------------")
    total_count = 0;
    time_to_backed_start = int(get_days_ago(days));
    logging.debug(str(days)+" ago:"+str(time_to_backed_start))
    min_clock = get_max_clock_from_des(descur, desconn, tablename)
    if(min_clock is 0):
       min_clock = get_min_clock_from_src(srccur, srcconn, tablename)-1;
       if(min_clock==-1):
           logging.debug("no data to back up")
           return 0;

    stop = is_stop_backingup();
    logging.debug("min clock:"+str(min_clock))
    logging.debug("stop:"+str(stop))

    if min_clock is 0 or stop is True:
       logging.debug("-----------end backup table "+tablename+" at"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"----------------")
       return 0;

    start_min_clock = int(min_clock)
    end_min_clock = int(start_min_clock) + 7200
    while is_stop_backingup() is False:
        if(start_min_clock>time_to_backed_start):
           break

        if (end_min_clock > time_to_backed_start):
            end_min_clock = time_to_backed_start
            total_count = total_count + backuptable_range(srccur, srcconn, descur, desconn, tablename, start_min_clock, time_to_backed_start)
            break

        ret_count = backuptable_range(srccur, srcconn, descur, desconn, tablename, start_min_clock, end_min_clock);
        total_count = total_count + ret_count
        if(ret_count == 0 and end_min_clock>=time_to_backed_start):
            break;

        if(ret_count == 0):
          min = get_min_clock_in_range_from_src_table(srccur, srcconn, tablename, end_min_clock, time_to_backed_start)
          if(min>start_min_clock):
              start_min_clock = min;
          else:
             break;         
 
        else:
          start_min_clock = end_min_clock

        end_min_clock = start_min_clock + 7200
    logging.debug(tablename+" total backup count:"+str(total_count))
    logging.debug("-----------end backup table "+tablename+" at"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"----------------")
    return total_count

def backuptable_range(srccur, srcconn, descur, desconn, tablename, start_timestamp, end_timestamp):
   sql = 'select * from '+ tablename +' where clock > %s and clock <= %s'

   logging.debug("time range:"+str(start_timestamp)+str(end_timestamp))

   logging.debug("----backuptable_range excute start-"+tablename+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"=-----")
   count = srccur.execute(sql, (start_timestamp, end_timestamp))
   logging.debug("----backuptable execute end-"+tablename+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"=-----")
   logging.debug('['+tablename+'] start:'+str( start_timestamp)+ "end:" + str(end_timestamp)+ ' row count='+str(count))

   logging.debug( 'select * from '+ tablename +' where clock > '+str(start_timestamp,)+' and clock <= '+str(end_timestamp));
   logging.debug("count="+str(count))
   if count<=0:
     return 0
   
   backed_count = 0;

   while is_stop_backingup() is False:
     logging.debug("--fech 10000 start:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"---")
     rows = srccur.fetchmany(10000)
     logging.debug("--fech 10000 end:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"---")
     if not rows:
        break

     try:
         logging.debug("--insert 10000 start:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"---")
         if(cmp(tablename, "history_text") == 0):
              sql = 'insert into '+ tablename +' values(%s, %s, %s, %s, %s) '
         elif(cmp(tablename, "history_log") == 0):
             sql = 'insert into '+ tablename +' values(%s, %s, %s, %s, %s, %s, %s, %s, %s) '
         else:
             sql = 'insert into '+ tablename +' values(%s, %s, %s, %s) '
         backed_count = backed_count + descur.executemany(sql, rows)
         desconn.commit()
         logging.debug("--insert 10000 end:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"---")

     except MySQLdb.Error,e:
       desconn.rollback()
       logging.debug(sql)
       logging.debug("Mysql Error %d: %s" % (e.args[0], e.args[1]))
       return backed_count;

   logging.debug( "===insert count="+str(backed_count) + "-queryed count="+str(count)+"===")
   if count!=backed_count:
       logging.debug("========================insert error!!!!!!!!!!!!!")   
   return backed_count;

def backup(table_name, timestamp): 
  try:
    print "start backup..."
    desconn=MySQLdb.connect(mysql_ip_des,mysql_username_des,mysql_pass_des,mysql_dbname_des,mysql_port_des)    
    srcconn=MySQLdb.connect(mysql_ip_src,mysql_username_src,mysql_pass_src,mysql_dbname_src,mysql_port_src)
    srccur=srcconn.cursor()
    descur=desconn.cursor()
    backuptable(srccur, srcconn, descur, desconn, table_name, timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_str', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_uint', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_text', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_log', timestamp)
    srccur.close()
    srcconn.close()
    descur.close()
    desconn.close()

    logging.debug("end backup...")
    return 0

  except MySQLdb.Error,e:
     logging.debug("Mysql Error %d: %s" % (e.args[0], e.args[1]))

  return -1
     
fh=0

def run_once():
    global fh
    path = os.path.realpath(__file__)+"_"+mysql_table_name_to_backup+"_lock"
    if (os.path.exists(path) is False):
       f=open(path,'w')
       f.write(mysql_table_name_to_backup);
       f.close()

    logging.debug(path)
    fh=open(path,'r')
    try:
        fcntl.flock(fh,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except:
        logging.debug("try to exit...")
        os._exit(0)


if __name__ == '__main__':
    run_once()
    remove_stop_backingup_file();
    backup(mysql_table_name_to_backup, days_before_to_backup)
