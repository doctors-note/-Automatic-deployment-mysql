# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import subprocess
import os
import tarfile
import random
import psutil
import shutil
import socket
from loguru import logger
from configparser import ConfigParser
from crontab import CronTab
from time import sleep
import pymysql
import re


def get_apt():
    parser = argparse.ArgumentParser(description='mysql安装脚本参数')
    group1 = parser.add_argument_group('必配参数','必配参数描述')
    group1.add_argument(
        '--myenv', action='store', metavar='[test|online]', dest='myenv', choices=['test', 'online'],
        required=True, help='指定数据库部署的环境')
    group1.add_argument(
        '--myuser', action='store', metavar='user', dest='myuser', required=True,
        help='指定一个业务账号')
    group1.add_argument(
        '--mypasswd', action='store', metavar='password', dest='mypasswd', required=True,
        help='指定一个业务密码')
    group1.add_argument(
        '--createdb', action='store', metavar='ip:port', dest='createdb', nargs='+', required=True,
        help='创建数据库 --createdb [10.0.0.1:3306 [10.0.0.1:3307]..]')
    group2 = parser.add_mutually_exclusive_group(required=True)
    group2.add_argument(
        '--ms-mode', action='store_true', dest='ms_mode', help='主从模式,第一个为主，其它为从 (ms-mode mgr-mode)二选一')
    group2.add_argument(
        '--mgr-mode', action='store_true', dest='mgr_mode', help='MGR模式')
    group3 = parser.add_argument_group('可选参数','可选参数描述')
    group3.add_argument(
        '--mgrid', action='store', metavar='uuid', dest='mgrid', default='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        help='指定MGR下的UUID，默认：aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
    group3.add_argument(
        '--perfix', action='store', metavar='dir', dest='perfix', default='/data/mysql',
        help='指定数据库安装路径，默认：/data/mysql')
    group3.add_argument(
        '--myversion', action='store', metavar='[5.7|8.0]', dest='myversion',
        choices=[5.7, 8.0], default=8.0, type=float, help='指定数据库版本，默认：8.0')
    group3.add_argument(
        '--myport', action='store', metavar='3306', dest='myport', default=3306, type=int,
        help='指定数据库端口，默认：3306')
    group3.add_argument(
        '--mybuffersize', action='store', metavar='int', dest='mybuffersize', type=int,
        help='指定innodb_buffer_pool_size的值，G为单位。默认：机器总内存的百分之70')
    group3.add_argument(
        '--uploadtype', action='store', metavar='[minio|local|aws|ali]', dest='uploadtype',
        choices=['minio', 'local', 'aws', 'ali'], default='local', help='上传备份类型，默认：local')
    group3.add_argument(
        '--skip-monitor', action='store_true', dest='skip_monitors',
        help='添加监控，默认添加')
    group3.add_argument(
        '--skip-backup', action='store_true', dest='skip_backups',
        help='添加备份，默认备份')
    opt = parser.parse_args()
    return opt

def get_host_ip():
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        local_ip = s.getsockname()[0]
        return local_ip
    finally:
        if s:
            s.close()

class Install_Mysql(object):
    def __init__(self, args):
        self.perfix = args.perfix
        self.myversion = args.myversion
        self.myport = args.myport
        self.myenv = args.myenv
        self.myuser = args.myuser
        self.mypasswd = args.mypasswd
        self.mybuffersize = args.mybuffersize
        self.mgrid = args.mgrid
        self.skip_monitors = args.skip_monitors
        self.skip_backups = args.skip_backups
        self.uploadtype = args.uploadtype
        self.createdb = args.createdb
        self.ms_mode = args.ms_mode
        self.mgr_mode = args.mgr_mode

        if self.mgr_mode:
            self.cnfdir = os.path.join(self.perfix, f"node{self.myport}")
            self.datadir = os.path.join(self.perfix, "data", f"node{self.myport}")
        else:
            self.cnfdir = os.path.join(self.perfix, f"db{self.myport}")
            self.datadir = os.path.join(self.perfix, "data", f"db{self.myport}")

    def minio_get(self):
        if not os.path.exists(self.datadir) and not os.path.exists(self.cnfdir):
            if not os.path.exists(f"{self.perfix}/bin"):
                if not os.path.exists(f'/root/rayman-mysql-{self.myversion}.tar.gz'):
                    if self.uploadtype == 'local':
                        logger.error(f"文件不存在 /root/rayman-mysql-{self.myversion}.tar.gz")
                        exit()
                    else:
                        logger.info(f"正在下载rayman-mysql-{self.myversion}.tar.gz")
                        from minio import Minio
                        minioClient = Minio('10.1.0.97:9199',
                                            access_key='getpkgkey',
                                            secret_key='RbDd78a8Z7A7w',
                                            secure=False,
                                            region='cn-shenzhen-01')
                        minioClient.fget_object('rayman-db-pkg',
                                                f'rayman-mysql-{self.myversion}.tar.gz',
                                                f'/root/rayman-mysql-{self.myversion}.tar.gz')
                self.extract(f'/root/rayman-mysql-{self.myversion}.tar.gz', self.perfix)
            os.makedirs(self.datadir)
            os.makedirs(self.cnfdir)
        else:
            logger.error(f"文件不为空{self.datadir} 或者 {self.cnfdir}，退出程序。。。。")
            exit()

    def extract(self, tarfilename, tarpath):
        logger.info(f"开始解压文件{tarfilename}到{tarpath}")
        try:
            tar = tarfile.open(tarfilename)
            tar.extractall(path = tarpath)
            tar.close()
        except Exception as e:
            logger.error(f"解压文件失败：{tarfilename} {e}")

    def check_port(self, host):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host.split(':')[0], int(host.split(':')[1])))
            return True
        except Exception:
            return False

    def sorted_hosts(self):
        hosts = []
        p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?):([0-9]{4,5})?$')
        for i in self.createdb:
            if p.match(i):
                if i.split(':')[0] == '127.0.0.1':
                    i = i.replace('127.0.0.1', ip)
                hosts.append(i)
            else:
                logger.error(f"非合法IP地址： {i}")
                exit()
        self.createdb = sorted(set(hosts), key=hosts.index)

    def inti_db(self):
        logger.info("修改my.cnf文件")
        cfg = ConfigParser(strict=False, allow_no_value=True)
        cfg.read(f'/root/mysql-pkg/my.cnf-{self.myversion}-default')
        cfg.set('client', 'port', str(self.myport))
        cfg.set('client', 'socket', f'{self.cnfdir}/mysql.sock')
        cfg.set('client', 'pid-file', f'{self.cnfdir}/mysql.pid')
        cfg.set('mysqld', 'port', str(self.myport))
        if self.myversion == 8.0:
            cfg.set('mysqld', 'mysqlx-port', f'{self.myport}0')
            cfg.set('mysqld', 'mysqlx_socket', f'{self.cnfdir}/mysqlx.sock')
        cfg.set('mysqld', 'server-id', str(random.randint(2,9999)))
        cfg.set('mysqld', 'datadir', self.datadir)
        cfg.set('mysqld', 'socket', f'{self.cnfdir}/mysql.sock')
        cfg.set('mysqld', 'pid-file', f'{self.cnfdir}/mysql.pid')
        cfg.set('mysqld', 'slow_query_log_file', f'{self.cnfdir}/mysql-slow.log')
        cfg.set('mysqld', 'log-error', f'{self.cnfdir}/mysql-error.log')
        cfg.set('mysqld', 'innodb_undo_directory', self.datadir)
        cfg.set('mysqld', 'datadir', self.datadir)
        cfg.set('mysqld', 'innodb_data_home_dir', self.datadir)
        cfg.set('mysqld', 'innodb_log_group_home_dir', self.datadir)
        if not self.mybuffersize:
            buffsize = int(psutil.virtual_memory().total / 1024 / 1024 / 1024 * 0.7)
            self.mybuffersize = buffsize
        cfg.set('mysqld', 'innodb_buffer_pool_size', f'{self.mybuffersize}G')
        cfg.set('mysqld', 'innodb_write_io_threads', str(psutil.cpu_count() * 2))
        cfg.set('mysqld', 'innodb_read_io_threads', str(psutil.cpu_count() * 2))

        if self.mybuffersize < 5:
            cfg.set('mysqld', 'read_buffer_size', '2M')
            cfg.set('mysqld', 'read_rnd_buffer_size', '1M')
            cfg.set('mysqld', 'sort_buffer_size', '1M')
            cfg.set('mysqld', 'join_buffer_size', '1M')
            cfg.set('mysqld', 'binlog_cache_size', '2M')
        if self.myenv == 'online':
            cfg.set('mysqld', 'innodb_io_capacity', '3000')
            cfg.set('mysqld', 'expire_logs_days', '7')
            cfg.set('mysqld', 'innodb_flush_log_at_trx_commit', '1')
            cfg.set('mysqld', 'sync_binlog', '1')
        else:
            cfg.set('mysqld', 'innodb_io_capacity', '1000')
            cfg.set('mysqld', 'expire_logs_days', '3')
            cfg.set('mysqld', 'innodb_flush_log_at_trx_commit', '2')
            cfg.set('mysqld', 'sync_binlog', '0')
        if self.mgr_mode:
            cfg.set('mysqld', 'plugin-load', 'audit_log=audit_log.so;group_replication=group_replication.so')
        else:
            cfg.set('mysqld', 'plugin-load', 'audit_log=audit_log.so')
        if self.mgr_mode:
            cfg.set('mysqld', 'binlog_checksum', 'NONE')
            cfg.set('mysqld', 'transaction_write_set_extraction', 'XXHASH64')
            cfg.set('mysqld', 'loose-group_replication_group_name', self.mgrid)
            cfg.set('mysqld', 'loose-group_replication_start_on_boot', 'OFF')
            cfg.set('mysqld', 'loose-group_replication_local_address', f'{ip}:{self.myport}1')
            group_seeds = ','.join([i + '1' for i in self.createdb])
            cfg.set('mysqld', 'loose-group_replication_group_seeds', group_seeds)
            cfg.set('mysqld', 'loose-group_replication_bootstrap_group', 'OFF')
            cfg.set('mysqld', 'report_host', ip)
            cfg.set('mysqld', 'report_port', str(self.myport))
        with open(f'{self.cnfdir}/my.cnf', 'w+') as fw:
            cfg.write(fw)
        with open(f'{self.cnfdir}/my.cnf', 'a+') as fa:
            fa.write('performance_schema = 1\n')
            fa.write('performance_schema_instrument = "%memory%=on"\n')
            fa.write('performance_schema_instrument = "%lock%=on"\n')
            fa.write('innodb_monitor_enable = "module_innodb"\n')
            fa.write('innodb_monitor_enable = "module_server"\n')
            fa.write('innodb_monitor_enable = "module_dml"\n')
            fa.write('innodb_monitor_enable = "module_ddl"\n')
            fa.write('innodb_monitor_enable = "module_trx"\n')
            fa.write('innodb_monitor_enable = "module_os"\n')
            fa.write('innodb_monitor_enable = "module_purge"\n')
            fa.write('innodb_monitor_enable = "module_log"\n')
            fa.write('innodb_monitor_enable = "module_lock"\n')
            fa.write('innodb_monitor_enable = "module_buffer"\n')
            fa.write('innodb_monitor_enable = "module_index"\n')
            fa.write('innodb_monitor_enable = "module_ibuf_system"\n')
            fa.write('innodb_monitor_enable = "module_buffer_page"\n')
            fa.write('innodb_monitor_enable = "module_adaptive_hash"\n')

        if self.myversion == 8.0:
            shutil.move('/lib64/libstdc++.so.6', '/lib64/libstdc++.so.6.old')
            shutil.copy('/root/mysql-pkg/libstdc++.so.6', '/lib64/libstdc++.so.6')
            shutil.copy('/root/mysql-pkg/audit_log-8.0.so', f'{self.perfix}/lib/plugin/audit_log.so')
        else:
            shutil.copy('/root/mysql-pkg/audit_log-5.7.so', f'{self.perfix}/lib/plugin/audit_log.so')
        if self.myenv == 'test':
            shutil.copy('/root/mysql-pkg/mysqld_service.sh', f'{self.perfix}/mysqld_service.sh')
        if not os.path.exists("/usr/bin/mysql"):
            os.symlink(f"{self.perfix}/bin/mysql", "/usr/bin/mysql")
        cfgw = ConfigParser(strict=False, allow_no_value=True)
        cfgw['mysql'] = {'prompt':'\\u@\\h [\\d]>', 'no-auto-rehash':None}
        with open('/etc/my.cnf', 'w') as fw:
            cfgw.write(fw)
        shutil.copy('/root/mysql-pkg/mysql_pyslow.py', self.perfix)
        logger.info("创建用户和文件授权")
        subprocess.call("useradd -M -s /sbin/nologin mysql", shell=True)
        subprocess.call("chown -R mysql.mysql {0}".format(self.perfix), shell=True)
        logger.info("初始化数据库")
        subprocess.call("{0}/bin/mysqld --defaults-file={1}/my.cnf --initialize-insecure --user=mysql"
                        .format(self.perfix, self.cnfdir), shell=True)
        with open(f'{self.cnfdir}/my.cnf', 'a+') as fa2:
            fa2.write('audit_log_format = JSON\n')
            fa2.write('audit_log_rotate_on_size = 1073741824\n')
            if self.myversion == 8.0:
                fa2.write("audit_log_exclude_accounts = 'monitoruser@localhost'\n")
            if self.myenv == 'online':
                fa2.write('audit_log_rotations = 7\n')
            else:
                fa2.write('audit_log_rotations = 3\n')
            if self.mgr_mode and self.myversion == 8.0:
                fa2.write('group_replication_recovery_get_public_key = ON\n')

        logger.info("启动数据库")
        subprocess.call("/usr/bin/numactl --interleave=all {0}/bin/mysqld_safe --defaults-file={1}/my.cnf \
                        --user=mysql --daemonize".format(self.perfix, self.cnfdir), shell=True)

    def init_mode(self):
        myconn = pymysql.connect(host='127.0.0.1', port=self.myport, user='root', password='', cursorclass = pymysql.cursors.DictCursor)
        cursor = myconn.cursor()
        cursor.execute("SET SQL_LOG_BIN = 0")
        cursor.execute("CREATE USER 'repluser'@'%' IDENTIFIED BY '4cmOlKRkX1H7k'")
        cursor.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repluser'@'%'")
        user_sql = ("GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY "
                    "TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, "
                    f"TRIGGER  ON *.* TO '{self.myuser}'@'%'")
        cursor.execute(f"CREATE USER '{self.myuser}'@'%' IDENTIFIED WITH mysql_native_password BY '{self.mypasswd}'")
        cursor.execute(user_sql)
        if self.ms_mode:
            if not f"{ip}:{self.myport}" == self.createdb[0]:
                logger.info("构建MS复制模式")
                master_host, master_port = self.createdb[0].split(':')
                ms_sql = (f"CHANGE MASTER TO MASTER_HOST = '{master_host}', MASTER_PORT = {master_port}, MASTER_USER = "
                            "'repluser', MASTER_PASSWORD = '4cmOlKRkX1H7k', MASTER_AUTO_POSITION = 1")
                if self.myversion == 8.0:
                    cursor.execute(f"{ms_sql}, GET_MASTER_PUBLIC_KEY = 1")
                else:
                    cursor.execute(ms_sql)
                cursor.execute("START SLAVE")
                logger.info("构建MS复制模式成功")
        else:
            logger.info("构建MGR复制模式")
            mgr_sql = ("CHANGE MASTER TO MASTER_USER = 'repluser', MASTER_PASSWORD = '4cmOlKRkX1H7k' FOR "
                        "CHANNEL 'group_replication_recovery'")
            cursor.execute(mgr_sql)
            if f"{ip}:{self.myport}" == self.createdb[0]:
                cursor.execute("SET GLOBAL group_replication_bootstrap_group = ON")
                cursor.execute("START GROUP_REPLICATION")
                cursor.execute("SET GLOBAL group_replication_bootstrap_group = OFF")
            else:
                cursor.execute("START GROUP_REPLICATION")
            for _ in range(10):
                cursor.execute("SHOW GLOBAL STATUS LIKE 'group_replication_primary_member'")
                master_memberid = cursor.fetchone()['Value']
                if master_memberid != 'UNDEFINED':
                    logger.debug(f"主节点ID：{master_memberid}")
                    logger.info("构建MGR复制模式成功")
                    break
                else:
                    sleep(3)
        cursor.close()
        myconn.close()

    def import_user(self):
        logger.info("添加账号")
        subprocess.call(f"{self.perfix}/bin/mysql -h127.0.0.1 -uroot -P {self.myport} mysql < \
                        /root/mysql-pkg/user-mysql{self.myversion}.sql", shell=True)

    def init_backup(self):
        logger.info("安装xtrabackup，对数据库进行备份")
        subprocess.call("yum -y localinstall /root/mysql-pkg/percona-toolkit-3.2.rpm", shell=True)
        shutil.copy('/root/mysql-pkg/qpress', '/usr/bin')
        if self.myversion == 8.0:
            subprocess.call("yum -y localinstall /root/mysql-pkg/percona-xtrabackup-80.rpm", shell=True)
        else:
            subprocess.call("yum -y localinstall /root/mysql-pkg/percona-xtrabackup-24.rpm", shell=True)

        shutil.copy('/root/mysql-pkg/backup.py', f'{self.perfix}/backup.py')
        if self.uploadtype == 'minio':
            shutil.copy('/root/mysql-pkg/upload-minio.py', '/usr/games/upload.py')
        elif self.uploadtype == 'ali':
            shutil.copy('/root/mysql-pkg/upload-ali.py', '/usr/games/upload.py')
        elif self.uploadtype == 'aws':
            subprocess.call("pip3 install boto3", shell=True)
            shutil.copy('/root/mysql-pkg/upload-aws.py', '/usr/games/upload.py')
        subprocess.call("chmod +x /usr/games/upload.py", shell=True)
        if self.uploadtype == 'local':
            clearday = 5
            upcycle = 0
        else:
            clearday = 3
            upcycle = 1
        my_cron = CronTab(user='root')
        iter = list(my_cron.find_command(re.compile(f"{self.cnfdir}/my.cnf")))
        if not iter:
            command = (f"/usr/bin/python3 {self.perfix}/backup.py -f {self.cnfdir}/my.cnf --clearday {clearday} "
                       f"--upcycle {upcycle}")
            job = my_cron.new(command=command)
            job.set_comment("mysql数据备份")
            job.setall('00 03 * * *')
            my_cron.write()

    def init_monitor(self):
        logger.info("添加mysql监控")
        cfg2 = ConfigParser()
        if not os.path.exists('/etc/supervisord.d'):
            os.makedirs('/etc/supervisord.d')
        if not os.path.exists('/etc/supervisord.d/supervisord.conf'):
            cfg2.read('/root/mysql-pkg/supervisord.conf')
        else:
            cfg2.read('/root/mysql-pkg/supervisord.conf')
        cfg2[f'program:mysqld_export{self.myport}'] = {}
        cfg2.set(f'program:mysqld_export{self.myport}', 'command',
                 f'/usr/bin/mysqld_exporter --config.my-cnf={self.cnfdir}/.my.cnf')
        cfg2.set(f'program:mysqld_export{self.myport}', 'autostart', 'true')
        cfg2.set(f'program:mysqld_export{self.myport}', 'startsecs', '10')
        cfg2.set(f'program:mysqld_export{self.myport}', 'autorestart', 'true')
        cfg2.set(f'program:mysqld_export{self.myport}', 'startretries', '3')
        cfg2.set(f'program:mysqld_export{self.myport}', 'user', 'mysql')
        cfg2.set(f'program:mysqld_export{self.myport}', 'priority', '999')
        cfg2.set(f'program:mysqld_export{self.myport}', 'redirect_stderr', 'true')
        cfg2.set(f'program:mysqld_export{self.myport}', 'stdout_logfile_maxbytes', '20MB')
        cfg2.set(f'program:mysqld_export{self.myport}', 'stdout_logfile_backups', '20')
        cfg2.set(f'program:mysqld_export{self.myport}', 'stdout_logfile', f'{self.cnfdir}/monitor.log')
        cfg2.set(f'program:mysqld_export{self.myport}', 'stopasgroup', 'false')
        cfg2.set(f'program:mysqld_export{self.myport}', 'killasgroup', 'false')
        with open('/etc/supervisord.d/supervisord.conf', 'w+') as fw2:
            cfg2.write(fw2)
        logger.info("安装mysqld_exporter")
        if not os.path.exists('/usr/bin/mysqld_exporter'):
            shutil.copy('/root/mysql-pkg/mysqld_exporter', '/usr/bin')
        cfg3 = ConfigParser()
        monitor_dict = {'client':{'user':'monitoruser','password':'oA4Pl5Usgr1sc','port':self.myport}}
        cfg3.read_dict(monitor_dict)
        with open(f'{self.cnfdir}/.my.cnf', 'w+') as fw3:
            cfg3.write(fw3)
        if os.path.exists('/etc/supervisord.d/supervisord.pid'):
            subprocess.call("/usr/local/bin/supervisorctl -c /etc/supervisord.d/supervisord.conf reload", shell=True)
        else:
            subprocess.call("/usr/local/bin/supervisord -c /etc/supervisord.d/supervisord.conf", shell=True)

    def main(self):
        self.sorted_hosts()
        logger.debug(f"主机列表：{self.createdb}")
        if f"{ip}:{self.myport}" in self.createdb:
            self.minio_get()
            self.inti_db()
            if f"{ip}:{self.myport}" != self.createdb[0]:
                for _ in range(30):
                    if self.ms_mode and self.check_port(self.createdb[0]):
                        self.init_mode()
                        break
                    elif self.mgr_mode and self.check_port(f"{self.createdb[0]}1"):
                        self.init_mode()
                        break
                    else:
                        sleep(3)
            else:
                self.init_mode()
                self.import_user()
            if not self.skip_backups:
                self.init_backup()
            if not self.skip_monitors:
                self.init_monitor()
            logger.info("清空日志")
            with open(f'{self.datadir}/audit.log', 'w'):
                pass
        else:
            logger.error("主机不存在")


if __name__ == '__main__':
    logger.add(f'/tmp/install_mysql.log',level='DEBUG',format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}')
    args = get_apt()
    ip = get_host_ip()
    install = Install_Mysql(args)
    install.main()
