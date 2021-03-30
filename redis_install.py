# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import subprocess
import os
import tarfile
import psutil
import shutil
from loguru import logger
from configparser import ConfigParser
from crontab import CronTab
import socket
from time import sleep
import redis
import re

def get_apt():
    parser = argparse.ArgumentParser(description='redis安装脚本参数')
    group1 = parser.add_argument_group('必配参数','必配参数描述')
    group1.add_argument(
        '--redisenv', action='store', metavar='[test|online]', dest='redisenv', choices=['test', 'online'],
        required=True, help='指定数据库部署的环境')
    group1.add_argument(
        '--redispasswd', action='store', metavar='password', dest='redispasswd', required=True,
        help='指定一个业务密码')
    group2 = parser.add_mutually_exclusive_group(required=True)
    group2.add_argument(
        '--sentinel-mode', action='store_true', dest='sentinel_mode', help='哨兵模式 (sentinel、db、cluster)三选一')
    group2.add_argument(
        '--cluster-mode', action='store_true', dest='cluster_mode', help='集群模式')
    group2.add_argument(
        '--db-mode', action='store_true', dest='db_mode', help='单机模式')
    group1.add_argument(
        '--createdb', action='store', dest='createdb', metavar='ip:port', nargs='+', required=True,
        help='创建数据库 --createdb [10.0.0.1:6379 [10.0.0.2:6379]..]')
    group4 = parser.add_argument_group('可选参数', '可选参数描述')
    group4.add_argument(
        '--perfix', action='store', metavar='dir', dest='perfix', default='/data/redis',
        help='指定数据库安装路径，默认：/data/redis')
    group4.add_argument(
        '--redisversion', action='store', metavar='[5.0|6.0]', dest='redisversion',
        choices=[5.0, 6.0], default=5.0, type=float, help='指定数据库版本，默认：5.0')
    group4.add_argument(
        '--redisport', action='store', metavar='int', dest='redisport', default=6379, type=int,
        help='指定数据库端口，默认：6379')
    group4.add_argument(
        '--redismaxmemory', action='store', metavar='int(G|MB)', dest='redismaxmemory',
        help='指定maxmemory的值，2gb或512MB，默认：机器总内存的百分之70')
    group4.add_argument(
        '--replicas', action='store', metavar='int', dest='replicas', default=1, type=int,
        help='主节点所需要的从节点数，默认：1')
    group4.add_argument(
        '--mastername', action='store', metavar='name', dest='mastername', default='lm-redis',
        help='节点名称， 默认：lm-redis')
    group4.add_argument(
        '--quorum', action='store', metavar='quorum', dest='quorum', default=1, type=int,
        help='达到quorum数量则判断master下线， 默认：1')
    group4.add_argument(
        '--uploadtype', action='store', metavar='[minio|local|aws|ali]', dest='uploadtype',
        choices=['minio', 'local', 'aws', 'ali'], default='local', help='上传备份类型，默认：local')
    group4.add_argument(
        '--skip-monitor', action='store_true', dest='skip_monitors',
        help='添加监控，默认添加')
    group4.add_argument(
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

class Install_Redis(object):
    def __init__(self, args):
        self.perfix = args.perfix
        self.redisversion = args.redisversion
        self.redisport = args.redisport
        self.redisenv = args.redisenv
        self.redispasswd = args.redispasswd
        self.redismaxmemory = args.redismaxmemory
        self.skip_monitors = args.skip_monitors
        self.skip_backups = args.skip_backups
        self.uploadtype = args.uploadtype
        self.sentinel_mode = args.sentinel_mode
        self.cluster_mode = args.cluster_mode
        self.db_mode = args.db_mode
        self.createdb = args.createdb
        self.replicas = args.replicas
        self.mastername = args.mastername
        self.quorum = args.quorum

        if self.cluster_mode:
            self.cnfdir = os.path.join(self.perfix, f"node{self.redisport}")
            self.datadir = os.path.join(self.perfix, "data", f"node{self.redisport}")
        elif self.sentinel_mode:
            self.cnfdir = os.path.join(self.perfix, f"sentinel{self.redisport}")
            self.datadir = os.path.join(self.perfix, "data", f"sentinel{self.redisport}")
        else:
            self.cnfdir = os.path.join(self.perfix, f"db{self.redisport}")
            self.datadir = os.path.join(self.perfix, "data", f"db{self.redisport}")

    def minio_get(self):
        if not os.path.exists(self.datadir) and not os.path.exists(self.cnfdir):
            if not os.path.exists(self.perfix):
                if not os.path.exists(f'/root/***-redis-{self.redisversion}.tar.gz'):
                    if self.uploadtype == 'local':
                        logger.error(f"文件不存在 /root/***-redis-{self.redisversion}.tar.gz")
                        exit()
                    else:
                        logger.info(f"正在下载***-redis-{self.redisversion}.tar.gz")
                        from minio import Minio
                        minioClient = Minio('******:9199',
                                        access_key='***',
                                        secret_key='***',
                                        secure=False,
                                        region='cn-shenzhen-01')
                        minioClient.fget_object('***-db-pkg',
                                        f'***-redis-{self.redisversion}.tar.gz',
                                        f'/root/***-redis-{self.redisversion}.tar.gz')
                self.extract(f'/root/***-redis-{self.redisversion}.tar.gz', self.perfix)
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

    def remove_fristline(self, filename):
        logger.info("整理配置文件")
        with open(filename, 'r') as fr:
            file_out = fr.readlines()
        with open(filename, 'w') as fw:
            file_in = ''.join(file_out[1:])
            fw.write(file_in)

    def init_db(self):
        logger.info("初始化redis")
        cfg = ConfigParser(delimiters=(' ',))
        with open("/root/redis-pkg/redis.conf-default") as stream:
            cfg.read_string("[redis]" + stream.read())
        cfg.set('redis', 'port', str(self.redisport))
        cfg.set('redis', 'pidfile', f'{self.cnfdir}/redis.pid')
        cfg.set('redis', 'logfile', f'{self.cnfdir}/redis.log')
        cfg.set('redis', 'dir', self.datadir)
        cfg.set('redis', 'requirepass', self.redispasswd)
        cfg.set('redis', 'masterauth', self.redispasswd)
        if self.redisenv == 'online':
            cfg.set('redis', 'slowlog-max-len', '1024')
        else:
            cfg.set('redis', 'slowlog-max-len', '128')
        cfg.set('redis', 'appendonly', 'yes')
        if self.redismaxmemory:
            cfg.set('redis', 'maxmemory', self.redismaxmemory)
        else:
            cfg.set('redis', 'maxmemory', f'{cachesize}gb')
        with open(f'{self.cnfdir}/redis.conf', 'w') as fcr:
            cfg.write(fcr, space_around_delimiters=False)
        self.remove_fristline(f'{self.cnfdir}/redis.conf')
        with open(f'{self.cnfdir}/redis.conf', 'a+') as fw:
            fw.write("client-output-buffer-limit normal 0 0 0\n")
            fw.write("client-output-buffer-limit replica 256mb 64mb 60\n")
            fw.write("client-output-buffer-limit pubsub 32mb 8mb 60\n")

    def inti_sentinel(self, hosts):
        logger.info("初始化redis哨兵")
        logger.debug(f"master:['{hosts[0]}'] slave:{hosts[1:self.replicas + 1:]} sentinel:{hosts[self.replicas + 1::]}")
        if f"{ip}:{self.redisport}" in hosts[0]:
            self.init_db()
        elif f"{ip}:{self.redisport}" in hosts[1:self.replicas + 1:]:
            for x in hosts[1:self.replicas + 1:]:
                self.init_db()
                with open(f'{self.cnfdir}/redis.conf', 'a+') as fw:
                    fw.write(f"slaveof {hosts[0].split(':')[0]} {hosts[0].split(':')[1]}")
        elif f"{ip}:{self.redisport}" in hosts[self.replicas + 1::]:
            master_host = hosts[0].split(":")[0]
            master_port = hosts[0].split(":")[1]
            with open(f'{self.cnfdir}/sentinel.conf', 'w') as fcw:
                fcw.write(f"port {self.redisport}\n")
                fcw.write(f"daemonize yes\n")
                fcw.write(f"pidfile {self.cnfdir}/sentinel.pid\n")
                fcw.write(f"logfile {self.cnfdir}/sentinel.log\n")
                fcw.write(f"dir {self.datadir}\n")
                fcw.write(f"sentinel monitor {self.mastername} {master_host} {master_port} {self.quorum}\n")
                fcw.write(f"sentinel auth-pass {self.mastername} {self.redispasswd}\n")
                fcw.write(f"sentinel down-after-milliseconds {self.mastername} 30000\n")
                fcw.write(f"sentinel parallel-syncs {self.mastername} 1\n")
                fcw.write(f"sentinel failover-timeout {self.mastername} 180000\n")
                fcw.write("sentinel deny-scripts-reconfig yes\n")
                fcw.write(f"requirepass {self.redispasswd}\n")

    def init_cluster(self, hosts):
        logger.info("初始化redis集群")
        logger.debug(f"集群主机:{hosts}")
        self.init_db()
        with open(f'{self.cnfdir}/redis.conf', 'a+') as fw:
            fw.write("cluster-enabled yes\n")
            fw.write(f"cluster-config-file nodes-{self.redisport}.conf\n")

    def create_cluster(self, hosts):
        logger.info("配置redis集群信息")
        if self.cluster_mode and f"{ip}:{self.redisport}" == hosts[-1]:
            for _ in range(60):
                if not False in [ self.check_port(i) for i in hosts ]:
                    nodes = ' '.join(hosts)
                    subprocess.call(f"echo 'yes' | {self.perfix}/bin/redis-cli -a {self.redispasswd} --cluster create {nodes} \
                                --cluster-replicas {self.replicas}", shell=True)
                    break
                else:
                    sleep(3)

    def deploy_env(self):
        global cachesize
        logger.info(f"搭建redis [{self.redisport}]")
        cachesize = int(psutil.virtual_memory().total / 1024 / 1024 / 1024 * 0.7)
        if self.db_mode and f"{ip}:{self.redisport}" in self.createdb:
            self.init_db()
        elif self.sentinel_mode and f"{ip}:{self.redisport}" in self.createdb:
            if len(self.createdb) - self.replicas >= 0:
                self.inti_sentinel(self.createdb)
            else:
                logger.error("缺少主机，无法满足设置的replicas的数量")
        elif self.cluster_mode and f"{ip}:{self.redisport}" in self.createdb:
            self.init_cluster(self.createdb)
        else:
            logger.error("主机不存在")

        logger.info("添加用户redis")
        subprocess.call("useradd -M -s /sbin/nologin redis", shell=True)
        subprocess.call("chown -R redis.redis {0}".format(self.perfix), shell=True)
        logger.info("启动数据库")
        if os.path.exists(f"{self.cnfdir}/sentinel.conf"):
            subprocess.call("sudo -u redis /usr/bin/numactl --interleave=all {0}/bin/redis-sentinel \
                                    {1}/sentinel.conf".format(self.perfix, self.cnfdir), shell=True)
        else:
            subprocess.call("sudo -u redis /usr/bin/numactl --interleave=all {0}/bin/redis-server \
                                    {1}/redis.conf".format(self.perfix, self.cnfdir), shell=True)
        if self.cluster_mode and f"{ip}:{self.redisport}" in self.createdb:
            self.create_cluster(self.createdb)
        if not os.path.exists("/usr/bin/redis-cli"):
            os.symlink(f"{self.perfix}/bin/redis-cli", "/usr/bin/redis-cli")

    def init_backup(self):
        if not self.skip_backups and not os.path.exists(f"{self.cnfdir}/sentinel.conf"):
            logger.info("对数据库进行备份")
            shutil.copy('/root/redis-pkg/backup.py', f'{self.perfix}/backup.py')
            if self.uploadtype == 'minio':
                shutil.copy('/root/redis-pkg/upload-minio.py', '/usr/games/upload.py')
            elif self.uploadtype == 'ali':
                shutil.copy('/root/redis-pkg/upload-ali.py', '/usr/games/upload.py')
            elif self.uploadtype == 'aws':
                subprocess.call("pip3 install boto3", shell=True)
                shutil.copy('/root/redis-pkg/upload-aws.py', '/usr/games/upload.py')
            subprocess.call("chmod +x /usr/games/upload.py", shell=True)
            if self.uploadtype == 'local':
                clearday = 5
                upcycle = 0
            else:
                clearday = 3
                upcycle = 1
            my_cron = CronTab(user='root')
            iter = list(my_cron.find_command(re.compile(f"{self.cnfdir}/redis.conf")))
            if not iter:
                command = (f"/usr/bin/python3 {self.perfix}/backup.py -f {self.cnfdir}/redis.conf "
                           f"--clearday {clearday} --upcycle {upcycle}")
                job = my_cron.new(command=command)
                job.set_comment("redis数据备份")
                job.setall('30 03 * * *')
                my_cron.write()

    def init_monitor(self):
        if not os.path.exists('/usr/bin/redis_exporter'):
            shutil.copy('/root/redis-pkg/redis_exporter', '/usr/bin')
        if not self.skip_monitors and not os.path.exists(f"{self.cnfdir}/sentinel.conf"):
            logger.info("安装redis_exporter")
            cfg2 = ConfigParser()
            if not os.path.exists('/etc/supervisord.d'):
                os.makedirs('/etc/supervisord.d')
            if not os.path.exists('/etc/supervisord.d/supervisord.conf'):
                cfg2.read('/root/redis-pkg/supervisord.conf')
            else:
                cfg2.read('/etc/supervisord.d/supervisord.conf')
            cfg2[f'program:redis_export{self.redisport}'] = {}
            cfg2.set(f'program:redis_export{self.redisport}', 'command',
                     f'/usr/bin/redis_exporter -redis.addr 127.0.0.1:{self.redisport} -redis.password {self.redispasswd}')
            cfg2.set(f'program:redis_export{self.redisport}', 'autostart', 'true')
            cfg2.set(f'program:redis_export{self.redisport}', 'startsecs', '10')
            cfg2.set(f'program:redis_export{self.redisport}', 'autorestart', 'true')
            cfg2.set(f'program:redis_export{self.redisport}', 'startretries', '3')
            cfg2.set(f'program:redis_export{self.redisport}', 'user', 'redis')
            cfg2.set(f'program:redis_export{self.redisport}', 'priority', '999')
            cfg2.set(f'program:redis_export{self.redisport}', 'redirect_stderr', 'true')
            cfg2.set(f'program:redis_export{self.redisport}', 'stdout_logfile_maxbytes', '20MB')
            cfg2.set(f'program:redis_export{self.redisport}', 'stdout_logfile_backups', '20')
            cfg2.set(f'program:redis_export{self.redisport}', 'stdout_logfile', f'{self.cnfdir}/monitor.log')
            cfg2.set(f'program:redis_export{self.redisport}', 'stopasgroup', 'false')
            cfg2.set(f'program:redis_export{self.redisport}', 'killasgroup', 'false')
            with open('/etc/supervisord.d/supervisord.conf', 'w+') as fw2:
                cfg2.write(fw2)
            if os.path.exists('/etc/supervisord.d/supervisord.pid'):
                subprocess.call("/usr/local/bin/supervisorctl -c /etc/supervisord.d/supervisord.conf reload", shell=True)
            else:
                subprocess.call("/usr/local/bin/supervisord -c /etc/supervisord.d/supervisord.conf", shell=True)

    def main(self):
        self.sorted_hosts()
        logger.debug(f"主机列表：{self.createdb}")
        if f"{ip}:{self.redisport}" in self.createdb:
            self.minio_get()
            self.deploy_env()
            self.init_backup()
            self.init_monitor()
        else:
            logger.error("找不到主机")

if __name__ == '__main__':
    logger.add(f'/tmp/install_redis.log', level='DEBUG', format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}')
    args = get_apt()
    ip = get_host_ip()
    install = Install_Redis(args)
    install.main()
