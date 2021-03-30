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
import yaml
import socket
import time
import re

def get_apt():
    parser = argparse.ArgumentParser(description='mongodb安装脚本参数')
    group1 = parser.add_argument_group('必配参数','必配参数描述')
    group1.add_argument(
        '--mongoenv', action='store', metavar='[test|online]', dest='mongoenv', choices=['test', 'online'],
        required=True, help='指定数据库部署的环境')
    group3 = parser.add_mutually_exclusive_group(required=True)
    group3.add_argument(
        '--replication-mode', action='store_true', dest='replication_mode',
        help='指定数据库集群类型，单机选择replication (replication、cluster)')
    group3.add_argument(
        '--cluster-mode', action='store_true', dest='cluster_mode',
        help='指定数据库类型为集群, cluster')
    group1.add_argument(
        '--mongoreplname', action='store', metavar='name', dest='mongoreplname', required=True,
        help='指定副本集的名称, 在名称设置为****，当集群为cluster时，\
                会生成****-shard0{0}，****-configs。非cluster，则为设置的****')
    group1.add_argument(
        '--createdb', action='store', metavar='ip:host', dest='createdb', required=True, nargs='+',
        help='指定数据库集群的地址端口，集群: --createdb [10.0.0.1:27017 [10.0.0.2:27017]..] [主 从..[主 从..]..]')
    group1.add_argument(
        '--nodegroups', action='store', metavar='int', dest='nodegroups', default=2, type=int,
        help='集群中分片的数量, 建议为2+n')
    group1.add_argument(
        '--replicas', action='store', metavar='int', dest='replicas', default=3, type=int,
        help='集群中单个分片里机器的数量，建议为2n+1')
    group1.add_argument(
        '--mongoss', action='store', metavar='int', dest='mongoss', default=2, type=int,
        help='集群中路由的数量，建议为2+n')
    group1.add_argument(
        '--configs', action='store', metavar='int', dest='configs', default=3, type=int,
        help='集群中配置服的数量，建议为2n+1')
    group2 = parser.add_argument_group('可选参数','可选参数描述')
    group2.add_argument(
        '--perfix', action='store', metavar='dir', dest='perfix', default='/data/mongo',
        help='指定数据库安装路径，默认：/data/mongo')
    group2.add_argument(
        '--uploadtype', action='store', metavar='[minio|local|aws|ali]', dest='uploadtype',
        choices=['minio', 'local', 'aws', 'ali'], default='local', help='上传备份类型，默认：local')
    group2.add_argument(
        '--mongoversion', action='store', metavar='[4.2]', dest='mongoversion',
        choices=[4.2], default=4.2, type=float, help='指定数据库版本，默认：4.2')
    group2.add_argument(
        '--mongoport', action='store', metavar='int', dest='mongoport', default=27017, type=int,
        help='指定数据库端口，默认：27017')
    group2.add_argument(
        '--mongocachesize', action='store', metavar='int', dest='mongocachesize', type=int,
        help='指定cacheSizeGB的值:2G，默认：机器总内存的百分之70')
    group2.add_argument(
        '--skip-monitor', action='store_true', dest='skip_monitors',
        help='禁止监控，默认：监控')
    group2.add_argument(
        '--skip-backup', action='store_true', dest='skip_backups',
        help='禁止备份，默认：备份')
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

class Install_Mongodb(object):
    def __init__(self, args):
        self.perfix = args.perfix
        self.mongoversion = args.mongoversion
        self.mongoport = args.mongoport
        self.mongoenv = args.mongoenv
        self.mongocachesize = args.mongocachesize
        self.mongoreplname = args.mongoreplname
        self.createdb = args.createdb
        self.cluster_mode = args.cluster_mode
        self.replication_mode = args.replication_mode
        self.replicas = args.replicas
        self.configs = args.configs
        self.nodegroups = args.nodegroups
        self.uploadtype = args.uploadtype
        self.skip_backups = args.skip_backups
        self.skip_monitors = args.skip_monitors
        self.mongoss = args.mongoss
        self.host_roles = {}

        if self.cluster_mode:
            self.cnfdir = os.path.join(self.perfix, f"node{self.mongoport}")
            self.datadir = os.path.join(self.perfix, "data", f"node{self.mongoport}")
        elif self.replication_mode:
            self.cnfdir = os.path.join(self.perfix, f"db{self.mongoport}")
            self.datadir = os.path.join(self.perfix, "data", f"db{self.mongoport}")

    def minio_get(self):
        if not os.path.exists(self.datadir) and not os.path.exists(self.cnfdir):
            if not os.path.exists(self.perfix):
                if not os.path.exists(f'/root/****-mongo-{self.mongoversion}.tar.gz'):
                    if self.uploadtype == 'local':
                        logger.error(f"文件不存在 /root/****-mongo-{self.mongoversion}.tar.gz")
                        exit()
                    else:
                        logger.info(f"正在下载****-mongo-{self.mongoversion}.tar.gz")
                        from minio import Minio
                        minioClient = Minio('********:9199',
                                            access_key='****',
                                            secret_key='****',
                                            secure=False,
                                            region='cn-****-01')
                        minioClient.fget_object('****-db-pkg',
                                            f'****-mongo-{self.mongoversion}.tar.gz',
                                            f'/root/****-mongo-{self.mongoversion}.tar.gz')
                        logger.info(f"解压缩文件到{self.perfix}")
                self.extract(f'/root/****-mongo-{self.mongoversion}.tar.gz', self.perfix)
            os.makedirs(self.datadir)
            os.makedirs(self.cnfdir)
        else:
            logger.error(f"文件不为空{self.datadir} 或者 {self.cnfdir}，退出程序。。。。")
            exit()

    def extract(self, tarfilename, tarpath):
        logger.info(f"开始解压文件{tarfilename}到{tarpath}")
        try:
            tar = tarfile.open(tarfilename)
            tar.extractall(path=tarpath)
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
        logger.info("整理数据库信息")
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
        if self.cluster_mode:
            if len(self.createdb) - self.configs - (self.replicas) * self.nodegroups >= self.mongoss:
                self.host_roles['db'] = [self.createdb[i:i + self.replicas] for i in range(0,
                                (self.replicas) * self.nodegroups, self.replicas)]
                self.host_roles['config'] = self.createdb[self.replicas *
                                self.nodegroups:self.replicas * self.nodegroups + self.configs:]
                self.host_roles['mongos'] = self.createdb[self.replicas * self.nodegroups + self.configs::]
            else:
                logger.error("mongodb服务地址的个数未达到设置的数量，请查看配置")
        elif self.replication_mode:
            if len(self.createdb) > 0:
                self.host_roles['db'] = [ self.createdb ]
                self.host_roles['config'] = []
                self.host_roles['mongos'] = []
            else:
                logger.error("未添加mongodb服务地址")

    def init_db(self, grouphosts, groups):
        logger.info("修改mongo.conf文件")
        with open('/root/mongo-pkg/mongo.conf-default', 'r', encoding='utf-8') as fr:
            mongo_yaml = yaml.safe_load(fr)
        mongo_yaml['systemLog']['path'] = f"{self.cnfdir}/mongo.log"
        mongo_yaml['processManagement']['pidFilePath'] = f"{self.cnfdir}/mongo.pid"
        mongo_yaml['net']['port'] = self.mongoport
        mongo_yaml['net']['unixDomainSocket']['pathPrefix'] = self.cnfdir
        mongo_yaml['storage']['dbPath'] = self.datadir
        if self.mongocachesize:
            mongo_yaml['storage']['wiredTiger']['engineConfig']['cacheSizeGB'] = self.mongocachesize
        else:
            mongo_yaml['storage']['wiredTiger']['engineConfig']['cacheSizeGB'] = cachesize
        mongo_yaml['replication']['oplogSizeMB'] = str(sizemb)
        if self.cluster_mode:
            mongo_yaml['replication']['replSetName'] = f'{self.mongoreplname}-shard0{groups}'
            mongo_yaml['sharding'] = {}
            mongo_yaml['sharding']['clusterRole'] = 'shardsvr'
        else:
            mongo_yaml['replication']['replSetName'] = self.mongoreplname
        if not grouphosts.index(f'{ip}:{self.mongoport}') == 0 and f'{ip}:{self.mongoport}' in grouphosts:
            mongo_yaml['security'] = {}
            mongo_yaml['security']['keyFile'] = f'{self.perfix}/mongodb-keyfile'
            mongo_yaml['security']['authorization'] = 'enabled'
        with open(f'{self.cnfdir}/mongo.conf', 'w+', encoding='utf-8') as fw:
            yaml.safe_dump(mongo_yaml, fw)
        logger.info("创建用户和文件授权")
        subprocess.call("useradd -M -s /sbin/nologin mongo", shell=True)
        subprocess.call("chown -R mongo.mongo {0}".format(self.perfix), shell=True)
        logger.info("启动数据库")
        subprocess.call("sudo -u mongo /usr/bin/numactl --interleave=all {0}/bin/mongod -f {1}/mongo.conf"
                        .format(self.perfix, self.cnfdir), shell=True)

    def init_config(self, grouphosts):
        logger.info("修改config.conf文件")
        with open('/root/mongo-pkg/config.conf-default', 'r') as fr:
            config_yaml = yaml.safe_load(fr)
        config_yaml['systemLog']['path'] = f"{self.cnfdir}/config.log"
        config_yaml['processManagement']['pidFilePath'] = f"{self.cnfdir}/config.pid"
        config_yaml['net']['port'] = self.mongoport
        config_yaml['net']['unixDomainSocket']['pathPrefix'] = self.cnfdir
        config_yaml['storage']['dbPath'] = self.datadir
        if self.mongocachesize:
            config_yaml['storage']['wiredTiger']['engineConfig']['cacheSizeGB'] = self.mongocachesize
        else:
            config_yaml['storage']['wiredTiger']['engineConfig']['cacheSizeGB'] = cachesize
        config_yaml['replication']['oplogSizeMB'] = sizemb
        config_yaml['replication']['replSetName'] = f'{self.mongoreplname}-configs'
        config_yaml['sharding']['clusterRole'] = 'configsvr'
        if not grouphosts.index(f'{ip}:{self.mongoport}') == 0:
            config_yaml['security'] = {}
            config_yaml['security']['keyFile'] = f'{self.perfix}/mongodb-keyfile'
            config_yaml['security']['authorization'] = 'enabled'
        with open(f'{self.cnfdir}/config.conf', 'w+', encoding='utf-8') as fw:
            yaml.safe_dump(config_yaml, fw)
        logger.info("创建用户和文件授权")
        subprocess.call("useradd -M -s /sbin/nologin mongo", shell=True)
        subprocess.call("chown -R mongo.mongo {0}".format(self.perfix), shell=True)
        logger.info("启动数据库")
        subprocess.call("sudo -u mongo /usr/bin/numactl --interleave=all {0}/bin/mongod -f {1}/config.conf"
                        .format(self.perfix, self.cnfdir), shell=True)

    def init_mongos(self, grouphosts):
        #这里的grouphosts的指config的地址，需要写到配置文件中，详情参考mongos的配置文档
        logger.info("修改mongos.conf文件")
        with open('/root/mongo-pkg/mongos.conf-default', 'r') as fr:
            mongos_yaml = yaml.safe_load(fr)
        mongos_yaml['systemLog']['path'] = f"{self.cnfdir}/mongos.log"
        mongos_yaml['processManagement']['pidFilePath'] = f"{self.cnfdir}/mongos.pid"
        mongos_yaml['net']['port'] = self.mongoport
        mongos_yaml['net']['unixDomainSocket']['pathPrefix'] = f"{self.cnfdir}"
        hosts = ','.join(grouphosts)
        mongos_yaml['sharding']['configDB'] = f"{self.mongoreplname}-configs/{hosts}"
        mongos_yaml['security'] = {}
        mongos_yaml['security']['keyFile'] = f'{self.perfix}/mongodb-keyfile'
        with open(f'{self.cnfdir}/mongos.conf', 'w+', encoding='utf-8') as fw:
            yaml.safe_dump(mongos_yaml, fw)
        logger.info("创建用户和文件授权")
        subprocess.call("useradd -M -s /sbin/nologin mongo", shell=True)
        subprocess.call("chown -R mongo.mongo {0}".format(self.perfix), shell=True)
        logger.info("启动数据库")
        subprocess.call("sudo -u mongo /usr/bin/numactl --interleave=all {0}/bin/mongos -f {1}/mongos.conf"
                        .format(self.perfix, self.cnfdir), shell=True)
        if not os.path.exists("/usr/bin/mongo"):
            os.symlink(f"{self.perfix}/bin/mongo", "/usr/bin/mongo")

    def create_pk_db(self, grouphosts, groups):
        if grouphosts.index(f'{ip}:{self.mongoport}') == 0:
            logger.info("初始化集群分片信息")
            logger.debug(f"集群成员：{grouphosts}, 集群名称：{self.mongoreplname}")
            usertext = "db.createUser({ user: 'root', pwd: '****', roles: [ { role: 'root', db: 'admin'} ]})"
            if self.cluster_mode:
                config = {'_id': f'{self.mongoreplname}-shard0{groups}', 'members': [{'_id': 0, 'host': grouphosts[0]}]}
            else:
                config = {'_id': self.mongoreplname, 'members': [{'_id': 0, 'host': grouphosts[0]}]}
            subprocess.call(f"echo \"rs.initiate({config})\nsleep(5000)\n{usertext}\ndb.shutdownServer()\" | \
                            {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin", shell=True)
            logger.info("重启添加配置文件")
            with open(f'{self.cnfdir}/mongo.conf', 'r') as fr:
                mongo_yaml = yaml.safe_load(fr)
            mongo_yaml['security'] = {}
            mongo_yaml['security']['keyFile'] = f'{self.perfix}/mongodb-keyfile'
            mongo_yaml['security']['authorization'] = 'enabled'
            with open(f'{self.cnfdir}/mongo.conf', 'w+', encoding='utf-8') as fw:
                yaml.safe_dump(mongo_yaml, fw)
            subprocess.call("sudo -u mongo /usr/bin/numactl --interleave=all {0}/bin/mongod -f {1}/mongo.conf"
                            .format(self.perfix, self.cnfdir), shell=True)
            logger.info("添加机器到副本集中")
            for _ in range(60):
                if not False in [ self.check_port(i) for i in grouphosts ]:
                    for z in grouphosts[1::]:
                        logger.debug(f"添加机器：{z}")
                        subprocess.call(f"echo \"rs.add('{z}')\" | {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin \
                                            -uroot -p****", shell=True)
                    subprocess.call(f"{self.perfix}/bin/mongorestore --host 127.0.0.1 --port {self.mongoport} -uroot\
                                            -p**** --archive=/root/mongo-pkg/user.arch", shell=True)
                    subprocess.call(f"echo \"db.dropUser('root')\" | \
                                            {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin -uroot -p****", shell=True)
                    break
                else:
                    time.sleep(3)

    def create_pk_config(self, grouphosts):
        if grouphosts.index(f'{ip}:{self.mongoport}') == 0:
            logger.info("初始化集群配置服信息")
            logger.debug(f"集群成员：{grouphosts}")
            usertext = "db.createUser({ user: 'root', pwd: '****', roles: [ { role: 'root', db: 'admin'} ]})"
            config = {'_id': f'{self.mongoreplname}-configs', 'members': [{'_id': 0, 'host': grouphosts[0]}]}
            subprocess.call(f"echo \"rs.initiate({config})\nsleep(5000)\n{usertext}\ndb.shutdownServer()\" | \
                                    {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin", shell=True)
            logger.info("重启添加配置文件")
            with open(f'{self.cnfdir}/config.conf', 'r') as fr:
                config_yaml = yaml.safe_load(fr)
            config_yaml['security'] = {}
            config_yaml['security']['keyFile'] = f'{self.perfix}/mongodb-keyfile'
            config_yaml['security']['authorization'] = 'enabled'
            with open(f'{self.cnfdir}/config.conf', 'w+', encoding='utf-8') as fw:
                yaml.safe_dump(config_yaml, fw)
            subprocess.call("sudo -u mongo /usr/bin/numactl --interleave=all {0}/bin/mongod -f {1}/config.conf"
                                    .format(self.perfix, self.cnfdir), shell=True)
            logger.info("添加机器到副本集中")
            for _ in range(60):
                if not False in [ self.check_port(i) for i in grouphosts ]:
                    for z in grouphosts[1::]:
                        logger.debug(f"添加机器：{z}")
                        subprocess.call(f"echo \"rs.add('{z}')\" | {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin \
                                                -uroot -p****", shell=True)
                    break
                else:
                    time.sleep(3)

    def create_pk_mongos(self, grouphosts):
        #grouphosts只指mongos的机器，与init不同
        if grouphosts.index(f'{ip}:{self.mongoport}') == 0:
            logger.info("初始化集群路由信息")
            logger.debug(f"集群成员：{grouphosts}")
            for w in range(60):
                if not False in [ self.check_port(i) for i in self.createdb ]:
                    for x,y in enumerate(self.host_roles['db']):
                        config = ','.join(y)
                        subprocess.call(f"echo \"sh.addShard('{self.mongoreplname}-shard0{x}/{config}')\" |\
                                        {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin -uroot -p****", shell=True)
                    subprocess.call(f"{self.perfix}/bin/mongorestore --host 127.0.0.1 --port {self.mongoport} -uroot\
                                        -p**** --archive=/root/mongo-pkg/user.arch", shell=True)
                    subprocess.call(f"echo \"db.dropUser('root')\" | \
                                        {self.perfix}/bin/mongo 127.0.0.1:{self.mongoport}/admin -uroot -p****", shell=True)
                    break
                else:
                    if w == 59:
                        logger.error("请检查集群所有成员状态是否正常")
                    else:
                        time.sleep(3)

    def deploy_env(self):
        global cachesize, sizemb
        cachesize = int(psutil.virtual_memory().total / 1024 / 1024 / 1024 * 0.7)
        sizemb = round(psutil.disk_usage(f'{self.perfix}').total / 1024 / 1024 / 1024 * 0.05) * 1024
        if not os.path.exists(os.path.join(self.perfix, 'mongodb-keyfile')):
            shutil.copy('/root/mongo-pkg/mongodb-keyfile', self.perfix)
            subprocess.call(f"chmod 400 {self.perfix}/mongodb-keyfile", shell=True)
        shutil.copy('/root/mongo-pkg/mongod_service.sh', self.perfix)
        self.sorted_hosts()
        dbs = [ i for i,x in enumerate(self.host_roles['db']) if f'{ip}:{self.mongoport}' in x ]
        if f'{ip}:{self.mongoport}' in self.host_roles['config']:
            self.init_config(self.host_roles['config'])
            self.create_pk_config(self.host_roles['config'])
            self.init_monitor(self.host_roles['config'])
        elif f'{ip}:{self.mongoport}' in self.host_roles['mongos']:
            self.init_mongos(self.host_roles['config'])
            self.create_pk_mongos(self.host_roles['mongos'])
            self.init_backup(self.host_roles['mongos'])
        else:
            if dbs:
                self.init_db(self.host_roles['db'][dbs[0]], dbs[0])
                self.create_pk_db(self.host_roles['db'][dbs[0]], dbs[0])
                self.init_monitor(self.host_roles['db'][dbs[0]])
        if not os.path.exists("/usr/bin/mongo"):
            os.symlink(f"{self.perfix}/bin/mongo", "/usr/bin/mongo")

    def init_backup(self, grouphosts):
        if not self.skip_backups and f"{ip}:{self.mongoport}" == grouphosts[-1]:
            logger.info("添加mongodb备份")
            shutil.copy('/root/mongo-pkg/backup.py', self.perfix)
            if self.uploadtype == 'minio':
                shutil.copy('/root/mongo-pkg/upload-minio.py', '/usr/games/upload.py')
            elif self.uploadtype == 'ali':
                shutil.copy('/root/mongo-pkg/upload-ali.py', '/usr/games/upload.py')
            elif self.uploadtype == 'aws':
                subprocess.call("pip3 install boto3", shell=True)
                shutil.copy('/root/mongo-pkg/upload-aws.py', '/usr/games/upload.py')
            subprocess.call("chmod +x /usr/games/upload.py", shell=True)
            if self.uploadtype == 'local':
                clearday = 5
                upcycle = 0
            else:
                clearday = 3
                upcycle = 1
            my_cron = CronTab(user='root')
            iter = list(my_cron.find_command(re.compile(f"127.0.0.1 (.*) {self.mongoport}")))
            if not iter:
                command = (f"/usr/bin/python3 {self.perfix}/backup.py --host 127.0.0.1 --port {self.mongoport} "
                           f"--clearday {clearday} --upcycle {upcycle}")
                job = my_cron.new(command=command)
                job.set_comment("mongo数据备份")
                job.setall('00 04 * * *')
                my_cron.write()

    def init_monitor(self, grouphosts):
        logger.info("安装mongodb_exporter")
        if not os.path.exists('/usr/bin/mongodb_exporter'):
            shutil.copy('/root/mongo-pkg/mongodb_exporter', '/usr/bin')
        if not self.skip_monitors and f"{ip}:{self.mongoport}" == grouphosts[-1]:
            cfg2 = ConfigParser()
            if not os.path.exists('/etc/supervisord.d'):
                os.makedirs('/etc/supervisord.d')
            if not os.path.exists('/etc/supervisord.d/supervisord.conf'):
                cfg2.read('/root/mongo-pkg/supervisord.conf')
            else:
                cfg2.read('/etc/supervisord.d/supervisord.conf')
            urls = ','.join(grouphosts)
            mongourl = f"mongodb://monitoruser:oA4Pl5Usgr1sc@{urls}"
            cfg2[f'program:mongo_export{self.mongoport}'] = {}
            cfg2.set(f'program:mongo_export{self.mongoport}', 'command',
                     f'/usr/bin/mongodb_exporter --mongodb.uri={mongourl}')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'autostart', 'true')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'startsecs', '10')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'autorestart', 'true')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'startretries', '3')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'user', 'mongo')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'priority', '999')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'redirect_stderr', 'true')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'stdout_logfile_maxbytes', '20MB')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'stdout_logfile_backups', '20')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'stdout_logfile', f'{self.cnfdir}/monitor.log')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'stopasgroup', 'false')
            cfg2.set(f'program:mongo_export{self.mongoport}', 'killasgroup', 'false')
            with open('/etc/supervisord.d/supervisord.conf', 'w+') as fw2:
                cfg2.write(fw2)
            if os.path.exists('/etc/supervisord.d/supervisord.pid'):
                subprocess.call("/usr/local/bin/supervisorctl -c /etc/supervisord.d/supervisord.conf reload", shell=True)
            else:
                subprocess.call("/usr/local/bin/supervisord -c /etc/supervisord.d/supervisord.conf", shell=True)

    def main(self):
        self.sorted_hosts()
        logger.debug(f"主机列表：{self.createdb}")
        if f"{ip}:{self.mongoport}" in self.createdb:
            self.minio_get()
            self.deploy_env()
        else:
            logger.error("找不到主机")

if __name__ == '__main__':
    logger.add(f'/tmp/install_mongodb.log', level='DEBUG',
               format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}')
    args = get_apt()
    ip = get_host_ip()
    install = Install_Mongodb(args)
    install.main()
