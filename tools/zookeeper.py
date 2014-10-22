from helper import SshClient, ScpClient


class Zookeeper:
    def __init__(self):
        pass

    @staticmethod
    def install(options):
        zookeeper_names_by_id = []
        for host in options.all_zookeeper_hosts:
            zookeeper_names_by_id.append(options.host_names[host])

        for host in options.all_zookeeper_hosts:
            ssh = SshClient(host, options.root_password)
            if 'cannot open' in ssh.execute("file " + options.zookeeper_path):
                ScpClient.local2remote(host, options.root_password, options.zookeeper_package_path,
                                       '/usr/local/zookeeper')
                ssh.execute('tar zxf /usr/local/zookeeper -C /usr/local')
                ssh.execute('rm -rf /usr/local/zookeeper')
                ssh.execute('echo export ZOOKEEPER_HOME=%s >>/etc/profile' % options.zookeeper_path)
                ssh.execute('echo export PATH=\$ZOOKEEPER_HOME/bin:\$PATH >>/etc/profile')
                ssh.execute('source /etc/profile')

                ssh.execute('touch %s/conf/zoo.cfg' % options.zookeeper_path)
                ssh.execute('mkdir %s' % options.zookeeper_data_dir)
                ssh.execute('mkdir %s' % options.zookeeper_log_dir)
                ssh.execute('echo tickTime=2000 >> %s/conf/zoo.cfg' % options.zookeeper_path)
                ssh.execute('echo initLimit=10 >> %s/conf/zoo.cfg' % options.zookeeper_path)
                ssh.execute('echo syncLimit=5 >> %s/conf/zoo.cfg' % options.zookeeper_path)
                ssh.execute('echo dataDir=%s >> %s/conf/zoo.cfg' % (
                    options.zookeeper_data_dir, options.zookeeper_path))
                ssh.execute('echo dataLogDir=%s >> %s/conf/zoo.cfg' % (
                    options.zookeeper_log_dir, options.zookeeper_path))
                ssh.execute('echo clientPort=2181 >> %s/conf/zoo.cfg' % options.zookeeper_path)
                for i in range(1, len(zookeeper_names_by_id) + 1):
                    ssh.execute('echo server.%d=%s:2888:3888 >> %s/conf/zoo.cfg' % (
                        i, zookeeper_names_by_id[i - 1], options.zookeeper_path))
                ssh.execute('touch %s/myid' % options.zookeeper_data_dir)
                myid = zookeeper_names_by_id.index(options.host_names[host])
                ssh.execute('echo %s >> %s/myid' % (myid + 1, options.zookeeper_data_dir))
            ssh.close()

    @staticmethod
    def start(options):
        print 'start zookeeper'
        for host in options.all_zookeeper_hosts:
            ssh = SshClient(host, options.root_password)
            ssh.execute('zkServer.sh start')
            ssh.close()

    @staticmethod
    def stop(options):
        print 'stop zookeeper'
        for host in options.all_zookeeper_hosts:
            ssh = SshClient(host, options.root_password)
            ssh.execute('zkServer.sh stop')
            ssh.close()