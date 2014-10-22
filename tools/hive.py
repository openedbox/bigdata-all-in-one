from helper import SshClient, ScpClient, HadoopConfigGen


class Hive:
    def __init__(self):
        pass

    @staticmethod
    def install(options):
        hive = HadoopConfigGen()
        hive.add_property('javax.jdo.option.ConnectionURL', options.hive_meta_db_url)
        hive.add_property('javax.jdo.option.ConnectionDriverName', options.hive_meta_db_driver)
        hive.add_property('javax.jdo.option.ConnectionUserName', options.hive_meta_db_user)
        hive.add_property('javax.jdo.option.ConnectionPassword', options.hive_meta_db_password)
        hive.save('./tmp/hive-site.xml')

        ssh = SshClient(options.hive_host, options.root_password)
        if 'cannot open' in ssh.execute("file " + options.hive_path):
            ScpClient.local2remote(options.hive_host, options.root_password, options.hive_package_path,
                                   '/usr/local/hive')
            ssh.execute('tar zxf /usr/local/hive -C /usr/local')
            ssh.execute('rm -rf /usr/local/hive')
            ssh.execute('echo export HIVE_HOME=%s >>/etc/profile' % options.hive_path)
            ssh.execute('echo export PATH=\$HIVE_HOME/bin:\$PATH >>/etc/profile')
            ssh.execute('source /etc/profile')

            ssh.execute('rm -rf %s/lib/zookeeper*.jar' % options.hive_path)
            ssh.execute('cp %s/zookeeper*.jar %s/lib/' % (options.zookeeper_path, options.hive_path))
            ssh.execute('cp %s %s/lib' % (options.protobuf_jar, options.hive_path))
            ssh.execute('cp %s %s/lib' % (options.hbase_common_jar, options.hive_path))
            ssh.execute('cp %s %s/lib' % (options.hbase_client_jar, options.hive_path))

            ssh.execute('touch %s/conf/hive-env.sh' % options.hive_path)
            ssh.execute('echo HADOOP_HOME=%s >> %s/conf/hive-env.sh' % (options.hadoop_path, options.hive_path))
            ssh.execute(
                'echo export HIVE_CONF_DIR=%s/conf >> %s/conf/hive-env.sh' % (options.hive_path, options.hive_path))
            ssh.execute(
                'echo export HIVE_AUX_JARS_PATH=%s/lib >> %s/conf/hive-env.sh' % (options.hive_path, options.hive_path))

            ScpClient.local2remote(options.hive_host, options.root_password, './tmp/hive-site.xml',
                                   '%s/conf/' % options.hive_path)
            ScpClient.local2remote(options.hive_host, options.root_password, options.hive_db_driver_jar,
                                   '%s/lib/' % options.hive_path)

        ssh.close()