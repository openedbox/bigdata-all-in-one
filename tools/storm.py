from helper import SshClient, ScpClient


class Storm:
    def __init__(self):
        pass

    @staticmethod
    def install(options):
        for host in options.all_storm_hosts:
            ssh = SshClient(host, options.root_password)
            if 'cannot open' in ssh.execute("file " + options.storm_path):
                ScpClient.local2remote(host, options.root_password, options.storm_package_path,
                                       '/usr/local/storm')
                ssh.execute('tar zxf /usr/local/storm -C /usr/local')
                ssh.execute('rm -rf /usr/local/storm')
                ssh.execute(
                    'echo export STORM_HOME=%slib/%s >>/etc/profile' % (options.storm_path, options.storm_version))
                ssh.execute('echo export STORM_YARN_HOME=%s >>/etc/profile' % options.storm_path)
                ssh.execute('echo export PATH=\$STORM_HOME/bin:\$STORM_YARN_HOME/bin:\$PATH >>/etc/profile')
                ssh.execute('source /etc/profile')


                ssh.execute('echo \\ storm.zookeeper.servers: >> %s/lib/%s/conf/storm.yaml' % (
                    options.storm_path, options.storm_version))
                for zookeeper_host in options.all_zookeeper_hosts:
                    ssh.execute(
                        'echo \\ -\\ \\"%s\\" >> %s/lib/%s/conf/storm.yaml' % (
                            options.host_names[zookeeper_host], options.storm_path, options.storm_version))

                ssh.execute('echo \\ nimbus.host: \\"%s\\" >> %s/lib/%s/conf/storm.yaml' % (
                    options.host_names[options.nimbus_nodes[0]], options.storm_path, options.storm_version))

                ssh.execute('echo \\ storm.local.dir: \\"%s\\" >> %s/lib/%s/conf/storm.yaml' % (
                    options.storm_local_dir, options.storm_path, options.storm_version))

                ssh.execute('mkdir %s' % options.storm_local_dir)

                ssh.execute('echo \\ supervisor.slots.ports: >> %s/lib/%s/conf/storm.yaml' % (
                    options.storm_path, options.storm_version))
                for port in options.storm_slot_ports:
                    ssh.execute(
                        'echo \\ -\\ %s >> %s/lib/%s/conf/storm.yaml' % (port, options.storm_path, options.storm_version))

                ssh.execute(
                    'echo  \\ ui.port: %s >> %s/yarn.yaml' % (options.storm_ui_port, options.storm_path))

                ssh.execute('touch %s/yarn.yaml' % options.storm_path)
                ssh.execute('echo \\ storm.zookeeper.servers: >> %s/yarn.yaml' % options.storm_path)
                for zookeeper_host in options.all_zookeeper_hosts:
                    ssh.execute(
                        'echo \\ -\\ \\"%s\\" >> %s/yarn.yaml' % (options.host_names[zookeeper_host], options.storm_path))

            ssh.close()

    @staticmethod
    def start(options):
        ssh = SshClient(options.nimbus_nodes[0], options.root_password)
        ssh.execute('storm nimbus > /dev/null 2>&1 &')
        ssh.execute('storm ui > /dev/null 2>&1 &')
        ssh.close()

        for host in options.supervisor_nodes:
            ssh = SshClient(host, options.root_password)
            ssh.execute('storm supervisor > /dev/null 2>&1 & ')
            ssh.close()