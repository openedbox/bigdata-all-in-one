from helper import SshClient, ScpClient, SshCopyIdClient


class Infra:
    def __init__(self):
        pass

    @staticmethod
    def install(options):
        for host in options.all_hosts:
            ssh = SshClient(host, options.root_password)
            ssh.execute('yum install -y ntp openssh-clients wget lsof')

            print 'clear installed files on %s' % host
            paths = [options.jdk_path, options.hadoop_path, options.zookeeper_path,
                     options.storm_path, options.hive_path]
            for path in paths:
                ssh.execute('rm -rf %s' % path)

            print 'reset env variable on %s' % host
            ScpClient.local2remote(host, options.root_password, '/etc/profile', '/etc')
            ssh.execute('source /etc/profile')

            print 'disable iptables on %s' % host
            ssh.execute('service iptables stop')
            ssh.execute('chkconfig iptables off')

            print 'disable selinux on %s' % host
            ssh.execute('sed -i \'s/SELINUX=enforcing/SELINUX=disabled/\' /etc/selinux/config')
            ssh.execute('sed -i \'s/SELINUXTYPE=targeted/#SELINUXTYPE=targeted/\' /etc/selinux/config')

            print 'reset ssh config on %s' % host
            ScpClient.local2remote(host, options.root_password, '/etc/ssh/ssh_config', '/etc/ssh')
            ssh.execute(
                'sed -i \'s/GSSAPIAuthentication yes/GSSAPIAuthentication yes \\n StrictHostKeyChecking no/\' /etc/ssh/ssh_config')
            ssh.execute('rm -rf /root/.ssh/id_rsa')

            print 'config hosts on %s' % host
            ssh.execute('rm -rf /etc/hosts')
            ssh.execute('touch /etc/hosts')

            for (k, v) in options.host_names.items():
                ssh.execute('echo %s %s >>/etc/hosts' % (k, v))

            SshCopyIdClient.genkey(host, options.root_password)
            SshCopyIdClient.copy(host, options.host_names.values(), options.root_password)

            print 'check jdk'
            if 'cannot open' in ssh.execute("file " + options.jdk_path):
                ScpClient.local2remote(host, options.root_password, options.jdk_package_path,
                                       '/usr/local/jdk')
                ssh.execute('tar zxf /usr/local/jdk -C /usr/local')
                ssh.execute('rm -rf /usr/local/jdk')
                ssh.execute('echo export JAVA_HOME=%s >>/etc/profile' % options.jdk_path)
                ssh.execute(
                    'echo export CLASSPATH=.:\$JAVA_HOME:/lib/tools.jar:\$JAVA_HOME:/lib/dt.jar >> /etc/profile')
                ssh.execute('echo export PATH=\$JAVA_HOME/bin:\$PATH >>/etc/profile')
                ssh.execute('source /etc/profile')

            ssh.execute('reboot')
            ssh.close()

    @staticmethod
    def reboot(options):
        for node in options.all_hosts:
            ssh = SshClient(node, options.root_password)
            ssh.execute('reboot')
            ssh.close()