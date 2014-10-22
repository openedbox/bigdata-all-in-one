import subprocess
import sys
import sqlite3
import os
# from thrift import Thrift
# from thrift.transport import TSocket
# from thrift.transport import TTransport
# from thrift.protocol import TBinaryProtocol
from xml.dom.minidom import Document
import pexpect
import pxssh

sys.path.append('..')
# from THService import THService


# class ThriftClient:
#     def __init__(self, host, port):
#         self.host = host
#         self.port = port
#         try:
#             self.transport = TSocket.TSocket(self.host, self.port)
#             transport = TTransport.TBufferedTransport(self.transport)
#             protocol = TBinaryProtocol.TBinaryProtocol(transport)
#             self.client = THService.Client(protocol)
#             transport.open()
#         except Thrift.TException, ex:
#             print "%s" % (ex.message)
#
#     def execute(self, cmd):
#         return self.client.Exec(cmd)
#
#     def transfer(self, filename, content):
#         return self.client.FileTransfer(filename, content)
#
#     def echo(self):
#         return self.client.Echo()
#
#     def close(self):
#         self.transport.close()


class DbClient:
    def __init__(self):
        self.conn = sqlite3.connect('thmanager.db')
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' "
                       "AND name='master'")
        if len(cursor.fetchall()) == 0:
            print "empty db"
            cursor.execute("create table master(id integer primary key,"
                           "name varchar(50),"
                           "ip varchar(20))")
        else:
            print "normal db"
        cursor.close()

    def close(self):
        self.conn.close()


class SshClient:
    def __init__(self, host, pwd):
        self.host = host
        client = pxssh.pxssh()
        if not client.login(host, 'root', pwd):
            print "cannot connect server %s" % host

        self.client = client

    def execute(self, cmd):
        print 'root@%s> %s' % (self.host, cmd)
        client = self.client
        client.sendline(cmd)
        client.prompt(timeout=3600000)
        return client.before.replace(cmd, '')[2:][:-2]

    def send_line(self, cmd):
        self.client.sendline(cmd)

    def close(self):
        self.client.logout()


class ScpClient:
    @staticmethod
    def local2remote(host, password, source, target):
        cmd = 'scp %s root@%s:%s' % (source, host, target)
        print cmd
        child = pexpect.spawn(cmd)
        i = child.expect(['password:', pexpect.EOF])
        if i == 0:
            child.sendline(password)
        child.expect(pexpect.EOF)
        child.close()

    @staticmethod
    def remote2local(host, password, source, target):
        child = pexpect.spawn('scp root@%s:%s %s' % (host, source, target))
        i = child.expect(['password', pexpect.EOF])
        if i == 0:
            child.sendline(password)
        child.close()


class SshCopyIdClient:
    @staticmethod
    def genkey(master, password):
        ssh = pxssh.pxssh()
        if not ssh.login(master, 'root', password):
            print "cannot connect server %s" % master

        print 'send ssh-keygen'
        ssh.sendline('ssh-keygen -t rsa')
        try:
            ssh.expect('Generating.*:')
            ssh.sendline()
            ssh.expect('Enter.*:')
            ssh.sendline()
            ssh.expect('Enter.*:')
            ssh.sendline()
        except pexpect.TIMEOUT:
            print 'timeout'
        finally:
            ssh.logout()

    @staticmethod
    def copy(master, nodes, password):
        ssh = pxssh.pxssh()
        if not ssh.login(master, 'root', password):
            print "cannot connect server %s" % master

        for node in nodes:
            ssh.sendline('ssh-copy-id root@%s' % node)
            try:
                index = ssh.expect(['continue connecting \(yes/no\)', '\'s password:', pexpect.EOF], timeout=8)
                if index == 0:
                    ssh.sendline('yes')
                if index == 1:
                    ssh.sendline(password)
                    ssh.sendline(password)
            except pexpect.TIMEOUT:
                print 'timeout'
            else:
                pass

        ssh.logout()


class CmdClient:
    @staticmethod
    def execute(cmd):
        process = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        tmp_out = process.stdout.readlines()
        tmp_err = process.stderr.readlines()
        tmp = tmp_out + tmp_err
        tmp = "".join(tmp)
        return tmp


class HadoopConfigGen:
    def __init__(self):
        self.doc = Document()
        self.root = self.doc.createElement('configuration')
        self.doc.appendChild(self.root)

    def add_property(self, name, value):
        prop_element = self.doc.createElement('property')
        name_element = self.doc.createElement('name')
        value_element = self.doc.createElement('value')
        name_element.appendChild(self.doc.createTextNode(name))
        value_element.appendChild(self.doc.createTextNode(value))
        prop_element.appendChild(name_element)
        prop_element.appendChild(value_element)
        self.root.appendChild(prop_element)

    def save(self, path):
        f = open(path, 'w')
        f.write(self.doc.toxml())
        f.close()

    def content(self):
        return self.doc.toprettyxml(indent=' ')
