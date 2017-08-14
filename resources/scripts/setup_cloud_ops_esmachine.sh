

# DIRECTORY FOR LOGS
sudo -i
mkdir /logs
chown klahnakoski:users /logs
exit

# PRIVATE FILE
echo -e "{}" > ~/private.json

# INSTALL REPLICATION
git clone https://github.com/klahnakoski/esReplicate.git
cd ~/esReplicate
sudo pip install -r requirements.txt

# INSTALL JAVA 8
mkdir ~/temp
# PUT JAVA ON esmachine
# ISSUED FROM JUMPHOST: scp jre-8u131-linux-x64.rpm klahnakoski@esmachine:~/temp
cd temp
sudo rpm -i jre-8u131-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/defau
lt/bin/java 20000
export JAVA_HOME=/usr/java/default
#CHECK IT IS 1.8
java -version

# INSTALL ELASTICSEARCH
cd ~
wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz
tar zxfv elasticsearch-1.7.1.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.7.1/* /usr/local/elasticsearch/
sudo -i
echo -e "script.inline: on\nscript.indexed: on\ncluster.name: cloud_ops_esmachine\nnode.name: 1\nindex.number_of_shards: 1\nindex.number_of_replicas: 0\nhttp.compression: true" > /usr/local/elasticsearch/config/elasticsearch.yml
exit

# INSTALL MODIFIED SUPERVISOR
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
sudo yum groupinstall -y "Development tools"

sudo pip install pyopenssl
sudo pip install ndg-httpsclient
sudo pip install pyasn1
sudo pip install requests
sudo pip install fabric==1.10.2
sudo pip install supervisor-plus-cron

sudo -i
cd /usr/bin
ln -s /usr/local/bin/supervisorctl supervisorctl
cp ~/esReplicate/resources/supervisor/es.conf /etc/supervisord.conf
exit

# START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo -i
/usr/bin/supervisord -c /etc/supervisord.conf
/usr/bin/supervisorctl reread
/usr/bin/supervisorctl update
exit


# SETUP CRON JOBS
chmod u+x /home/klahnakoski/esReplicate/resources/scripts/replicate_orange_factor.sh


# CRON FILE (TURN "OFF" AND "ON", RESPECTIVLY)
sudo rm /var/spool/cron/ec2-user
sudo cp /home/klahnakoski/esReplicate/resources/cron/cloud_ops_esmachine.cron /var/spool/cron/klahnakoski




