mkdir /DB
sudo mkfs.ext4 -b 4096 /dev/vdb

# disable packages autoupdate
sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sed 's/APT::Periodic::Unattended-Upgrade "1";/APT::Periodic::Unattended-Upgrade "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/10periodic -i

# add /dev/vdb to fstab
blk=(`blkid | grep /dev/vdb`)
uuid=${blk[1]}
echo "${uuid} /DB ext4 errors=remount-ro 0       1" >> /etc/fstab
mount /dev/vdb /DB

# change mysql data dir
service mysql stop
cp -R -p /var/lib/mysql /DB/mysql
sed 's/\/var\/lib\/mysql/\/DB\/mysql/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sed 's/# datadir/datadir/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sed 's/.*binlog_expire_logs_seconds.*/binlog_expire_logs_seconds = 604800/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sed 's/\/var\/lib\/mysql/\/DB\/mysql/' /etc/apparmor.d/usr.sbin.mysqld -i
service apparmor reload
service mysql restart

# prepare database
mysql --user='root' --execute='CREATE DATABASE sbtest;'
