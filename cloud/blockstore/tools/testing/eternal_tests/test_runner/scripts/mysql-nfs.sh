# disable packages autoupdate
sudo sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sudo sed 's/APT::Periodic::Unattended-Upgrade "1";/APT::Periodic::Unattended-Upgrade "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sudo sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/10periodic -i

# change mysql data dir
sudo systemctl stop mysql
sudo cp -R -p /var/lib/mysql /DB/mysql
sudo sed 's/\/var\/lib\/mysql/\/DB\/mysql/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sudo sed 's/# datadir/datadir/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sudo sed 's/.*binlog_expire_logs_seconds.*/binlog_expire_logs_seconds = 604800/' /etc/mysql/mysql.conf.d/mysqld.cnf -i
sudo sed 's/\/var\/lib\/mysql/\/DB\/mysql/' /etc/apparmor.d/usr.sbin.mysqld -i
sudo systemctl reload apparmor
sudo systemctl start mysql

# prepare database
sudo mysql --user='root' --execute='CREATE DATABASE sbtest;'
