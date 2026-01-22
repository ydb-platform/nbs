# disable packages autoupdate
sudo sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sudo sed 's/APT::Periodic::Unattended-Upgrade "1";/APT::Periodic::Unattended-Upgrade "0";/' /etc/apt/apt.conf.d/20auto-upgrades -i
sudo sed 's/APT::Periodic::Update-Package-Lists "1";/APT::Periodic::Update-Package-Lists "0";/' /etc/apt/apt.conf.d/10periodic -i

sudo systemctl stop stop

sudo cp -R -p  /var/lib/postgresql /DB/postgresql
sudo chmod 700 -R /DB/postgresql

sudo sed 's/\/var\/lib\/postgresql/\/DB\/postgresql/' /etc/postgresql/*/main/postgresql.conf -i
sudo sed 's/peer/trust/' /etc/postgresql/*/main/pg_hba.conf -i

sudo systemctl start stop

sudo -u postgres -i psql -c "CREATE DATABASE pgbench;"
