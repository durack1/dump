#!/bin/bash

# File written to download all INM PMIP files
# PJD 26 Sep 2022	- Created to download latest
# PJD  7 Oct 2022   - Updated to deal with CMIP6_2 data
# 			- TODO: create matlab file to reconstruct netcdf files with time periods within filename

export date=`date +%y%m%d`
export inmPath=/p/user_pub/climate_work/durack1/tmp
cd ${inmPath}
#\rm -r -f ${date}
#\mkdir ${date}
#cd ${date}

# Connect local (nautilus file manager) sftp connection
# https://superuser.com/questions/1256238/where-does-nautilus-file-manager-mounts-ftp-smb-connections
# Connect to server: sftp://guest@ksv.inm.ras.ru/
# Works
# bash-4.2$ rsync -vrutaz /run/user/40336/gvfs/sftp\:host\=ksv.inm.ras.ru\,user\=guest/archive2/volodin/CMIP6/CMIP6/PMIP/INM/INM-CM4-8/past1000/r1i1p1f1/Amon/tas ~/p-work/tmp/

:<<"--"
bash-4.2$ rsync -vrutaz /run/user/40336/gvfs/sftp\:host\=ksv.inm.ras.ru\,user\=guest/archive2/volodin/CMIP6/CMIP6/PMIP/INM/INM-CM4-8/past1000/r1i1p1f1/Amon/tas ~/p-work/tmp/
sending incremental file list
tas/
tas/gr1/
tas/gr1/v20220526/
tas/gr1/v20220526/tas_Amon_INM-CM4-8_past1000_r1i1p1f1_gr1_085001-094912.nc
tas/gr1/v20220526/tas_Amon_INM-CM4-8_past1000_r1i1p1f1_gr1_095001-104912.nc
^C^Crsync error: received SIGINT, SIGTERM, or SIGHUP (code 20) at rsync.c(638) [sender=3.1.2]
--

\rsync -vrutaz --progress /run/user/40336/gvfs/sftp\:host\=ksv.inm.ras.ru\,user\=guest/archive2/volodin/CMIP6_2/ ~/p-work/tmp/ --log-file=${date}_CMIP6-INM-PMIP.log
# https://superuser.com/questions/1256238/where-does-nautilus-file-manager-mounts-ftp-smb-connections
# https://www.cyberciti.biz/faq/show-progress-during-file-transfer/

# https://serverfault.com/questions/24622/how-to-use-rsync-over-ftp
<<"--"
HOST="ksv.inm.ras.ru"
USER="guest"
PASS=""
FTPURL="ftp://$USER:$PASS@$HOST"
LCD="."
RCD="/archive2/volodin"
#DELETE="--delete"
lftp -c "set ftp:list-options -a;
open '$FTPURL';
lcd $LCD;
cd $RCD;
mirror --reverse \
       $DELETE \
       --verbose \
       --exclude-glob a-dir-to-exclude/ \
       --exclude-glob a-file-to-exclude"
--

<<"--"
Comment Block for symlink creation
--
echo "done"
