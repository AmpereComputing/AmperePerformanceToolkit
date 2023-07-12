#!/bin/bash

# Here are the patch on Hammerdb on version 4.0 so it works on Azure Postgres
#
# Patch is necessary because Hammerdb release version is not in sync
# with the github version https://github.com/TPC-Council/HammerDB
# and the fix for postgres is not yet accepted into the official branch.
# This patch is made against commit 58a8d0ab4c1674274b2ce0539a07fae0daf93a90
#
# To update Hammerdb version, you will need to merge the current patch against
# the latter version and make update if necessary.
#
# 1. Fork the HammerDB official repo
# 2. Apply the patch on pgolap.tcl pgoltp.tcl and postgresql.xml
# 3. Merge with the latest release and resolve conflicts
sudo patch -u -b /var/lib/google/HammerDB/src/postgresql/pgolap.tcl -i pgolap.tcl.patch
sudo patch -u -b /var/lib/google/HammerDB/src/postgresql/pgoltp.tcl -i pgoltp.tcl.patch
sudo patch -u -b /var/lib/google/HammerDB/config/postgresql.xml -i postgresql.xml.patch
sudo patch -u -b /var/lib/google/HammerDB/modules/etprof-1.1.tm -i etprof-1.1.tm.patch
#sudo apt-get --assume-yes install libxft2 lib32ncurses5
