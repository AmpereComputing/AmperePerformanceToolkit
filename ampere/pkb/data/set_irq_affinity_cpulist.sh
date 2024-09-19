#! /bin/bash
#
# Modifications Copyright (c) 2024 Ampere Computing LLC
# Copyright (c) 2017 Mellanox Technologies. All rights reserved.
#
# Distributed by Ampere Computing LLC per terms of the BSD License as permitted by original
# Mellanox License below
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation and/or
#    other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors may be
#    used to endorse or promote products derived from this software without specific prior
#    written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS” AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# This Software is licensed under one of the following licenses:
#
# 1) under the terms of the "Common Public License 1.0" a copy of which is
#    available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/cpl.php.
#
# 2) under the terms of the "The BSD License" a copy of which is
#    available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/bsd-license.php.
#
# 3) under the terms of the "GNU General Public License (GPL) Version 2" a
#    copy of which is available from the Open Source Initiative, see
#    http://www.opensource.org/licenses/gpl-license.php.
#
# Licensee has the right to choose one of the above licenses.
#
# Redistributions of source code must retain the above copyright
# notice and one of the license notices.
#
# Redistributions in binary form must reproduce both the above copyright
# notice, one of the license notices in the documentation
# and/or other materials provided with the distribution.
#
if [ -z $2 ]; then
	echo "usage: $0 <cpu list> <interface or IB device> "
	echo "       <cpu list> can be either a comma separated list of single core numbers (0,1,2,3) or core groups (0-3)"
	exit 1
fi
cpulist=$1
interface=$2
IRQS=$3
NCPUS=$(cat /proc/cpuinfo | grep -c processor)
ONLINE_CPUS=$(cat /proc/cpuinfo | grep processor | cut -d ":" -f 2)

source common_irq_affinity.sh

if [ -z "$IRQS" ] ; then
	IRQS=$( get_irq_list $interface )
fi


if [ -z "$IRQS" ] ; then
        echo No IRQs found for $interface.
	exit 1
fi

CORES=$( echo $cpulist | sed 's/,/ /g' | wc -w )
for word in $(seq 1 $CORES)
do
	SEQ=$(echo $cpulist | cut -d "," -f $word | sed 's/-/ /')	
	if [ "$(echo $SEQ | wc -w)" != "1" ]; then
		CPULIST="$CPULIST $( echo $(seq $SEQ) | sed 's/ /,/g' )"
	fi
done
if [ "$CPULIST" != "" ]; then
	cpulist=$(echo $CPULIST | sed 's/ /,/g')
fi
CORES=$( echo $cpulist | sed 's/,/ /g' | wc -w )


echo Discovered irqs for $interface: $IRQS
I=1
for IRQ in $IRQS
do 
	core_id=$(echo $cpulist | cut -d "," -f $I)
	online=1
	if [ $core_id -ge $NCPUS ]
	then
		online=0
		for online_cpu in $ONLINE_CPUS
		do
			if [ "$online_cpu" == "$core_id" ]
			then
				online=1
				break
			fi
		done
	fi
	if [ $online -eq 0 ]
	then
		echo "irq $IRQ: Error - core $core_id does not exist"
	else
		echo Assign irq $IRQ core_id $core_id
	        affinity=$( core_to_affinity $core_id )
	        set_irq_affinity $IRQ $affinity
	fi
	I=$(( (I%CORES) + 1 ))
done
echo 
echo done.


