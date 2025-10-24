#!/bin/tclsh
puts "SETTING CONFIGURATION"
global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} { after 5000 wait_to_complete } else { exit }
}
dbset db pg
loadscript
diset connection pg_host localhost
diset connection pg_port 5004
diset tpcc pg_dbase chbenchmark
diset tpcc pg_user vista
diset tpcc pg_superuser vista
diset tpcc pg_defaultdbase chbenchmark
diset tpcc pg_pass vista
diset tpcc pg_superuserpass vista
diset tpcc pg_storedprocs true
diset tpcc pg_count_ware 1000
diset tpcc pg_allwarehouse false
diset tpcc pg_driver timed
diset tpcc pg_rampup 0
diset tpcc pg_duration 1000
diset tpcc pg_timeprofile true
diset tpcc pg_raiseerror true
loadscript
print dict
vuset vu 12
vuset timestamps 1
vuset logtotemp 1
vuset showoutput 0
vuset unique 1
vuset delay 20
vuset repeat 1
vurun
wait_to_complete
vwait forever
