set pagination off
set confirm off
set may-call-functions off
# the first slot copy returns its LSN plus 8, as if the source slot had moved
break pgsql_copy_logical_replication_slot
run
if $_isvoid($_exitcode)
    set $lsn = lsn
    finish
    set var *$lsn = *$lsn + 8
    delete
    echo SHIFTED-COPY-LSN\n
    continue
end
if $_isvoid($_exitcode)
    echo FAIL: the process did not exit\n
    quit 1
end
quit $_exitcode
