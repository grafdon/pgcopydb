set pagination off
set confirm off
set may-call-functions off
# the breakpoint and $finish come from the command line; with $finish set the
# function returns first, then the process is killed as by kill -9
continue
if $_isvoid($_exitcode)
    if $finish
        info args
        finish
    end
    echo KILLED-AT-BREAKPOINT\n
    kill
    quit 0
end
echo FAIL: the process exited before the breakpoint\n
quit 1
