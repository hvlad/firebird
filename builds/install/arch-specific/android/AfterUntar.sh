#!/bin/sh

echo "This script builds arch-specific files, useful for firebird functionality"

runAndCheckExit() {
	Msg=$1
    Cmd=$2

	echo $Msg please wait...
    eval $Cmd
    ExitCode=$?

    if [ $ExitCode -ne 0 ]
    then
        echo "Aborted: The command $Cmd "
        echo "         failed with error code $ExitCode"
        exit $ExitCode
    fi
}

runAndCheckExit "Build messages file (firebird.msg)" "bin/build_file -f firebird.msg"
runAndCheckExit "Creating security database" "echo create database \'security5.fdb\'^ | bin/isql -q -term ^"
runAndCheckExit "Creating security database metadata" "bin/isql -q security5.fdb -i security.sql"
runAndCheckExit "Restore examples database (employee)" "(cd examples/empbuild ; ../../bin/isql -q -i ../../employe2.sql)"

rm -f security.sql employe2.sql bin/build_file AfterUntar.sh
