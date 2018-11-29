#!/bin/bash

BOLD="\033[1;37m"
UNDERLINE="\033[4;37m"
RED="\033[31m"
GREEN="\033[32m"
BLUE="\033[34m"
END="\033[0m"

function print_usage() {
	echo -e "${BOLD}OpsWorker${END}"
	echo
	echo -e "${UNDERLINE}Usage:${END}"
	echo -e "  ${GREEN}ops.sh worker [command]${END}"
	echo
	echo -e "${UNDERLINE}Commands:${END}"
	echo -e "  ${GREEN}start${END}          Start OpsWorker"
	echo -e "  ${GREEN}stop${END}           Stop OpsWorker"
	echo
	echo -e "${UNDERLINE}Options:${END}"
	echo -e "  ${BLUE}-h, --help${END}     Show usage"
}

TIMEOUT=5
CLASS=OpsWorker

if [[ ${CLASSPATH} ]]; then
	case $1 in
	start)
		java -cp ${CLASSPATH} cn.edu.sjtu.ist.ops.${CLASS}
		;;
	stop)
		PID=$(ps -ef | grep ${CLASS} | grep -v grep | awk '{print $2}' | head -1)
		if kill -0 ${PID} >/dev/null 2>&1; then
			echo Stopping ${CLASS}
			kill ${PID}
			sleep ${TIMEOUT}
			if kill -0 ${PID} >/dev/null 2>&1; then
				echo "Warning: ${CLASS} did not stop gracefully after ${TIMEOUT} seconds: killing with kill -9"
				kill -9 ${PID}
			fi
		else
			echo "Warning: No ${CLASS} to stop"
		fi
		;;
	--help | -h | *)
		print_usage
		exit
		;;
	esac
else
    echo -e "${RED}Error: Please execute ops.sh instead${END}"
fi
