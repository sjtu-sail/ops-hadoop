#!/bin/bash

BOLD="\033[1;37m"
UNDERLINE="\033[4;37m"
RED="\033[31m"
GREEN="\033[32m"
BLUE="\033[34m"
END="\033[0m"

function print_usage() {
	echo -e "${BOLD}OpsClient${END}"
	echo
	echo -e "${UNDERLINE}Usage:${END}"
	echo -e "  ${GREEN}ops.sh client [command]${END}"
	echo
	echo -e "${UNDERLINE}Commands:${END}"
	echo
	echo -e "${UNDERLINE}Options:${END}"
	echo -e "  ${BLUE}-rj, --registerjob${END} ${RED}[jobId] [numMap] [numReduce]${END}"
	echo -e "                 Send a RegisterJob message to master"
	echo -e "  ${BLUE}-tc, --taskcomplete${END} ${RED}[isMap] [taskId] [jobId] [ip] [hostname]${END}"
	echo -e "                 Send a TaskComplete message to master"
	echo -e "  ${BLUE}-h, --help${END}     Show usage"
}

TIMEOUT=5
CLASS=OpsClient

if [[ ${CLASSPATH} ]]; then
	case $1 in
	--registerjob | -rj | --taskcomplete | -tc)
        java -cp ${CLASSPATH} cn.edu.sjtu.ist.ops.${CLASS} $@
		;;
	--help | -h | *)
		print_usage
		exit
		;;
	esac
else
    echo -e "${RED}Error: Please execute ops.sh instead${END}"
fi
