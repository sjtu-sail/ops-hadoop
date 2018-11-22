#!/bin/bash

BOLD="\033[1;37m"
UNDERLINE="\033[4;37m"
RED="\033[31m"
GREEN="\033[32m"
BLUE="\033[34m"
END="\033[0m"

function print_usage() {
	echo -e "${BOLD}OPS: Optimized Shuffle${END}"
	echo
	echo -e "${UNDERLINE}Usage:${END}"
	echo -e "  ${GREEN}ops.sh [command]${END}"
	echo
	echo -e "${UNDERLINE}Commands:${END}"
	echo -e "  ${GREEN}master${END}         OpsMaster"
	echo -e "  ${GREEN}worker${END}         OpsWorker"
	echo -e "  ${GREEN}client${END}         OpsClient"
	echo
	echo -e "${UNDERLINE}Options:${END}"
	echo -e "  ${BLUE}-h, --help${END}     Show usage"
	echo
	echo -e "Use ${BOLD}ops.sh [command] --help${END} for more information about a command."
}

function build() {
	mvn test
	if [[ $? -ne 0 ]]; then
		exit 1
	else
		mvn clean package
	fi
}

if [[ $# == 0 ]]; then
	print_usage
	exit
fi

cd ..

if [[ -f target/ops.jar ]]; then
	case $1 in
	master | worker | client)
		export CLASSPATH=target/ops.jar
		params=($@)
		exec ./scripts/ops-$1.sh ${params[@]:1}
		;;
	--help | -h | *)
		print_usage
		exit
		;;
	esac
fi
