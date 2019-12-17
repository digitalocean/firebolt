#!/bin/bash
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "example name is required, $# arguments provided"
docker-compose up -d --force-recreate
go run main.go "$@"
docker-compose down -v
