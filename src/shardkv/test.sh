#!/bin/bash
go test -run $1 2>/dev/null | egrep -v 'EOF|connection|broken'
