export GOPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
alias gt="go test 2>/dev/null | egrep -v 'EOF|connection|broken'"
go get github.com/mattn/go-sqlite3
