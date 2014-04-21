export GOPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
alias gt="go test 2>/dev/null | egrep -v 'EOF|connection|broken'"
sudo apt-get install sqlite3
