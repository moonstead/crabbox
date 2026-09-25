//go:build !windows

package cli

import "syscall"

// setExecStatusCloseOnExec keeps an inherited status descriptor out of the
// SSH transport and any other process exec starts.
func setExecStatusCloseOnExec(fd int) {
	syscall.CloseOnExec(fd)
}
