package kvstore

import (
	"hash/adler32"
	"syscall"
	"time"
	"unsafe"
)

// getTimeCoarse uses the CLOCK_REALTIME_COURSE syscall.
// This is way faster than time.Now().  The precision is only millisecondish, which is way good enough for this purpose
func getTimeCoarse() time.Time {
	tspec := syscall.Timespec{}
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, 5, uintptr(unsafe.Pointer(&tspec)), 0)
	sec, nsec := tspec.Unix()
	return time.Unix(sec, nsec).UTC()
}

// qhash uses adler32 for a checksum and returns a modulo of number of stores available
func qhash(in []byte, num uint32) uint32 {
	return adler32.Checksum(in) % num
}
