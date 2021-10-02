package server

type Config struct {
	Delimiter byte
	Port uint32
	Network string // "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
}
