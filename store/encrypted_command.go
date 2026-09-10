package store

// ValidEncryptedCommandType is the bounded durable command vocabulary. It does
// not authorize execution; endpoint grants and signatures remain authoritative.
func ValidEncryptedCommandType(kind string) bool {
	switch kind {
	case "session.send", "session.interrupt", "session.stop", "permission.respond", "session.settings.change", "session.membership.change", "session.file.read", "session.file.list":
		return true
	default:
		return false
	}
}
