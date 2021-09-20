package systemmetadata

type SystemRole string

const (
	SystemRoleAdmin      SystemRole = "$admins"
	SystemRoleOperations SystemRole = "$ops"
	SystemRoleAll        SystemRole = "$all"
)
