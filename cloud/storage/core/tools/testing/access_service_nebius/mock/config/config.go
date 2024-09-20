package config

////////////////////////////////////////////////////////////////////////////////

type Permission struct {
	Permission string `json:"permission,omitempty"`
	Resource   string `json:"resource,omitempty"`
}

type AccountConfig struct {
	Permissions      []Permission `json:"permissions,omitempty"`
	Id               string       `json:"id,omitempty"`
	IsUnknownSubject bool         `json:"is_unknown_subject,omitempty"`
	Token            string       `json:"token,omitempty"`
}

type MockConfig struct {
	Port        int             `json:"port,omitempty"`
	CertFile    string          `json:"cert_file,omitempty"`
	CertKeyFile string          `json:"cert_key_file,omitempty"`
	Accounts    []AccountConfig `json:"accounts" json:"accounts,omitempty"`
}
