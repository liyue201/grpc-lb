package registry

type ServiceInfo struct {
	InstanceId string
	Name       string
	Version    string
	Address    string
	Metadata   map[string]string
}

type Registrar interface {
	Register(service *ServiceInfo) error
	Unregister(service *ServiceInfo) error
	Close()
}
