package framework

import (
	"github.com/slidebolt/plugin-sdk"
)

type frameworkRegistry struct{}

func (r *frameworkRegistry) RegisterBundle(id string) (sdk.Bundle, error) {
	return RegisterBundle(id)
}

func (r *frameworkRegistry) GetByUUID(id sdk.UUID) (interface{}, bool) {
	return GetByUUID(id)
}

func (r *frameworkRegistry) GetBySourceID(sid sdk.SourceID) (interface{}, bool) {
	return GetBySourceID(sid)
}

func (r *frameworkRegistry) GetByLabel(labels interface{}) []interface{} {
	return GetByLabel(labels)
}

func (r *frameworkRegistry) GetDevices() []sdk.Device {
	return GetDevices()
}

func init() {
	sdk.GlobalRegistry = &frameworkRegistry{}
}
