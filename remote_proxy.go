package framework

import (
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/slidebolt/plugin-sdk"
)

type remoteProxyBase struct {
	id       sdk.UUID
	deviceID sdk.UUID
	bundle   *bundleImpl
}

func (p *remoteProxyBase) ID() sdk.UUID       { return p.id }
func (p *remoteProxyBase) DeviceID() sdk.UUID { return p.deviceID }
func (p *remoteProxyBase) Raw() map[string]interface{} {
	return nil
}
func (p *remoteProxyBase) Script() string      { return "" }
func (p *remoteProxyBase) StateScript() string { return "" }

func (p *remoteProxyBase) publish(subject string, payload map[string]interface{}) error {
	if p.bundle == nil || p.bundle.nc == nil {
		return fmt.Errorf("remote proxy publish failed: bundle transport unavailable")
	}
	msg := sdk.Message{
		Source:    p.bundle.id,
		DeviceID:  p.deviceID,
		EntityID:  p.id,
		Subject:   subject,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
	if msg.Payload == nil {
		msg.Payload = make(map[string]interface{})
	}
	data, _ := json.Marshal(msg)
	return p.bundle.nc.Publish(subject, data)
}

func (p *remoteProxyBase) sendCommand(kind string, cmd string, payload map[string]interface{}) error {
	subject := fmt.Sprintf("%s.%s.command", kind, p.id)
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["command"] = cmd
	if p.bundle != nil {
		p.bundle.Log().Info("[PROXY] Sending command %s to %s", cmd, subject)
	}
	return p.publish(subject, payload)
}

func remoteUnsupported(op string, id sdk.UUID) error {
	return fmt.Errorf("%s unsupported on remote proxy %s", op, id)
}

type remoteDeviceProxy struct {
	remoteProxyBase
	metadata sdk.DeviceMetadata
	state    sdk.DeviceState
}

func (p *remoteDeviceProxy) BundleID() sdk.UUID { return "" }
func (p *remoteDeviceProxy) Metadata() sdk.DeviceMetadata {
	return p.metadata
}
func (p *remoteDeviceProxy) State() sdk.DeviceState {
	return p.state
}
func (p *remoteDeviceProxy) Publish(subject string, payload map[string]interface{}) error {
	if p.bundle != nil {
		p.bundle.Log().Info("[PROXY] Publish %s", subject)
	}
	return p.publish(subject, payload)
}
func (p *remoteDeviceProxy) UpdateMetadata(string, sdk.SourceID) error {
	return remoteUnsupported("UpdateMetadata", p.id)
}
func (p *remoteDeviceProxy) UpdateState(string) error { return remoteUnsupported("UpdateState", p.id) }
func (p *remoteDeviceProxy) UpdateProperties(map[string]interface{}) error {
	return remoteUnsupported("UpdateProperties", p.id)
}
func (p *remoteDeviceProxy) SetProperties(map[string]interface{}) error {
	return remoteUnsupported("SetProperties", p.id)
}
func (p *remoteDeviceProxy) AddLabel(string) error { return remoteUnsupported("AddLabel", p.id) }
func (p *remoteDeviceProxy) RemoveLabel(string) error { return remoteUnsupported("RemoveLabel", p.id) }
func (p *remoteDeviceProxy) SetLabels([]string) error { return remoteUnsupported("SetLabels", p.id) }
func (p *remoteDeviceProxy) Disable(string) error     { return remoteUnsupported("Disable", p.id) }
func (p *remoteDeviceProxy) UpdateRaw(map[string]interface{}) error {
	return remoteUnsupported("UpdateRaw", p.id)
}
func (p *remoteDeviceProxy) UpdateScript(string) error {
	return remoteUnsupported("UpdateScript", p.id)
}
func (p *remoteDeviceProxy) UpdateStateScript(string) error {
	return remoteUnsupported("UpdateStateScript", p.id)
}
func (p *remoteDeviceProxy) CreateEntity(sdk.EntityType) (sdk.Entity, error) {
	return nil, remoteUnsupported("CreateEntity", p.id)
}
func (p *remoteDeviceProxy) CreateEntityEx(sdk.EntityType, []string) (sdk.Entity, error) {
	return nil, remoteUnsupported("CreateEntityEx", p.id)
}
func (p *remoteDeviceProxy) GetEntities() ([]sdk.Entity, error) {
	return nil, remoteUnsupported("GetEntities", p.id)
}
func (p *remoteDeviceProxy) GetBySourceID(sdk.SourceID) (interface{}, bool) { return nil, false }
func (p *remoteDeviceProxy) DeleteEntity(sdk.UUID) error {
	return remoteUnsupported("DeleteEntity", p.id)
}
func (p *remoteDeviceProxy) OnCommand(sdk.CommandHandler) {}
func (p *remoteDeviceProxy) SendCommand(cmd string, payload map[string]interface{}) error {
	return p.sendCommand("device", cmd, payload)
}

type remoteEntityProxy struct {
	remoteProxyBase
	metadata sdk.EntityMetadata
	state    sdk.EntityState
}

func (p *remoteEntityProxy) Metadata() sdk.EntityMetadata {
	return p.metadata
}
func (p *remoteEntityProxy) State() sdk.EntityState {
	return p.state
}
func (p *remoteEntityProxy) Publish(subject string, payload map[string]interface{}) error {
	if p.bundle != nil {
		p.bundle.Log().Info("[PROXY] Publish %s", subject)
	}
	return p.publish(subject, payload)
}
func (p *remoteEntityProxy) UpdateMetadata(string, sdk.SourceID) error {
	return remoteUnsupported("UpdateMetadata", p.id)
}
func (p *remoteEntityProxy) UpdateCapabilities([]string) error {
	return remoteUnsupported("UpdateCapabilities", p.id)
}
func (p *remoteEntityProxy) UpdateState(string) error { return remoteUnsupported("UpdateState", p.id) }
func (p *remoteEntityProxy) UpdateProperties(map[string]interface{}) error {
	return remoteUnsupported("UpdateProperties", p.id)
}
func (p *remoteEntityProxy) SetProperties(map[string]interface{}) error {
	return remoteUnsupported("SetProperties", p.id)
}
func (p *remoteEntityProxy) AddLabel(string) error { return remoteUnsupported("AddLabel", p.id) }
func (p *remoteEntityProxy) RemoveLabel(string) error { return remoteUnsupported("RemoveLabel", p.id) }
func (p *remoteEntityProxy) SetLabels([]string) error { return remoteUnsupported("SetLabels", p.id) }
func (p *remoteEntityProxy) Disable(string) error     { return remoteUnsupported("Disable", p.id) }
func (p *remoteEntityProxy) UpdateRaw(map[string]interface{}) error {
	return remoteUnsupported("UpdateRaw", p.id)
}
func (p *remoteEntityProxy) UpdateScript(string) error {
	return remoteUnsupported("UpdateScript", p.id)
}
func (p *remoteEntityProxy) UpdateStateScript(string) error {
	return remoteUnsupported("UpdateStateScript", p.id)
}
func (p *remoteEntityProxy) OnCommand(sdk.CommandHandler) {}
func (p *remoteEntityProxy) SendCommand(cmd string, payload map[string]interface{}) error {
	return p.sendCommand("entity", cmd, payload)
}

func (p *remoteEntityProxy) TurnOn() error  { return p.SendCommand("TurnOn", nil) }
func (p *remoteEntityProxy) TurnOff() error { return p.SendCommand("TurnOff", nil) }
func (p *remoteEntityProxy) Toggle() error  { return p.SendCommand("Toggle", nil) }
func (p *remoteEntityProxy) SetBrightness(level int) error {
	return p.SendCommand("SetBrightness", map[string]interface{}{"level": level})
}
func (p *remoteEntityProxy) SetRGB(r, g, b int) error {
	return p.SendCommand("SetRGB", map[string]interface{}{"r": r, "g": g, "b": b})
}
func (p *remoteEntityProxy) SetTemperature(k int) error {
	return p.SendCommand("SetTemperature", map[string]interface{}{"kelvin": k})
}
func (p *remoteEntityProxy) SetScene(s string) error {
	return p.SendCommand("SetScene", map[string]interface{}{"scene": s})
}
func (p *remoteEntityProxy) Snapshot() ([]byte, error) {
	return nil, remoteUnsupported("Snapshot", p.id)
}
func (p *remoteEntityProxy) Stream() (string, error) { return "", remoteUnsupported("Stream", p.id) }
func (p *remoteEntityProxy) Open() error             { return p.SendCommand("Open", nil) }
func (p *remoteEntityProxy) Close() error            { return p.SendCommand("Close", nil) }
func (p *remoteEntityProxy) Stop() error             { return p.SendCommand("Stop", nil) }
func (p *remoteEntityProxy) SetPosition(pos int) error {
	return p.SendCommand("SetPosition", map[string]interface{}{"position": pos})
}
