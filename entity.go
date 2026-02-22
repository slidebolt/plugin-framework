package framework

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"github.com/slidebolt/plugin-framework/pkg/script"
	"github.com/slidebolt/plugin-sdk"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
)

type entityImpl struct {
	id        sdk.UUID
	deviceID  sdk.UUID
	bundle    *bundleImpl
	worker    *script.Worker
	sub       *nats.Subscription
	cmdSub    *nats.Subscription
	extraSubs []*nats.Subscription
	metadata  sdk.EntityMetadata
	state     sdk.EntityState
	raw       map[string]interface{}
	mu        sync.RWMutex
}

func (e *entityImpl) MarshalJSON() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return json.Marshal(map[string]any{
		"id":        e.id,
		"deviceID":  e.deviceID,
		"metadata":  e.metadata,
		"state":     e.state.Status, // Compatibility with UI chip logic
		"status":    e.state.Status,
		"fullState": e.state,
		"raw":       e.raw,
		"name":      e.metadata.Name,
		"type":      e.metadata.Type,
	})
}

func (e *entityImpl) ID() sdk.UUID       { return e.id }
func (e *entityImpl) DeviceID() sdk.UUID { return e.deviceID }
func (e *entityImpl) Metadata() sdk.EntityMetadata {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.metadata
}
func (e *entityImpl) State() sdk.EntityState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}
func (e *entityImpl) Raw() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.raw
}
func (e *entityImpl) Script() string {
	prefix := string(e.deviceID) + "." + string(e.id)
	b, _ := ioutil.ReadFile(filepath.Join(e.bundle.statePath, prefix+".script"))
	return string(b)
}
func (e *entityImpl) StateScript() string {
	prefix := string(e.deviceID) + "." + string(e.id)
	b, _ := ioutil.ReadFile(filepath.Join(e.bundle.statePath, prefix+".state.script"))
	return string(b)
}

func (e *entityImpl) UpdateMetadata(name string, sourceID sdk.SourceID) error {
	e.mu.Lock()
	e.metadata.Name = name
	e.metadata.SourceID = sourceID
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{"name": name, "source_id": sourceID})
	return err
}
func (e *entityImpl) setState(enabled bool, status string) error {
	e.mu.Lock()
	oldEnabled := e.state.Enabled
	e.state.Enabled = enabled
	e.state.Status = status
	s := e.state
	e.mu.Unlock()

	if oldEnabled && !enabled {
		if e.worker != nil {
			e.worker.Close()
			e.worker = nil
		}
		if e.sub != nil {
			e.sub.Unsubscribe()
			e.sub = nil
		}
		if e.cmdSub != nil {
			e.cmdSub.Unsubscribe()
			e.cmdSub = nil
		}
		for _, sub := range e.extraSubs {
			sub.Unsubscribe()
		}
		e.extraSubs = nil
	} else if !oldEnabled && enabled {
		e.initWorker()
	}

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".state.json", s)
	e.bundle.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"enabled": enabled, "status": status})
	return err
}

func (e *entityImpl) UpdateState(status string) error {
	return e.setState(true, status)
}

func (e *entityImpl) Disable(status string) error {
	return e.setState(false, status)
}
func (e *entityImpl) UpdateRaw(data map[string]interface{}) error {
	e.mu.Lock()
	e.raw = data
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".raw.json", data)
	e.bundle.Publish(fmt.Sprintf("entity.%s.raw", e.id), data)
	return err
}

func (e *entityImpl) UpdateScript(code string) error {
	if _, err := script.PreCompile(code); err != nil {
		e.bundle.Log().Error("Lua Pre-Compile Error (UpdateScript rejected) [%s]: %v", e.id, err)
		return err
	}
	prefix := string(e.deviceID) + "." + string(e.id)
	err := ioutil.WriteFile(filepath.Join(e.bundle.statePath, prefix+".script"), []byte(code), 0644)
	if err == nil {
		e.initWorker()
	}
	return err
}

func (e *entityImpl) UpdateStateScript(config string) error {
	prefix := string(e.deviceID) + "." + string(e.id)
	return ioutil.WriteFile(filepath.Join(e.bundle.statePath, prefix+".state.script"), []byte(config), 0644)
}

func (e *entityImpl) initWorker() {
	if e.sub != nil {
		e.sub.Unsubscribe()
		e.sub = nil
	}
	if e.cmdSub != nil {
		e.cmdSub.Unsubscribe()
		e.cmdSub = nil
	}

	e.mu.RLock()
	enabled := e.state.Enabled
	e.mu.RUnlock()
	if !enabled {
		return
	}

	for _, sub := range e.extraSubs {
		sub.Unsubscribe()
	}
	e.extraSubs = nil

	if e.worker != nil {
		e.worker.Close()
		e.worker = nil
	}

	code := e.Script()
	if code == "" || code == "-- OnLoad() {}" {
		return
	}

	w, err := script.NewWorker(e.id, code, e.bundle.Log(), e)
	if err == nil {
		e.worker = w
		w.OnLoad()
		e.sub, _ = e.bundle.nc.Subscribe("entity."+string(e.id)+".>", func(m *nats.Msg) {
			if strings.HasSuffix(m.Subject, ".command") {
				return
			}
			var msg sdk.Message
			json.Unmarshal(m.Data, &msg)
			go w.OnEvent(msg)
		})
		e.cmdSub, _ = e.bundle.nc.Subscribe("entity."+string(e.id)+".command", func(m *nats.Msg) {
			var msg sdk.Message
			json.Unmarshal(m.Data, &msg)
			cmd, _ := msg.Payload["command"].(string)
			if cmd != "" {
				w.OnCommand(cmd, msg.Payload)
			}
		})
	} else {
		e.bundle.Log().Error("Lua Compile Error [%s]: %v", e.id, err)
	}
}

// HostBridge
func (e *entityImpl) GetMetadata() map[string]interface{} {
	m := e.Metadata()
	return map[string]interface{}{"id": string(m.ID), "name": m.Name, "source_id": string(m.SourceID)}
}
func (e *entityImpl) GetState() map[string]interface{} {
	s := e.State()
	return map[string]interface{}{"enabled": s.Enabled, "status": s.Status, "phase": s.Phase, "run_id": s.RunID, "progress": s.Progress}
}
func (e *entityImpl) GetRaw() map[string]interface{} { return e.Raw() }
func (e *entityImpl) GetDeviceRaw() map[string]interface{} {
	e.bundle.mu.RLock()
	dev, ok := e.bundle.devices[e.deviceID]
	e.bundle.mu.RUnlock()
	if ok {
		return dev.Raw()
	}
	return nil
}
func (e *entityImpl) Publish(subj string, p map[string]interface{}) error {
	return e.bundle.Publish(subj, p)
}
func (e *entityImpl) Subscribe(topic string) error {
	if e.bundle.nc == nil {
		return fmt.Errorf("no NATS connection")
	}
	sub, err := e.bundle.nc.Subscribe(topic, func(m *nats.Msg) {
		e.mu.RLock()
		w := e.worker
		e.mu.RUnlock()
		if w == nil {
			return
		}
		var msg sdk.Message
		json.Unmarshal(m.Data, &msg)
		if msg.Subject == "" {
			msg.Subject = m.Subject
		}
		go w.OnEvent(msg)
	})
	if err != nil {
		return err
	}
	e.extraSubs = append(e.extraSubs, sub)
	return nil
}

func (e *entityImpl) GetByUUID(id sdk.UUID) (interface{}, bool) { return GetByUUID(id) }
func (e *entityImpl) GetBySourceID(sid sdk.SourceID) (interface{}, bool) {
	return GetBySourceID(sid)
}

func (e *entityImpl) BeginRun(meta map[string]interface{}) (int, error) {
	e.mu.Lock()
	e.state.Enabled = true
	e.state.Phase = "running"
	e.state.RunID++
	e.state.Progress = 0
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	e.bundle.saveJSON(prefix+".state.json", s)
	e.bundle.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"enabled": true, "phase": "running", "run_id": s.RunID})
	return s.RunID, nil
}
func (e *entityImpl) Progress(percent int, meta map[string]interface{}) error {
	e.mu.Lock()
	e.state.Progress = percent
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	e.bundle.saveJSON(prefix+".state.json", s)
	e.bundle.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"progress": percent})
	return nil
}
func (e *entityImpl) Complete(status string, meta map[string]interface{}) error {
	e.mu.Lock()
	e.state.Enabled = false
	e.state.Phase = "idle"
	e.state.Status = status
	e.state.Progress = 100
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	e.bundle.saveJSON(prefix+".state.json", s)
	e.bundle.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"enabled": false, "phase": "idle", "status": status})
	return nil
}
func (e *entityImpl) Fail(reason string, meta map[string]interface{}) error {
	e.mu.Lock()
	e.state.Enabled = false
	e.state.Phase = "idle"
	e.state.Status = "failed: " + reason
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	e.bundle.saveJSON(prefix+".state.json", s)
	e.bundle.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"enabled": false, "phase": "idle", "status": s.Status})
	return nil
}

func (e *entityImpl) OnCommand(handler sdk.CommandHandler) {
	e.bundle.Log().Info("Entity.OnCommand bound for entity %s", e.id)
	if e.bundle.nc == nil {
		return
	}

	e.mu.Lock()
	if e.cmdSub != nil {
		_ = e.cmdSub.Unsubscribe()
	}

	subject := fmt.Sprintf("entity.%s.command", e.id)
	sub, _ := e.bundle.nc.Subscribe(subject, func(msg *nats.Msg) {
		var m sdk.Message
		json.Unmarshal(msg.Data, &m)
		SafeRun(e.bundle.id, "Entity.OnCommand", func() {
			cmd, _ := m.Payload["command"].(string)
			if cmd == "" {
				cmd, _ = m.Payload["action"].(string)
			}
			if cmd != "" {
				handler(cmd, m.Payload)
			}
		})
	})
	e.cmdSub = sub
	e.mu.Unlock()
}

// Specialized Types
type switchImpl struct{ *entityImpl }

func (s *switchImpl) UpdateMetadata(name string, sid sdk.SourceID) error {
	return s.entityImpl.UpdateMetadata(name, sid)
}
func (s *switchImpl) TurnOn() error {
	return s.bundle.Publish(fmt.Sprintf("entity.%s.command", s.id), map[string]interface{}{"command": "TurnOn", "action": "TurnOn"})
}
func (s *switchImpl) TurnOff() error {
	return s.bundle.Publish(fmt.Sprintf("entity.%s.command", s.id), map[string]interface{}{"command": "TurnOff", "action": "TurnOff"})
}
func (s *switchImpl) Toggle() error {
	return s.bundle.Publish(fmt.Sprintf("entity.%s.command", s.id), map[string]interface{}{"command": "Toggle", "action": "Toggle"})
}

type lightImpl struct{ *entityImpl }

func (l *lightImpl) UpdateMetadata(name string, sid sdk.SourceID) error {
	return l.entityImpl.UpdateMetadata(name, sid)
}
func (l *lightImpl) TurnOn() error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "TurnOn", "action": "TurnOn"})
}
func (l *lightImpl) TurnOff() error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "TurnOff", "action": "TurnOff"})
}
func (l *lightImpl) Toggle() error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "Toggle", "action": "Toggle"})
}
func (l *lightImpl) SetBrightness(level int) error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "SetBrightness", "level": level})
}
func (l *lightImpl) SetRGB(r, g, b int) error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "SetRGB", "r": r, "g": g, "b": b})
}
func (l *lightImpl) SetTemperature(k int) error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "SetTemperature", "kelvin": k})
}
func (l *lightImpl) SetScene(s string) error {
	return l.bundle.Publish(fmt.Sprintf("entity.%s.command", l.id), map[string]interface{}{"command": "SetScene", "scene": s})
}

type sensorImpl struct{ *entityImpl }
type binarySensorImpl struct{ *entityImpl }

type coverImpl struct{ *entityImpl }

func (c *coverImpl) Open() error {
	return c.bundle.Publish(fmt.Sprintf("entity.%s.command", c.id), map[string]interface{}{"command": "Open", "action": "Open"})
}
func (c *coverImpl) Close() error {
	return c.bundle.Publish(fmt.Sprintf("entity.%s.command", c.id), map[string]interface{}{"command": "Close", "action": "Close"})
}
func (c *coverImpl) Stop() error {
	return c.bundle.Publish(fmt.Sprintf("entity.%s.command", c.id), map[string]interface{}{"command": "Stop", "action": "Stop"})
}
func (c *coverImpl) SetPosition(pos int) error {
	return c.bundle.Publish(fmt.Sprintf("entity.%s.command", c.id), map[string]interface{}{"command": "SetPosition", "position": pos})
}

type cameraImpl struct{ *entityImpl }

func (c *cameraImpl) Snapshot() ([]byte, error) {
	// Logic would go here
	return nil, nil
}
func (c *cameraImpl) Stream() (string, error) {
	return "", nil
}
