package framework

import (
	"encoding/json"
	"fmt"
	"github.com/slidebolt/plugin-framework/pkg/script"
	"github.com/slidebolt/plugin-sdk"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type entityImpl struct {
	id               sdk.UUID
	deviceID         sdk.UUID
	bundle           *bundleImpl
	worker           *script.Worker
	sub              *nats.Subscription
	cmdSub           *nats.Subscription
	handlers         []sdk.CommandHandler
	scriptCmdHandler sdk.CommandHandler
	extraSubs        []*nats.Subscription
	metadata         sdk.EntityMetadata
	state            sdk.EntityState
	raw              map[string]interface{}
	mu               sync.RWMutex
}

func (e *entityImpl) MarshalJSON() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Ensure the interface property is set based on metadata
	st := e.state
	st.Interface = strings.ToLower(string(e.metadata.Type))

	return json.Marshal(map[string]any{
		"id":       e.id,
		"deviceID": e.deviceID,
		"metadata": e.metadata,
		"state":    st,
		"raw":      e.raw,
		"name":     e.metadata.Name,
		"type":     e.metadata.Type,
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

func (e *entityImpl) recalculateName() {
	if e.metadata.LocalName != "" {
		e.metadata.Name = e.metadata.LocalName
	} else if e.metadata.SourceName != "" {
		e.metadata.Name = e.metadata.SourceName
	} else if e.metadata.SourceID != "" {
		e.metadata.Name = string(e.metadata.SourceID)
	} else {
		e.metadata.Name = string(e.id)
	}
}

func (e *entityImpl) UpdateMetadata(name string, sourceID sdk.SourceID) error {
	e.mu.Lock()
	e.metadata.SourceName = name
	e.metadata.SourceID = sourceID
	e.recalculateName()
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{
		"name":        m.Name,
		"local_name":  m.LocalName,
		"source_name": m.SourceName,
		"source_id":   m.SourceID,
	})
	return err
}

func (e *entityImpl) UpdateLocalName(name string) error {
	e.mu.Lock()
	e.metadata.LocalName = name
	e.recalculateName()
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{
		"name":       m.Name,
		"local_name": m.LocalName,
	})
	return err
}

func (e *entityImpl) UpdateSourceName(name string) error {
	e.mu.Lock()
	e.metadata.SourceName = name
	e.recalculateName()
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{
		"name":        m.Name,
		"source_name": m.SourceName,
	})
	return err
}

func (e *entityImpl) AddLabel(label string) error {
	e.mu.Lock()
	found := false
	for _, l := range e.metadata.Labels {
		if l == label {
			found = true
			break
		}
	}
	if !found {
		e.metadata.Labels = append(e.metadata.Labels, label)
	}
	m := e.metadata
	e.mu.Unlock()

	if found {
		return nil
	}

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{"labels": m.Labels})
	return err
}

func (e *entityImpl) RemoveLabel(label string) error {
	e.mu.Lock()
	newLabels := []string{}
	found := false
	for _, l := range e.metadata.Labels {
		if l == label {
			found = true
			continue
		}
		newLabels = append(newLabels, l)
	}
	e.metadata.Labels = newLabels
	m := e.metadata
	e.mu.Unlock()

	if !found {
		return nil
	}

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{"labels": m.Labels})
	return err
}

func (e *entityImpl) SetLabels(labels []string) error {
	e.mu.Lock()
	e.metadata.Labels = labels
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{"labels": labels})
	return err
}

func (e *entityImpl) UpdateCapabilities(caps []string) error {
	e.mu.Lock()
	e.metadata.Capabilities = caps
	m := e.metadata
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".json", m)
	e.bundle.Publish(fmt.Sprintf("entity.%s.metadata", e.id), map[string]interface{}{"capabilities": caps})
	return err
}
func (e *entityImpl) setState(enabled bool, status string) error {
	e.mu.Lock()
	oldEnabled := e.state.Enabled
	e.state.Enabled = enabled
	e.state.Status = status
	e.state.Interface = strings.ToLower(string(e.metadata.Type))
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
	e.Publish(fmt.Sprintf("entity.%s.state", e.id), map[string]interface{}{"enabled": enabled, "status": status})
	return err
}

func (e *entityImpl) UpdateState(status string) error {
	return e.setState(true, status)
}

func (e *entityImpl) UpdateProperties(props map[string]interface{}) error {
	e.mu.Lock()
	if e.state.Properties == nil {
		e.state.Properties = make(map[string]interface{})
	}
	for k, v := range props {
		e.state.Properties[k] = v
	}
	e.state.Interface = strings.ToLower(string(e.metadata.Type))
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".state.json", s)
	// Important: We publish the full state so the UI gets everything
	e.Publish(fmt.Sprintf("entity.%s.state", e.id), props)
	return err
}

func (e *entityImpl) SetProperties(props map[string]interface{}) error {
	e.mu.Lock()
	e.state.Properties = props
	e.state.Interface = strings.ToLower(string(e.metadata.Type))
	s := e.state
	e.mu.Unlock()

	prefix := string(e.deviceID) + "." + string(e.id)
	err := e.bundle.saveJSON(prefix+".state.json", s)
	e.Publish(fmt.Sprintf("entity.%s.state", e.id), props)
	return err
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
	e.mu.Lock()
	e.scriptCmdHandler = nil
	e.mu.Unlock()

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
		e.mu.Lock()
		e.scriptCmdHandler = func(cmd string, payload map[string]interface{}) {
			w.OnCommand(cmd, payload)
		}
		e.ensureCommandSubscriptionLocked()
		e.mu.Unlock()
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
	if e.bundle.nc == nil {
		return nil
	}
	msg := sdk.Message{
		Source:    e.bundle.id,
		DeviceID:  e.deviceID,
		EntityID:  e.id,
		Subject:   subj,
		Payload:   p,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(msg)
	return e.bundle.nc.Publish(subj, data)
}
func (e *entityImpl) SendCommand(cmd string, payload map[string]interface{}) error {
	if e == nil || e.bundle == nil || e.bundle.nc == nil {
		id := sdk.UUID("")
		if e != nil {
			id = e.id
		}
		return fmt.Errorf("entity %s command transport unavailable", id)
	}
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["command"] = cmd
	return e.Publish("entity."+string(e.id)+".command", payload)
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
	dev, err := e.bundle.GetDevice(e.deviceID)
	if err != nil {
		return nil, false
	}
	return dev.GetBySourceID(sid)
}
func (e *entityImpl) GetByLabel(labels interface{}) []interface{} {
	return GetByLabel(labels)
}
func (e *entityImpl) GetRemoteObject(id sdk.UUID) (interface{}, bool) {
	return e.bundle.GetRemoteObject(id)
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
	e.bundle.Log().Info("Entity.OnCommand bound for entity %s (total handlers: %d)", e.id, len(e.handlers)+1)
	if e.bundle.nc == nil {
		return
	}

	e.mu.Lock()
	e.handlers = append(e.handlers, handler)
	e.ensureCommandSubscriptionLocked()
	e.mu.Unlock()
}

func (e *entityImpl) ensureCommandSubscriptionLocked() {
	if e.bundle == nil || e.bundle.nc == nil || e.cmdSub != nil {
		return
	}
	subject := fmt.Sprintf("entity.%s.command", e.id)
	e.bundle.Log().Info("[CMDSUB] SUBSCRIBE entity command subject=%s", subject)
	sub, _ := e.bundle.nc.Subscribe(subject, func(msg *nats.Msg) {
		e.bundle.Log().Info("[CMDSUB] RECV entity command subject=%s bytes=%d", msg.Subject, len(msg.Data))
		var m sdk.Message
		json.Unmarshal(msg.Data, &m)
		SafeRun(e.bundle.id, "Entity.OnCommand", func() {
			cmd, _ := m.Payload["command"].(string)
			if cmd == "" {
				cmd, _ = m.Payload["action"].(string)
			}
			if cmd != "" {
				e.mu.RLock()
				userHandlers := make([]sdk.CommandHandler, len(e.handlers))
				copy(userHandlers, e.handlers)
				scriptHandler := e.scriptCmdHandler
				e.mu.RUnlock()
				handlers := make([]sdk.CommandHandler, 0, len(userHandlers)+1)
				if scriptHandler != nil {
					handlers = append(handlers, scriptHandler)
				}
				handlers = append(handlers, userHandlers...)

				e.bundle.Log().Info("Entity %s dispatching command %s to %d handlers", e.id, cmd, len(handlers))
				for _, h := range handlers {
					h(cmd, m.Payload)
				}
			}
		})
	})
	e.cmdSub = sub
}

// Specialized Types
type switchImpl struct{ *entityImpl }

func (s *switchImpl) UpdateMetadata(name string, sid sdk.SourceID) error {
	return s.entityImpl.UpdateMetadata(name, sid)
}
func (s *switchImpl) TurnOn() error {
	return s.entityImpl.SendCommand("TurnOn", map[string]interface{}{"action": "TurnOn"})
}
func (s *switchImpl) TurnOff() error {
	return s.entityImpl.SendCommand("TurnOff", map[string]interface{}{"action": "TurnOff"})
}
func (s *switchImpl) Toggle() error {
	return s.entityImpl.SendCommand("Toggle", map[string]interface{}{"action": "Toggle"})
}

type lightImpl struct{ *entityImpl }

func (l *lightImpl) UpdateMetadata(name string, sid sdk.SourceID) error {
	return l.entityImpl.UpdateMetadata(name, sid)
}
func (l *lightImpl) TurnOn() error {
	return l.entityImpl.SendCommand("TurnOn", map[string]interface{}{"action": "TurnOn"})
}
func (l *lightImpl) TurnOff() error {
	return l.entityImpl.SendCommand("TurnOff", map[string]interface{}{"action": "TurnOff"})
}
func (l *lightImpl) Toggle() error {
	return l.entityImpl.SendCommand("Toggle", map[string]interface{}{"action": "Toggle"})
}
func (l *lightImpl) SetBrightness(level int) error {
	return l.entityImpl.SendCommand("SetBrightness", map[string]interface{}{"level": level})
}
func (l *lightImpl) SetRGB(r, g, b int) error {
	return l.entityImpl.SendCommand("SetRGB", map[string]interface{}{"r": r, "g": g, "b": b})
}
func (l *lightImpl) SetTemperature(k int) error {
	return l.entityImpl.SendCommand("SetTemperature", map[string]interface{}{"kelvin": k})
}
func (l *lightImpl) SetScene(s string) error {
	return l.entityImpl.SendCommand("SetScene", map[string]interface{}{"scene": s})
}

type sensorImpl struct{ *entityImpl }
type binarySensorImpl struct{ *entityImpl }

type coverImpl struct{ *entityImpl }

func (c *coverImpl) Open() error {
	return c.entityImpl.SendCommand("Open", map[string]interface{}{"action": "Open"})
}
func (c *coverImpl) Close() error {
	return c.entityImpl.SendCommand("Close", map[string]interface{}{"action": "Close"})
}
func (c *coverImpl) Stop() error {
	return c.entityImpl.SendCommand("Stop", map[string]interface{}{"action": "Stop"})
}
func (c *coverImpl) SetPosition(pos int) error {
	return c.entityImpl.SendCommand("SetPosition", map[string]interface{}{"position": pos})
}

type cameraImpl struct{ *entityImpl }

func (c *cameraImpl) Snapshot() ([]byte, error) {
	// Logic would go here
	return nil, nil
}
func (c *cameraImpl) Stream() (string, error) {
	return "", nil
}
