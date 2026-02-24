package framework

import (
	"encoding/json"
	"fmt"
	"github.com/slidebolt/plugin-framework/pkg/script"
	"github.com/slidebolt/plugin-sdk"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type deviceImpl struct {
	id               sdk.UUID
	bundle           *bundleImpl
	worker           *script.Worker
	sub              *nats.Subscription
	cmdSub           *nats.Subscription
	handlers         []sdk.CommandHandler
	scriptCmdHandler sdk.CommandHandler
	extraSubs        []*nats.Subscription
	metadata         sdk.DeviceMetadata
	state            sdk.DeviceState
	raw              map[string]interface{}
	mu               sync.RWMutex
}

func (d *deviceImpl) MarshalJSON() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Fetch all entities for this device to include in the payload
	entities := make(map[sdk.UUID]sdk.Entity)
	for _, eid := range d.metadata.Entities {
		if ent, ok := d.bundle.entities[eid]; ok {
			// We use the typed wrapper
			obj, _ := d.bundle.createEntityObject(ent.id, d.id)
			entities[eid] = obj
		}
	}

	st := d.state
	st.Interface = "device"

	return json.Marshal(map[string]any{
		"uuid":     d.id,
		"bundle":   d.bundle.id,
		"metadata": d.metadata,
		"state":    st,
		"entities": entities,
		"raw":      d.raw,
		"name":     d.metadata.Name, // Helper for UI
		"type":     "device",
	})
}

func (d *deviceImpl) ID() sdk.UUID       { return d.id }
func (d *deviceImpl) BundleID() sdk.UUID { return d.bundle.id }
func (d *deviceImpl) Metadata() sdk.DeviceMetadata {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.metadata
}
func (d *deviceImpl) State() sdk.DeviceState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}
func (d *deviceImpl) Raw() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.raw
}
func (d *deviceImpl) Script() string {
	b, _ := ioutil.ReadFile(filepath.Join(d.bundle.statePath, string(d.id)+".script"))
	return string(b)
}
func (d *deviceImpl) StateScript() string {
	b, _ := ioutil.ReadFile(filepath.Join(d.bundle.statePath, string(d.id)+".state.script"))
	return string(b)
}

func (d *deviceImpl) recalculateName() {
	if d.metadata.LocalName != "" {
		d.metadata.Name = d.metadata.LocalName
	} else if d.metadata.SourceName != "" {
		d.metadata.Name = d.metadata.SourceName
	} else if d.metadata.SourceID != "" {
		d.metadata.Name = string(d.metadata.SourceID)
	} else {
		d.metadata.Name = string(d.id)
	}
}

func (d *deviceImpl) UpdateMetadata(name string, sourceID sdk.SourceID) error {
	d.mu.Lock()
	d.metadata.SourceName = name
	d.metadata.SourceID = sourceID
	d.recalculateName()
	m := d.metadata
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{
		"name":        m.Name,
		"local_name":  m.LocalName,
		"source_name": m.SourceName,
		"source_id":   m.SourceID,
	})
	return err
}

func (d *deviceImpl) UpdateLocalName(name string) error {
	d.mu.Lock()
	d.metadata.LocalName = name
	d.recalculateName()
	m := d.metadata
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{
		"name":       m.Name,
		"local_name": m.LocalName,
	})
	return err
}

func (d *deviceImpl) UpdateSourceName(name string) error {
	d.mu.Lock()
	d.metadata.SourceName = name
	d.recalculateName()
	m := d.metadata
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{
		"name":        m.Name,
		"source_name": m.SourceName,
	})
	return err
}

func (d *deviceImpl) AddLabel(label string) error {
	d.mu.Lock()
	found := false
	for _, l := range d.metadata.Labels {
		if l == label {
			found = true
			break
		}
	}
	if !found {
		d.metadata.Labels = append(d.metadata.Labels, label)
	}
	m := d.metadata
	d.mu.Unlock()

	if found {
		return nil
	}

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{"labels": m.Labels})
	return err
}

func (d *deviceImpl) RemoveLabel(label string) error {
	d.mu.Lock()
	newLabels := []string{}
	found := false
	for _, l := range d.metadata.Labels {
		if l == label {
			found = true
			continue
		}
		newLabels = append(newLabels, l)
	}
	d.metadata.Labels = newLabels
	m := d.metadata
	d.mu.Unlock()

	if !found {
		return nil
	}

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{"labels": m.Labels})
	return err
}

func (d *deviceImpl) SetLabels(labels []string) error {
	d.mu.Lock()
	d.metadata.Labels = labels
	m := d.metadata
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".json", m)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{"labels": labels})
	return err
}
func (d *deviceImpl) setState(enabled bool, status string) error {
	d.mu.Lock()
	oldEnabled := d.state.Enabled
	d.state.Enabled = enabled
	d.state.Status = status
	d.state.Interface = "device"
	s := d.state
	d.mu.Unlock()

	if oldEnabled && !enabled {
		// Just disabled
		if d.worker != nil {
			d.worker.Close()
			d.worker = nil
		}
		if d.sub != nil {
			d.sub.Unsubscribe()
			d.sub = nil
		}
		if d.cmdSub != nil {
			d.cmdSub.Unsubscribe()
			d.cmdSub = nil
		}
		for _, sub := range d.extraSubs {
			sub.Unsubscribe()
		}
		d.extraSubs = nil
	} else if !oldEnabled && enabled {
		// Just enabled
		d.initWorker()
	}

	err := d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.bundle.Publish(fmt.Sprintf("device.%s.state", d.id), map[string]interface{}{"enabled": enabled, "status": status})
	return err
}

func (d *deviceImpl) UpdateState(status string) error {
	return d.setState(true, status)
}

func (d *deviceImpl) UpdateProperties(props map[string]interface{}) error {
	d.mu.Lock()
	if d.state.Properties == nil {
		d.state.Properties = make(map[string]interface{})
	}
	for k, v := range props {
		d.state.Properties[k] = v
	}
	d.state.Interface = "device"
	s := d.state
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.Publish(fmt.Sprintf("device.%s.state", d.id), props)
	return err
}

func (d *deviceImpl) SetProperties(props map[string]interface{}) error {
	d.mu.Lock()
	d.state.Properties = props
	d.state.Interface = "device"
	s := d.state
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.Publish(fmt.Sprintf("device.%s.state", d.id), props)
	return err
}

func (d *deviceImpl) Disable(status string) error {
	return d.setState(false, status)
}
func (d *deviceImpl) UpdateRaw(data map[string]interface{}) error {
	d.mu.Lock()
	d.raw = data
	d.mu.Unlock()

	err := d.bundle.saveJSON(string(d.id)+".raw.json", data)
	d.bundle.Publish(fmt.Sprintf("device.%s.raw", d.id), data)
	return err
}

func (d *deviceImpl) UpdateScript(code string) error {
	if _, err := script.PreCompile(code); err != nil {
		d.bundle.Log().Error("Lua Pre-Compile Error (UpdateScript rejected) [%s]: %v", d.id, err)
		return err
	}
	err := ioutil.WriteFile(filepath.Join(d.bundle.statePath, string(d.id)+".script"), []byte(code), 0644)
	if err == nil {
		d.initWorker()
	}
	return err
}

func (d *deviceImpl) UpdateStateScript(config string) error {
	return ioutil.WriteFile(filepath.Join(d.bundle.statePath, string(d.id)+".state.script"), []byte(config), 0644)
}

func (d *deviceImpl) initWorker() {
	if d.sub != nil {
		d.sub.Unsubscribe()
		d.sub = nil
	}
	if d.cmdSub != nil {
		d.cmdSub.Unsubscribe()
		d.cmdSub = nil
	}

	d.mu.RLock()
	enabled := d.state.Enabled
	d.mu.RUnlock()
	if !enabled {
		return
	}

	for _, sub := range d.extraSubs {
		sub.Unsubscribe()
	}
	d.extraSubs = nil

	if d.worker != nil {
		d.worker.Close()
		d.worker = nil
	}
	d.mu.Lock()
	d.scriptCmdHandler = nil
	d.mu.Unlock()

	code := d.Script()
	if code == "" || code == "-- OnLoad() {}" {
		return
	}

	w, err := script.NewWorker(d.id, code, d.bundle.Log(), d)
	if err == nil {
		d.worker = w
		w.OnLoad()
		d.sub, _ = d.bundle.nc.Subscribe("device."+string(d.id)+".>", func(m *nats.Msg) {
			if strings.HasSuffix(m.Subject, ".command") {
				return
			}
			var msg sdk.Message
			json.Unmarshal(m.Data, &msg)
			go w.OnEvent(msg)
		})
		d.mu.Lock()
		d.scriptCmdHandler = func(cmd string, payload map[string]interface{}) {
			w.OnCommand(cmd, payload)
		}
		d.ensureCommandSubscriptionLocked()
		d.mu.Unlock()
	} else {
		d.bundle.Log().Error("Lua Compile Error [%s]: %v", d.id, err)
	}
}

// HostBridge
func (d *deviceImpl) GetMetadata() map[string]interface{} {
	m := d.Metadata()
	return map[string]interface{}{"id": string(m.ID), "name": m.Name, "source_id": string(m.SourceID)}
}
func (d *deviceImpl) GetState() map[string]interface{} {
	s := d.State()
	return map[string]interface{}{"enabled": s.Enabled, "status": s.Status, "phase": s.Phase, "run_id": s.RunID, "progress": s.Progress}
}
func (d *deviceImpl) GetRaw() map[string]interface{}       { return d.Raw() }
func (d *deviceImpl) GetDeviceRaw() map[string]interface{} { return d.Raw() }
func (d *deviceImpl) Publish(subj string, p map[string]interface{}) error {
	if d.bundle.nc == nil {
		return nil
	}
	msg := sdk.Message{
		Source:    d.bundle.id,
		DeviceID:  d.id,
		Subject:   subj,
		Payload:   p,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(msg)
	return d.bundle.nc.Publish(subj, data)
}
func (d *deviceImpl) SendCommand(cmd string, payload map[string]interface{}) error {
	if d == nil || d.bundle == nil || d.bundle.nc == nil {
		id := sdk.UUID("")
		if d != nil {
			id = d.id
		}
		return fmt.Errorf("device %s command transport unavailable", id)
	}
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["command"] = cmd
	return d.Publish("device."+string(d.id)+".command", payload)
}
func (d *deviceImpl) Subscribe(topic string) error {
	if d.bundle.nc == nil {
		return fmt.Errorf("no NATS connection")
	}
	sub, err := d.bundle.nc.Subscribe(topic, func(m *nats.Msg) {
		d.mu.RLock()
		w := d.worker
		d.mu.RUnlock()
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
	d.extraSubs = append(d.extraSubs, sub)
	return nil
}

func (d *deviceImpl) GetByUUID(id sdk.UUID) (interface{}, bool) { return GetByUUID(id) }
func (d *deviceImpl) GetBySourceID(sid sdk.SourceID) (interface{}, bool) {
	return d.bundle.GetBySourceID(sid)
}
func (d *deviceImpl) GetByLabel(labels interface{}) []interface{} {
	return GetByLabel(labels)
}
func (d *deviceImpl) GetRemoteObject(id sdk.UUID) (interface{}, bool) {
	return d.bundle.GetRemoteObject(id)
}

func (d *deviceImpl) BeginRun(meta map[string]interface{}) (int, error) {
	d.mu.Lock()
	d.state.Enabled = true
	d.state.Phase = "running"
	d.state.RunID++
	d.state.Progress = 0
	s := d.state
	d.mu.Unlock()

	d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.bundle.Publish(fmt.Sprintf("device.%s.state", d.id), map[string]interface{}{"enabled": true, "phase": "running", "run_id": s.RunID})
	return s.RunID, nil
}
func (d *deviceImpl) Progress(percent int, meta map[string]interface{}) error {
	d.mu.Lock()
	d.state.Progress = percent
	s := d.state
	d.mu.Unlock()

	d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.bundle.Publish(fmt.Sprintf("device.%s.state", d.id), map[string]interface{}{"progress": percent})
	return nil
}
func (d *deviceImpl) Complete(status string, meta map[string]interface{}) error {
	d.mu.Lock()
	d.state.Enabled = false
	d.state.Phase = "idle"
	d.state.Status = status
	d.state.Progress = 100
	s := d.state
	d.mu.Unlock()

	d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.bundle.Publish(fmt.Sprintf("device.%s.state", d.id), map[string]interface{}{"enabled": false, "phase": "idle", "status": status})
	return nil
}
func (d *deviceImpl) Fail(reason string, meta map[string]interface{}) error {
	d.mu.Lock()
	d.state.Enabled = false
	d.state.Phase = "idle"
	d.state.Status = "failed: " + reason
	s := d.state
	d.mu.Unlock()

	d.bundle.saveJSON(string(d.id)+".state.json", s)
	d.bundle.Publish(fmt.Sprintf("device.%s.state", d.id), map[string]interface{}{"enabled": false, "phase": "idle", "status": s.Status})
	return nil
}

func (d *deviceImpl) CreateEntity(entityType sdk.EntityType) (sdk.Entity, error) {
	return d.CreateEntityEx(entityType, nil)
}

func (d *deviceImpl) CreateEntityEx(entityType sdk.EntityType, capabilities []string) (sdk.Entity, error) {
	id := generateUUID()
	prefix := string(d.id) + "." + string(id)

	m := sdk.EntityMetadata{ID: id, EntityID: id, Type: entityType, Capabilities: capabilities}
	s := sdk.EntityState{Enabled: true, Status: "active"}
	r := map[string]interface{}{}

	d.bundle.saveJSON(prefix+".json", m)
	d.bundle.saveJSON(prefix+".state.json", s)
	d.bundle.saveJSON(prefix+".raw.json", r)
	ioutil.WriteFile(filepath.Join(d.bundle.statePath, prefix+".script"), []byte("-- OnLoad() {}"), 0644)
	ioutil.WriteFile(filepath.Join(d.bundle.statePath, prefix+".state.script"), []byte("-- {}"), 0644)

	d.mu.Lock()
	d.metadata.Entities = append(d.metadata.Entities, id)
	devM := d.metadata
	d.mu.Unlock()
	d.bundle.saveJSON(string(d.id)+".json", devM)
	d.bundle.Publish(fmt.Sprintf("device.%s.metadata", d.id), map[string]interface{}{"entities": devM.Entities})

	ei := &entityImpl{
		id:       id,
		deviceID: d.id,
		bundle:   d.bundle,
		metadata: m,
		state:    s,
		raw:      r,
	}

	d.bundle.mu.Lock()
	d.bundle.entities[id] = ei
	d.bundle.mu.Unlock()

	// Now we can get the typed version
	ent, _ := d.bundle.createEntityObject(id, d.id)

	ei.initWorker()

	d.bundle.Publish(fmt.Sprintf("entity.%s.state", id), map[string]interface{}{"event": "created", "type": entityType, "capabilities": capabilities})
	return ent, nil
}

func (d *deviceImpl) GetEntities() ([]sdk.Entity, error) {
	m := d.Metadata()
	var entities []sdk.Entity
	d.bundle.mu.RLock()
	defer d.bundle.mu.RUnlock()
	for _, eid := range m.Entities {
		if _, ok := d.bundle.entities[eid]; ok {
			// Get the typed wrapper
			ent, _ := d.bundle.createEntityObject(eid, d.id)
			entities = append(entities, ent)
		}
	}
	return entities, nil
}

func (d *deviceImpl) DeleteEntity(id sdk.UUID) error {
	prefix := string(d.id) + "." + string(id)
	os.Remove(filepath.Join(d.bundle.statePath, prefix+".json"))
	os.Remove(filepath.Join(d.bundle.statePath, prefix+".state.json"))
	os.Remove(filepath.Join(d.bundle.statePath, prefix+".raw.json"))
	os.Remove(filepath.Join(d.bundle.statePath, prefix+".script"))
	os.Remove(filepath.Join(d.bundle.statePath, prefix+".state.script"))

	devM := d.Metadata()
	newList := []sdk.UUID{}
	for _, eid := range devM.Entities {
		if eid != id {
			newList = append(newList, eid)
		}
	}
	devM.Entities = newList
	d.bundle.saveJSON(string(d.id)+".json", devM)

	d.bundle.mu.Lock()
	delete(d.bundle.entities, id)
	d.bundle.mu.Unlock()

	d.bundle.Publish(fmt.Sprintf("entity.%s.state", id), map[string]interface{}{"event": "deleted"})
	return nil
}

func (d *deviceImpl) OnCommand(handler sdk.CommandHandler) {
	d.bundle.Log().Info("Device.OnCommand bound for device %s (total handlers: %d)", d.id, len(d.handlers)+1)
	if d.bundle.nc == nil {
		return
	}

	d.mu.Lock()
	d.handlers = append(d.handlers, handler)
	d.ensureCommandSubscriptionLocked()
	d.mu.Unlock()
}

func (d *deviceImpl) ensureCommandSubscriptionLocked() {
	if d.bundle == nil || d.bundle.nc == nil || d.cmdSub != nil {
		return
	}
	subject := fmt.Sprintf("device.%s.command", d.id)
	d.bundle.Log().Info("[CMDSUB] SUBSCRIBE device command subject=%s", subject)
	sub, _ := d.bundle.nc.Subscribe(subject, func(msg *nats.Msg) {
		d.bundle.Log().Info("[CMDSUB] RECV device command subject=%s bytes=%d", msg.Subject, len(msg.Data))
		var m sdk.Message
		json.Unmarshal(msg.Data, &m)
		SafeRun(d.bundle.id, "Device.OnCommand", func() {
			cmd, _ := m.Payload["command"].(string)
			if cmd == "" {
				cmd, _ = m.Payload["action"].(string)
			}
			if cmd != "" {
				d.mu.RLock()
				userHandlers := make([]sdk.CommandHandler, len(d.handlers))
				copy(userHandlers, d.handlers)
				scriptHandler := d.scriptCmdHandler
				d.mu.RUnlock()
				handlers := make([]sdk.CommandHandler, 0, len(userHandlers)+1)
				if scriptHandler != nil {
					handlers = append(handlers, scriptHandler)
				}
				handlers = append(handlers, userHandlers...)

				d.bundle.Log().Info("Device %s dispatching command %s to %d handlers", d.id, cmd, len(handlers))
				for _, h := range handlers {
					h(cmd, m.Payload)
				}
			}
		})
	})
	d.cmdSub = sub
}
