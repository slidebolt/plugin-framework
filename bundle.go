package framework

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"github.com/slidebolt/plugin-sdk"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type bundleImpl struct {
	id          sdk.UUID
	statePath   string
	logsPath    string
	devices     map[sdk.UUID]*deviceImpl
	entities    map[sdk.UUID]*entityImpl
	nc          *nats.Conn
	mu          sync.RWMutex
	logger      *loggerImpl
	lockFile    *os.File
	onConfigure func()
}

func (b *bundleImpl) ID() sdk.UUID    { return b.id }
func (b *bundleImpl) Log() sdk.Logger { return b.logger }

func (b *bundleImpl) OnConfigure(handler func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onConfigure = handler
}

func (b *bundleImpl) Metadata() sdk.BundleMetadata {
	var m sdk.BundleMetadata
	b.loadJSON(string(b.id)+".json", &m)
	return m
}
func (b *bundleImpl) State() sdk.BundleState {
	var s sdk.BundleState
	b.loadJSON(string(b.id)+".state.json", &s)
	return s
}
func (b *bundleImpl) Raw() map[string]interface{} {
	var r map[string]interface{}
	b.loadJSON(string(b.id)+".raw.json", &r)
	return r
}
func (b *bundleImpl) Script() string {
	bytes, _ := ioutil.ReadFile(filepath.Join(b.statePath, string(b.id)+".script"))
	return string(bytes)
}
func (b *bundleImpl) StateScript() string {
	bytes, _ := ioutil.ReadFile(filepath.Join(b.statePath, string(b.id)+".state.script"))
	return string(bytes)
}

func (b *bundleImpl) UpdateMetadata(name string) error {
	m := sdk.BundleMetadata{ID: b.id, Name: name}
	err := b.saveJSON(string(b.id)+".json", m)
	b.Publish(fmt.Sprintf("bundle.%s.metadata", b.id), map[string]interface{}{"name": name})
	return err
}
func (b *bundleImpl) UpdateState(status string) error {
	return b.setState(true, status)
}

func (b *bundleImpl) Disable(status string) error {
	return b.setState(false, status)
}

func (b *bundleImpl) setState(enabled bool, status string) error {
	s := sdk.BundleState{Enabled: enabled, Status: status}
	err := b.saveJSON(string(b.id)+".state.json", s)
	b.Publish(fmt.Sprintf("bundle.%s.state", b.id), map[string]interface{}{"enabled": enabled, "status": status})
	return err
}
func (b *bundleImpl) UpdateRaw(data map[string]interface{}) error {
	err := b.saveJSON(string(b.id)+".raw.json", data)
	b.Publish(fmt.Sprintf("bundle.%s.raw", b.id), data)
	return err
}
func (b *bundleImpl) UpdateScript(code string) error {
	return ioutil.WriteFile(filepath.Join(b.statePath, string(b.id)+".script"), []byte(code), 0644)
}
func (b *bundleImpl) UpdateStateScript(config string) error {
	return ioutil.WriteFile(filepath.Join(b.statePath, string(b.id)+".state.script"), []byte(config), 0644)
}

func (b *bundleImpl) Publish(subject string, payload map[string]interface{}) error {
	if b.nc == nil {
		return nil
	}
	msg := sdk.Message{
		Source:    b.id,
		Subject:   subject,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(msg)
	return b.nc.Publish(subject, data)
}

func (b *bundleImpl) startHeartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		b.Publish(fmt.Sprintf("bundle.%s.heartbeat", b.id), map[string]interface{}{
			"status":    "alive",
			"timestamp": time.Now().UnixNano(),
		})
	}
}

func (b *bundleImpl) Every15Seconds(handler func()) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			SafeRun(b.id, "Every15Seconds", handler)
		}
	}()
}

func (b *bundleImpl) EveryMinute(handler func()) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			SafeRun(b.id, "EveryMinute", handler)
		}
	}()
}

func (b *bundleImpl) loadFromDisk() {
	files, _ := ioutil.ReadDir(b.statePath)

	// Collect work to do after releasing the write lock; initWorker and
	// broadcastRegistration both need to acquire b.mu themselves.
	var devicesToWake []*deviceImpl
	var entitiesToWake []*entityImpl

	b.mu.Lock()
	for _, f := range files {
		name := f.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		count := strings.Count(name, ".")
		switch count {
		case 1:
			id := sdk.UUID(strings.TrimSuffix(name, ".json"))
			if id == b.id {
				continue
			}
			d := &deviceImpl{id: id, bundle: b}
			b.loadJSON(string(id)+".json", &d.metadata)
			b.loadJSON(string(id)+".state.json", &d.state)
			b.loadJSON(string(id)+".raw.json", &d.raw)
			b.devices[id] = d
			devicesToWake = append(devicesToWake, d)
		case 2:
			parts := strings.Split(name, ".")
			// parts[1] is "state" or "raw" for device state/raw files
			// (e.g. {uuid}.state.json, {uuid}.raw.json).  Only the form
			// {deviceUUID}.{entityUUID}.json has a UUID-shaped parts[1].
			if parts[1] == "state" || parts[1] == "raw" {
				continue
			}
			deviceID := sdk.UUID(parts[0])
			entityID := sdk.UUID(parts[1])
			prefix := deviceID.String() + "." + entityID.String()
			// Construct entityImpl directly — createEntityObject acquires
			// b.mu.RLock() which would deadlock against the Lock() we hold.
			ei := &entityImpl{id: entityID, deviceID: deviceID, bundle: b}
			b.loadJSON(prefix+".json", &ei.metadata)
			b.loadJSON(prefix+".state.json", &ei.state)
			b.loadJSON(prefix+".raw.json", &ei.raw)
			b.entities[entityID] = ei
			entitiesToWake = append(entitiesToWake, ei)
		}
	}
	b.mu.Unlock()

	// Workers and NATS subscriptions are started only after all state is in
	// memory and the write lock is released.
	for _, ei := range entitiesToWake {
		ei.initWorker()
	}
	for _, d := range devicesToWake {
		d.initWorker()
		b.broadcastRegistration(d)
	}
}

func (b *bundleImpl) broadcastRegistration(d *deviceImpl) {
	if b.nc == nil {
		return
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Capture all entities for this device
	entities := make(map[sdk.UUID]interface{})
	b.mu.RLock()
	for _, eid := range d.metadata.Entities {
		if e, ok := b.entities[eid]; ok {
			e.mu.RLock()
			entities[eid] = struct {
				ID       sdk.UUID           `json:"id"`
				DeviceID sdk.UUID           `json:"deviceID"`
				Metadata sdk.EntityMetadata `json:"metadata"`
				State    sdk.EntityState    `json:"state"`
				Raw      map[string]any     `json:"raw"`
			}{
				ID: e.id, DeviceID: e.deviceID, Metadata: e.metadata, State: e.state, Raw: e.raw,
			}
			e.mu.RUnlock()
		}
	}
	b.mu.RUnlock()

	payload := struct {
		BundleID sdk.UUID `json:"bundle_id"`
		Device   struct {
			ID       sdk.UUID                 `json:"id"`
			Metadata sdk.DeviceMetadata       `json:"metadata"`
			State    sdk.DeviceState          `json:"state"`
			Raw      map[string]any           `json:"raw"`
			Entities map[sdk.UUID]interface{} `json:"entities"`
		} `json:"device"`
	}{
		BundleID: b.id,
		Device: struct {
			ID       sdk.UUID                 `json:"id"`
			Metadata sdk.DeviceMetadata       `json:"metadata"`
			State    sdk.DeviceState          `json:"state"`
			Raw      map[string]any           `json:"raw"`
			Entities map[sdk.UUID]interface{} `json:"entities"`
		}{
			ID: d.id, Metadata: d.metadata, State: d.state, Raw: d.raw, Entities: entities,
		},
	}

	data, _ := json.Marshal(payload)
	b.nc.Publish("registry.device.register", data)
}

func (b *bundleImpl) CreateDevice() (sdk.Device, error) {
	id := generateUUID()

	m := sdk.DeviceMetadata{ID: id, Entities: []sdk.UUID{}}
	s := sdk.DeviceState{Enabled: true, Status: "active"}
	r := map[string]interface{}{}

	d := &deviceImpl{
		id:       id,
		bundle:   b,
		metadata: m,
		state:    s,
		raw:      r,
	}
	b.mu.Lock()
	b.devices[id] = d
	b.mu.Unlock()

	fmt.Printf("[FRAMEWORK] Created Device %s in Bundle %s\n", id, b.id)

	b.saveJSON(string(id)+".json", m)
	b.saveJSON(string(id)+".state.json", s)
	b.saveJSON(string(id)+".raw.json", r)
	ioutil.WriteFile(filepath.Join(b.statePath, string(id)+".script"), []byte("-- OnLoad() {}"), 0644)
	ioutil.WriteFile(filepath.Join(b.statePath, string(id)+".state.script"), []byte("-- {}"), 0644)

	b.broadcastRegistration(d)
	b.Publish(fmt.Sprintf("device.%s.state", id), map[string]interface{}{"event": "created"})
	return d, nil
}

func (b *bundleImpl) GetDevice(id sdk.UUID) (sdk.Device, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if d, ok := b.devices[id]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("not found")
}

func (b *bundleImpl) GetDevices() []sdk.Device {
	b.mu.RLock()
	defer b.mu.RUnlock()
	devs := make([]sdk.Device, 0, len(b.devices))
	for _, d := range b.devices {
		devs = append(devs, d)
	}
	return devs
}

func (b *bundleImpl) DeleteDevice(id sdk.UUID) error {
	b.mu.RLock()
	d, ok := b.devices[id]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("device not found: %s", id)
	}

	ents, _ := d.GetEntities()
	for _, ent := range ents {
		_ = d.DeleteEntity(ent.ID())
	}

	d.mu.Lock()
	if d.worker != nil {
		d.worker.Close()
		d.worker = nil
	}
	if d.sub != nil {
		_ = d.sub.Unsubscribe()
		d.sub = nil
	}
	if d.cmdSub != nil {
		_ = d.cmdSub.Unsubscribe()
		d.cmdSub = nil
	}
	d.mu.Unlock()

	_ = os.Remove(filepath.Join(b.statePath, string(id)+".json"))
	_ = os.Remove(filepath.Join(b.statePath, string(id)+".state.json"))
	_ = os.Remove(filepath.Join(b.statePath, string(id)+".raw.json"))
	_ = os.Remove(filepath.Join(b.statePath, string(id)+".script"))
	_ = os.Remove(filepath.Join(b.statePath, string(id)+".state.script"))

	b.mu.Lock()
	delete(b.devices, id)
	b.mu.Unlock()

	b.Publish(fmt.Sprintf("device.%s.state", id), map[string]interface{}{"event": "deleted"})
	if b.nc != nil {
		data, _ := json.Marshal(map[string]interface{}{
			"bundle_id": b.id,
			"device_id": id,
		})
		_ = b.nc.Publish("registry.device.unregister", data)
	}

	return nil
}

func (b *bundleImpl) GetEntities() ([]sdk.Entity, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ents := make([]sdk.Entity, 0, len(b.entities))
	for _, e := range b.entities {
		ents = append(ents, e)
	}
	return ents, nil
}

func (b *bundleImpl) GetBySourceID(sourceID sdk.SourceID) (interface{}, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sidStr := string(sourceID)
	// Check devices
	for _, d := range b.devices {
		msid := string(d.Metadata().SourceID)
		if msid == sidStr {
			return d, true
		}
	}
	// Check entities
	for _, e := range b.entities {
		msid := string(e.Metadata().SourceID)
		if msid == sidStr {
			ent, _ := b.createEntityObject(e.id, e.deviceID)
			return ent, true
		}
	}
	return nil, false
}

func (b *bundleImpl) ensureFile(filename string, defaultData interface{}) {
	path := filepath.Join(b.statePath, filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		b.saveJSON(filename, defaultData)
	}
}
func (b *bundleImpl) ensureRawFile(filename string, defaultData string) {
	path := filepath.Join(b.statePath, filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		ioutil.WriteFile(path, []byte(defaultData), 0644)
	}
}
func (b *bundleImpl) saveJSON(filename string, data interface{}) error {
	bytes, _ := json.MarshalIndent(data, "", "  ")
	return ioutil.WriteFile(filepath.Join(b.statePath, filename), bytes, 0644)
}
func (b *bundleImpl) loadJSON(filename string, v interface{}) error {
	path := filepath.Join(b.statePath, filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, v)
}

func (b *bundleImpl) createEntityObject(id sdk.UUID, deviceID sdk.UUID) (sdk.Entity, *entityImpl) {
	b.mu.RLock()
	ei, ok := b.entities[id]
	b.mu.RUnlock()

	var e *entityImpl
	if ok {
		e = ei
	} else {
		e = &entityImpl{id: id, deviceID: deviceID, bundle: b}
	}

	m := e.Metadata()
	switch m.Type {
	case sdk.TYPE_SWITCH:
		return &switchImpl{entityImpl: e}, e
	case sdk.TYPE_LIGHT:
		return &lightImpl{entityImpl: e}, e
	case sdk.TYPE_SENSOR:
		return &sensorImpl{entityImpl: e}, e
	case sdk.TYPE_BINARY_SENSOR:
		return &binarySensorImpl{entityImpl: e}, e
	case sdk.TYPE_COVER:
		return &coverImpl{entityImpl: e}, e
	case sdk.TYPE_CAMERA:
		return &cameraImpl{entityImpl: e}, e
	default:
		return e, e
	}
}
