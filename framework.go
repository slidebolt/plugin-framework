package framework

import (
	"encoding/json"
	"fmt"
	"github.com/slidebolt/plugin-sdk"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	bundles   = make(map[sdk.UUID]*bundleImpl)
	bundlesMu sync.RWMutex
	sharedNC  *nats.Conn
)

func getNatsURL() string {
	url := os.Getenv("NATS_URL")
	if url == "" {
		return "nats://127.0.0.1:4222"
	}
	return url
}

func getNC() *nats.Conn {
	bundlesMu.Lock()
	defer bundlesMu.Unlock()
	if sharedNC == nil {
		var (
			nc  *nats.Conn
			err error
		)
		url := getNatsURL()
		fmt.Printf("[FRAMEWORK] Initializing NATS connection to %s...\n", url)
		// Core starts NATS and then sidecars in quick succession. A short retry
		// window avoids early plugin startup races where the first connect fails.
		for i := 0; i < 10; i++ {
			nc, err = nats.Connect(url)
			if err == nil {
				sharedNC = nc
				fmt.Printf("[FRAMEWORK] NATS connected successfully.\n")
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if sharedNC == nil {
			fmt.Printf("NATS Connect Error (%s): %v\n", url, err)
			return nil
		}
	}
	return sharedNC
}

func RegisterBundle(bundleID string) (sdk.Bundle, error) {
	id := sdk.UUID(bundleID)

	stateDir := os.Getenv("STATE_DIR")
	if stateDir == "" {
		cwd, _ := os.Getwd()
		stateDir = filepath.Join(cwd, "state")
	}
	statePath := filepath.Join(stateDir, string(id))

	cwd, _ := os.Getwd()
	logsPath := filepath.Join(cwd, "logs", string(id), "log.folder")

	os.MkdirAll(statePath, 0755)
	os.MkdirAll(logsPath, 0755)

	lockPath := filepath.Join(statePath, ".bundle.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("bundle lock open failed: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("bundle %s already running (lock busy)", id)
	}
	_ = lockFile.Truncate(0)
	_, _ = lockFile.Seek(0, 0)
	_, _ = fmt.Fprintf(lockFile, "pid=%d\nbundle=%s\n", os.Getpid(), id)

	bundle := &bundleImpl{
		id:        id,
		statePath: statePath,
		logsPath:  logsPath,
		devices:   make(map[sdk.UUID]*deviceImpl),
		entities:  make(map[sdk.UUID]*entityImpl),
		logger:    &loggerImpl{bundleID: id, logPath: logsPath},
		lockFile:  lockFile,
	}

	bundle.ensureFile(string(id)+".json", sdk.BundleMetadata{ID: id, Name: "New Bundle"})
	bundle.ensureFile(string(id)+".state.json", sdk.BundleState{Enabled: true, Status: "active"})
	bundle.ensureFile(string(id)+".raw.json", map[string]interface{}{})
	bundle.ensureRawFile(string(id)+".script", "-- OnLoad() {}")
	bundle.ensureRawFile(string(id)+".state.script", "-- {}")

	bundle.nc = getNC()
	url := getNatsURL()
	if bundle.nc == nil {
		fmt.Printf("[FRAMEWORK] [%s] NATS NOT CONNECTED\n", bundle.id)
	} else {
		fmt.Printf("[FRAMEWORK] [%s] NATS CONNECTED to %s\n", bundle.id, url)
	}
	bundle.loadFromDisk()

	// Setup RPC before adding to registry to ensure listeners are ready
	bundle.setupRPC()

	bundlesMu.Lock()
	bundles[id] = bundle
	bundlesMu.Unlock()

	return bundle, nil
}

func (b *bundleImpl) setupRPC() {
	if b.nc == nil {
		fmt.Printf("[FRAMEWORK] [%s] Skipping RPC setup: NATS connection is nil\n", b.id)
		return
	}

	fmt.Printf("[FRAMEWORK] [%s] Setting up RPC responders...\n", b.id)

	// 1. Respond to global bundle discovery
	b.nc.Subscribe("bundle.discovery", func(msg *nats.Msg) {
		meta := b.Metadata()
		state := b.State()
		resp, _ := json.Marshal(map[string]any{
			"id":     b.id,
			"name":   meta.Name,
			"status": state.Status,
		})
		b.nc.Publish(msg.Reply, resp)
	})

	// 2. Metadata / State / Raw / Config Access
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_metadata", b.id), func(msg *nats.Msg) {
		resp, _ := json.Marshal(b.Metadata())
		b.nc.Publish(msg.Reply, resp)
	})
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_state", b.id), func(msg *nats.Msg) {
		resp, _ := json.Marshal(b.State())
		b.nc.Publish(msg.Reply, resp)
	})
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_raw", b.id), func(msg *nats.Msg) {
		resp, _ := json.Marshal(b.Raw())
		b.nc.Publish(msg.Reply, resp)
	})
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.health", b.id), func(msg *nats.Msg) {
		resp, _ := json.Marshal(b.healthSnapshot())
		if msg.Reply != "" {
			_ = b.nc.Publish(msg.Reply, resp)
		}
	})

	// 3. Script Access
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_script", b.id), func(msg *nats.Msg) {
		b.nc.Publish(msg.Reply, []byte(b.Script()))
	})
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_state_script", b.id), func(msg *nats.Msg) {
		b.nc.Publish(msg.Reply, []byte(b.StateScript()))
	})
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.set_script", b.id), func(msg *nats.Msg) {
		_ = b.UpdateScript(string(msg.Data))
		b.nc.Publish(msg.Reply, []byte("ok"))
	})

	// 4. Device list and lifecycle
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_devices", b.id), func(msg *nats.Msg) {
		devs := b.GetDevices()
		resp, _ := json.Marshal(devs)
		b.nc.Publish(msg.Reply, resp)
	})

	// 3. Delete a device (and purge its entities/state)
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.delete_device", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID string `json:"uuid"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if req.UUID == "" {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "uuid is required"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		err := b.DeleteDevice(sdk.UUID(req.UUID))
		if msg.Reply != "" {
			if err != nil {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				_ = b.nc.Publish(msg.Reply, resp)
				return
			}
			resp, _ := json.Marshal(map[string]any{"ok": true})
			_ = b.nc.Publish(msg.Reply, resp)
		}
	})

	// 4. Update device raw
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_device_raw", b.id), func(msg *nats.Msg) {
		var req struct {
			DeviceUUID string                 `json:"device_uuid"`
			Raw        map[string]interface{} `json:"raw"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if req.DeviceUUID == "" {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "device_uuid is required"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		b.mu.RLock()
		dev, ok := b.devices[sdk.UUID(req.DeviceUUID)]
		b.mu.RUnlock()
		if !ok {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "device not found"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		err := dev.UpdateRaw(req.Raw)
		if msg.Reply != "" {
			if err != nil {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				_ = b.nc.Publish(msg.Reply, resp)
				return
			}
			resp, _ := json.Marshal(map[string]any{"ok": true})
			_ = b.nc.Publish(msg.Reply, resp)
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_device_metadata", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			if dev, err := b.GetDevice(req.UUID); err == nil {
				m := dev.Metadata()
				if req.Name != "" {
					m.Name = req.Name
				}
				dev.UpdateMetadata(m.Name, m.SourceID)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_device_local_name", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			if dev, err := b.GetDevice(req.UUID); err == nil {
				dev.UpdateLocalName(req.Name)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_device_source_name", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			if dev, err := b.GetDevice(req.UUID); err == nil {
				dev.UpdateSourceName(req.Name)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_entity_local_name", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			b.mu.RLock()
			ent, ok := b.entities[req.UUID]
			b.mu.RUnlock()
			if ok {
				ent.UpdateLocalName(req.Name)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_entity_source_name", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			b.mu.RLock()
			ent, ok := b.entities[req.UUID]
			b.mu.RUnlock()
			if ok {
				ent.UpdateSourceName(req.Name)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.update_entity_metadata", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID sdk.UUID `json:"uuid"`
			Name string   `json:"name"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil && req.UUID != "" {
			b.mu.RLock()
			ent, ok := b.entities[req.UUID]
			b.mu.RUnlock()
			if ok {
				m := ent.Metadata()
				if req.Name != "" {
					m.Name = req.Name
				}
				ent.UpdateMetadata(m.Name, m.SourceID)
			}
		}
	})

	// 5. Create a new entity on an existing device
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.create_entity", b.id), func(msg *nats.Msg) {
		var req struct {
			DeviceUUID string `json:"device_uuid"`
			Type       string `json:"type"`
			Name       string `json:"name"`
			SourceID   string `json:"source_id"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if req.DeviceUUID == "" {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "device_uuid is required"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		b.mu.RLock()
		dev, ok := b.devices[sdk.UUID(req.DeviceUUID)]
		b.mu.RUnlock()
		if !ok {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "device not found"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		ent, err := dev.CreateEntity(sdk.EntityType(req.Type))
		if err != nil {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}
		if req.Name != "" || req.SourceID != "" {
			_ = ent.UpdateMetadata(req.Name, sdk.SourceID(req.SourceID))
		}
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]any{"ok": true, "entity_uuid": string(ent.ID())})
			_ = b.nc.Publish(msg.Reply, resp)
		}
	})

	// 6. Entity lifecycle

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_entity", b.id), func(msg *nats.Msg) {

		var req struct {
			UUID string `json:"uuid"`
		}

		_ = json.Unmarshal(msg.Data, &req)

		b.mu.RLock()

		ent, ok := b.entities[sdk.UUID(req.UUID)]

		b.mu.RUnlock()

		if !ok {

			b.nc.Publish(msg.Reply, []byte("{}"))

			return

		}

		resp, _ := json.Marshal(ent)

		b.nc.Publish(msg.Reply, resp)

	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_entity_raw", b.id), func(msg *nats.Msg) {

		var req struct {
			UUID string `json:"uuid"`
		}

		_ = json.Unmarshal(msg.Data, &req)

		b.mu.RLock()

		ent, ok := b.entities[sdk.UUID(req.UUID)]

		b.mu.RUnlock()

		if !ok {

			b.nc.Publish(msg.Reply, []byte("{}"))

			return

		}

		resp, _ := json.Marshal(ent.Raw())

		b.nc.Publish(msg.Reply, resp)

	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.get_entity_script", b.id), func(msg *nats.Msg) {

		eid := sdk.UUID(string(msg.Data))

		b.mu.RLock()

		ei, ok := b.entities[eid]

		b.mu.RUnlock()

		if !ok {

			b.nc.Publish(msg.Reply, []byte(""))

			return

		}

		ent, _ := b.createEntityObject(eid, ei.deviceID)

		b.nc.Publish(msg.Reply, []byte(ent.Script()))

	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.delete_entity", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID string `json:"uuid"`
		}
		_ = json.Unmarshal(msg.Data, &req)

		b.mu.RLock()
		ei, ok := b.entities[sdk.UUID(req.UUID)]
		b.mu.RUnlock()
		if !ok {
			if msg.Reply != "" {
				b.nc.Publish(msg.Reply, []byte("not found"))
			}
			return
		}

		dev, err := b.GetDevice(ei.deviceID)
		if err == nil {
			_ = dev.DeleteEntity(sdk.UUID(req.UUID))
		}

		if msg.Reply != "" {
			b.nc.Publish(msg.Reply, []byte("ok"))
		}
	})

	// 7. Update entity script and hot-reload worker
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.set_entity_script", b.id), func(msg *nats.Msg) {
		var req struct {
			EntityUUID string `json:"entity_uuid"`
			Script     string `json:"script"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if req.EntityUUID == "" {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "entity_uuid is required"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}

		eid := sdk.UUID(req.EntityUUID)
		b.mu.RLock()
		ei, ok := b.entities[eid]
		b.mu.RUnlock()
		if !ok {
			if msg.Reply != "" {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": "entity not found"})
				_ = b.nc.Publish(msg.Reply, resp)
			}
			return
		}

		ent, _ := b.createEntityObject(eid, ei.deviceID)
		err := ent.UpdateScript(req.Script)
		if msg.Reply != "" {
			if err != nil {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				_ = b.nc.Publish(msg.Reply, resp)
				return
			}
			resp, _ := json.Marshal(map[string]any{"ok": true})
			_ = b.nc.Publish(msg.Reply, resp)
		}
	})

	// 7. Configure plugin (update bundle raw config and trigger re-init)
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.configure", b.id), func(msg *nats.Msg) {
		var config map[string]interface{}
		_ = json.Unmarshal(msg.Data, &config)
		err := b.UpdateRaw(config)
		if msg.Reply != "" {
			if err != nil {
				resp, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
				_ = b.nc.Publish(msg.Reply, resp)
				return
			}
			resp, _ := json.Marshal(map[string]any{"ok": true})
			_ = b.nc.Publish(msg.Reply, resp)
		}
		b.mu.RLock()
		handler := b.onConfigure
		b.mu.RUnlock()
		if handler != nil {
			go handler()
		}
	})

	// 8. Reload plugin — re-runs onConfigure without changing raw config.
	// This allows the plugin to rebuild its devices while preserving UUIDs.
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.reload", b.id), func(msg *nats.Msg) {
		b.mu.RLock()
		handler := b.onConfigure
		b.mu.RUnlock()
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]any{"ok": true})
			_ = b.nc.Publish(msg.Reply, resp)
		}
		if handler != nil {
			go handler()
		}
	})

	// 9. Label Management
	b.nc.Subscribe(fmt.Sprintf("bundle.%s.add_label", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID  string `json:"uuid"`
			Label string `json:"label"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if obj, ok := b.GetByUUID(sdk.UUID(req.UUID)); ok {
			if dev, ok := obj.(sdk.Device); ok {
				_ = dev.AddLabel(req.Label)
			} else if ent, ok := obj.(sdk.Entity); ok {
				_ = ent.AddLabel(req.Label)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.remove_label", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID  string `json:"uuid"`
			Label string `json:"label"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if obj, ok := b.GetByUUID(sdk.UUID(req.UUID)); ok {
			if dev, ok := obj.(sdk.Device); ok {
				_ = dev.RemoveLabel(req.Label)
			} else if ent, ok := obj.(sdk.Entity); ok {
				_ = ent.RemoveLabel(req.Label)
			}
		}
	})

	b.nc.Subscribe(fmt.Sprintf("bundle.%s.set_labels", b.id), func(msg *nats.Msg) {
		var req struct {
			UUID   string   `json:"uuid"`
			Labels []string `json:"labels"`
		}
		_ = json.Unmarshal(msg.Data, &req)
		if obj, ok := b.GetByUUID(sdk.UUID(req.UUID)); ok {
			if dev, ok := obj.(sdk.Device); ok {
				_ = dev.SetLabels(req.Labels)
			} else if ent, ok := obj.(sdk.Entity); ok {
				_ = ent.SetLabels(req.Labels)
			}
		}
	})
}

func GetDevices() []sdk.Device {
	bundlesMu.RLock()
	defer bundlesMu.RUnlock()
	var out []sdk.Device
	for _, b := range bundles {
		out = append(out, b.GetDevices()...)
	}
	return out
}

func GetByUUID(id sdk.UUID) (interface{}, bool) {
	bundlesMu.RLock()
	defer bundlesMu.RUnlock()
	for _, b := range bundles {
		b.mu.RLock()
		if dev, ok := b.devices[id]; ok {
			b.mu.RUnlock()
			return dev, true
		}
		if ent, ok := b.entities[id]; ok {
			b.mu.RUnlock()
			return ent, true
		}
		b.mu.RUnlock()
	}
	return nil, false
}

func GetBySourceID(sid sdk.SourceID) (interface{}, bool) {
	bundlesMu.RLock()
	defer bundlesMu.RUnlock()
	for _, b := range bundles {
		if obj, ok := b.GetBySourceID(sid); ok {
			return obj, true
		}
	}
	return nil, false
}

func GetByLabel(labelsIn interface{}) []interface{} {
	var searchLabels []string
	switch v := labelsIn.(type) {
	case string:
		searchLabels = []string{v}
	case []string:
		searchLabels = v
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok {
				searchLabels = append(searchLabels, s)
			}
		}
	}

	if len(searchLabels) == 0 {
		return nil
	}

	matchesAll := func(target []string) bool {
		for _, s := range searchLabels {
			found := false
			for _, t := range target {
				if t == s {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	var out []interface{}

	// 1. Local Search (High speed, avoids NATS latency for local objects)
	bundlesMu.RLock()
	for _, b := range bundles {
		if b == nil {
			continue
		}
		// Device check
		for _, d := range b.GetDevices() {
			if d == nil {
				continue
			}
			if matchesAll(d.Metadata().Labels) {
				out = append(out, d)
			}
		}
		// Entity check
		ents, _ := b.GetEntities()
		for _, e := range ents {
			if e == nil {
				continue
			}
			if matchesAll(e.Metadata().Labels) {
				out = append(out, e)
			}
		}
	}
	bundlesMu.RUnlock()

	// 2. Global Search (via Core registry)
	if sharedNC != nil {
		var payload []byte
		if len(searchLabels) == 1 {
			payload = []byte(searchLabels[0])
		} else {
			payload, _ = json.Marshal(searchLabels)
		}

		msg, err := sharedNC.Request("registry.search_by_label", payload, 500*time.Millisecond)
		if err == nil {
			var uuids []string
			if err := json.Unmarshal(msg.Data, &uuids); err == nil {
				// We need a bundle instance to call GetRemoteObject
				bundlesMu.RLock()
				var firstBundle *bundleImpl
				for _, b := range bundles {
					if b != nil {
						firstBundle = b
						break
					}
				}
				bundlesMu.RUnlock()

				if firstBundle != nil {
					for _, id := range uuids {
						// Avoid duplicates if already found locally
						alreadyFound := false
						for _, existing := range out {
							if dev, ok := existing.(sdk.Device); ok && string(dev.ID()) == id {
								alreadyFound = true
								break
							}
							if ent, ok := existing.(sdk.Entity); ok && string(ent.ID()) == id {
								alreadyFound = true
								break
							}
						}
						if alreadyFound {
							continue
						}

						// Fetch remote proxy object
						if obj, ok := firstBundle.GetRemoteObject(sdk.UUID(id)); ok {
							out = append(out, obj)
						}
					}
				}
			}
		}
	}

	return out
}

func Init() {
	fmt.Println("Framework Init")
}

func StartObserver() {
	// Obsolete in passthrough mode
}
