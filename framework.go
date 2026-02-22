package framework

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"github.com/slidebolt/plugin-sdk"
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
		// Core starts NATS and then sidecars in quick succession. A short retry
		// window avoids early plugin startup races where the first connect fails.
		for i := 0; i < 10; i++ {
			nc, err = nats.Connect(url)
			if err == nil {
				sharedNC = nc
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
		return
	}

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

	// 2. Respond to specific device list request
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

	// 6. Update entity script and hot-reload worker
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

func Init() {
	fmt.Println("Framework Init")
}

func StartObserver() {
	// Obsolete in passthrough mode
}
