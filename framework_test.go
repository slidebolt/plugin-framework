package framework

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"plugin-sdk"
	"strings"
	"testing"
	"time"
)

func setupIntegrationTest(t *testing.T) {
	t.Helper()

	// Ensure deterministic isolation across tests and repeated `go test` runs.
	_ = os.RemoveAll("state")
	_ = os.RemoveAll("logs")

	bundlesMu.Lock()
	bundles = make(map[sdk.UUID]*bundleImpl)
	bundlesMu.Unlock()

	if sharedNC != nil {
		sharedNC.Close()
		sharedNC = nil
	}

	t.Cleanup(func() {
		_ = os.RemoveAll("state")
		_ = os.RemoveAll("logs")
		bundlesMu.Lock()
		bundles = make(map[sdk.UUID]*bundleImpl)
		bundlesMu.Unlock()
		if sharedNC != nil {
			sharedNC.Close()
			sharedNC = nil
		}
	})
}

func TestIntegration_PluginInterfaces(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000002"
	b, _ := RegisterBundle(bundleID)

	device, _ := b.CreateDevice()
	switchEnt, _ := device.CreateEntity(sdk.TYPE_SWITCH)
	lightEnt, _ := device.CreateEntity(sdk.TYPE_LIGHT)

	// Cast to Switch
	_, ok := switchEnt.(sdk.Switch)
	if !ok {
		t.Fatal("Failed to cast to Switch interface")
	}

	// Cast to Light
	li, ok := lightEnt.(sdk.Light)
	if !ok {
		t.Fatal("Failed to cast to Light interface")
	}
	li.UpdateMetadata("My Light", "LIGHT_1")
	time.Sleep(100 * time.Millisecond)

	// Test SourceID lookup
	res, ok := b.GetBySourceID("LIGHT_1")
	if !ok {
		t.Fatal("SourceID lookup failed for LIGHT_1")
	}
	liRes, ok := res.(sdk.Light)
	if !ok || liRes.ID() != lightEnt.ID() {
		t.Errorf("SourceID lookup did not return Light interface. Type: %T", res)
	}

	// Test list entities
	ents, _ := device.GetEntities()
	if len(ents) != 2 {
		t.Errorf("Expected 2 entities, got %d", len(ents))
	}
}

func TestIntegration_InvisiblePersistence(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000003"
	b, _ := RegisterBundle(bundleID)

	device, _ := b.CreateDevice()
	deviceID := device.ID()

	device.UpdateMetadata("Persistence Test", "SRC_999")
	device.Disable("error")
	device.UpdateRaw(map[string]interface{}{"firmware": "1.0.1"})

	reloadedDevice, err := b.GetDevice(deviceID)
	if err != nil {
		t.Fatalf("Failed to reload device: %v", err)
	}

	if reloadedDevice.Metadata().Name != "Persistence Test" {
		t.Errorf("Metadata mismatch after reload")
	}
	if reloadedDevice.State().Enabled != false {
		t.Errorf("State mismatch after reload")
	}
	if reloadedDevice.Raw()["firmware"] != "1.0.1" {
		t.Errorf("Raw data mismatch after reload")
	}
}

func TestIntegration_MemoryConsistency(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000004"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_SWITCH)

	if len(b.GetDevices()) != 1 {
		t.Error("In-memory devices map inconsistent")
	}

	dev.DeleteEntity(ent.ID())
	ents, _ := dev.GetEntities()
	if len(ents) != 0 {
		t.Error("GetEntities inconsistency after delete")
	}
}

func TestIntegration_Messaging(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000005"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()
	light, _ := dev.CreateEntity(sdk.TYPE_LIGHT)

	commandReceived := make(chan string, 1)
	light.OnCommand(func(cmd string, payload map[string]interface{}) {
		commandReceived <- cmd
	})

	li := light.(sdk.Light)
	li.TurnOn()

	select {
	case cmd := <-commandReceived:
		if cmd != "TurnOn" {
			t.Errorf("Expected TurnOn, got %s", cmd)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out waiting for command")
	}
}

func TestIntegration_PanicRecovery(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000010"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()
	dev.OnCommand(func(cmd string, payload map[string]interface{}) {
		panic("intentional plugin panic")
	})

	b.Publish("device."+string(dev.ID())+".command", map[string]interface{}{"command": "PanicNow"})
	time.Sleep(1000 * time.Millisecond)

	logFile := filepath.Join("logs", bundleID, "log.folder", "current.log")
	content, _ := ioutil.ReadFile(logFile)
	if !strings.Contains(string(content), "PANIC RECOVERED") {
		t.Errorf("Panic was not logged. Logs: %s", string(content))
	}
}

func TestIntegration_LuaAdvancedAPI(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000014"
	b, _ := RegisterBundle(bundleID)

	devA, _ := b.CreateDevice()
	devB, _ := b.CreateDevice()

	luaA := `
		function onCommand(cmd, payload, ctx)
			if cmd == "start_task" then
				local run_id = ctx.BeginRun({ task = "test" })
				ctx.Log("Started run: " .. run_id)
				ctx.Progress(50)
				ctx.Complete("success")
			end
		end
		function onEvent(msg, ctx)
			if msg.payload.action == "find" then
				local res = { found_other_sid = false, found_other_uuid = false }
				local other = ctx.GetBySourceID("sid-b")
				if other then
					if tostring(other.ID) == tostring(msg.payload.target_id) then
						res.found_other_sid = true
					end
				end
				local byUUID = ctx.GetByUUID(msg.payload.target_id)
				if byUUID then
					res.found_other_uuid = true
				end
				ctx.UpdateRaw(res)
			end
		end
	`
	devA.UpdateScript(luaA)
	time.Sleep(500 * time.Millisecond)

	// 1. Test Lifecycle
	b.Publish("device."+string(devA.ID())+".command", map[string]interface{}{"command": "start_task"})

	var stateA sdk.DeviceState
	for i := 0; i < 50; i++ {
		stateA = devA.State()
		if stateA.Phase == "idle" && stateA.RunID == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if stateA.RunID != 1 || stateA.Status != "success" || stateA.Progress != 100 {
		t.Errorf("Lifecycle failed. State: %+v", stateA)
	}

	// 2. Test Global Lookups
	devB.UpdateScript(`function onCommand(cmd, p, ctx) if cmd=="set_sid" then ctx.UpdateMetadata("Device B", "sid-b") end end`)
	time.Sleep(500 * time.Millisecond)
	b.Publish("device."+string(devB.ID())+".command", map[string]interface{}{"command": "set_sid"})

	for i := 0; i < 50; i++ {
		if devB.Metadata().SourceID == "sid-b" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	b.(*bundleImpl).nc.Publish("device."+string(devA.ID())+".find_trigger", func() []byte {
		m := sdk.Message{Source: devA.ID(), Subject: "device." + string(devA.ID()) + ".find_trigger", Payload: map[string]interface{}{"action": "find", "target_id": string(devB.ID())}}
		bytes, _ := json.Marshal(m)
		return bytes
	}())

	var rawA map[string]interface{}
	for i := 0; i < 50; i++ {
		rawA = devA.Raw()
		if rawA["found_other_sid"] == true && rawA["found_other_uuid"] == true {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if rawA["found_other_sid"] != true {
		t.Errorf("GetBySourceID failed. Raw: %+v", rawA)
	}
	if rawA["found_other_uuid"] != true {
		t.Errorf("GetByUUID failed. Raw: %+v", rawA)
	}
}

func TestIntegration_Capabilities(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000015"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()
	bulb, _ := dev.CreateEntityEx(sdk.TYPE_LIGHT, []string{sdk.CAP_BRIGHTNESS, sdk.CAP_TEMPERATURE})

	lua := `
		function onEvent(msg, ctx)
			if msg.payload.action == "check" then
				local target = ctx.GetByUUID(msg.payload.id)
				if target then
					ctx.UpdateRaw({ 
						has_rgb = target.HasCapability("rgb"), 
						has_temp = target.HasCapability("temperature") 
					})
				end
			end
		end
	`
	dev.UpdateScript(lua)
	time.Sleep(500 * time.Millisecond)

	b.(*bundleImpl).nc.Publish("device."+string(dev.ID())+".check", func() []byte {
		m := sdk.Message{Source: dev.ID(), Subject: "device." + string(dev.ID()) + ".check", Payload: map[string]interface{}{"action": "check", "id": string(bulb.ID())}}
		bytes, _ := json.Marshal(m)
		return bytes
	}())

	var raw map[string]interface{}
	for i := 0; i < 50; i++ {
		raw = dev.Raw()
		if raw["has_temp"] == true {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if raw["has_rgb"] == true || raw["has_temp"] != true {
		t.Errorf("Capability check failed. Raw: %+v", raw)
	}
}

func TestIntegration_EnableDisable(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000016"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()
	dev.UpdateScript("function onEvent(msg,ctx) if msg.payload.action=='ping' then ctx.UpdateRaw({p=true}) end end")
	time.Sleep(500 * time.Millisecond)

	// 1. Enabled
	b.(*bundleImpl).nc.Publish("device."+string(dev.ID())+".ping", func() []byte {
		m := sdk.Message{Source: dev.ID(), Subject: "device." + string(dev.ID()) + ".ping", Payload: map[string]interface{}{"action": "ping"}}
		bytes, _ := json.Marshal(m)
		return bytes
	}())
	time.Sleep(200 * time.Millisecond)
	if dev.Raw()["p"] != true {
		t.Error("Failed to ping while enabled")
	}

	// 2. Disabled
	dev.Disable("off")
	dev.UpdateRaw(map[string]interface{}{"p": false})
	b.(*bundleImpl).nc.Publish("device."+string(dev.ID())+".ping", func() []byte {
		m := sdk.Message{Source: dev.ID(), Subject: "device." + string(dev.ID()) + ".ping", Payload: map[string]interface{}{"action": "ping"}}
		bytes, _ := json.Marshal(m)
		return bytes
	}())
	time.Sleep(200 * time.Millisecond)
	if dev.Raw()["p"] == true {
		t.Error("Responded while disabled")
	}

	// 3. Re-enabled
	dev.UpdateState("on")
	time.Sleep(500 * time.Millisecond)
	b.(*bundleImpl).nc.Publish("device."+string(dev.ID())+".ping", func() []byte {
		m := sdk.Message{Source: dev.ID(), Subject: "device." + string(dev.ID()) + ".ping", Payload: map[string]interface{}{"action": "ping"}}
		bytes, _ := json.Marshal(m)
		return bytes
	}())
	time.Sleep(200 * time.Millisecond)
	if dev.Raw()["p"] != true {
		t.Error("Failed to ping after re-enable")
	}
}

func TestIntegration_ExtendedEntityTypes(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000017"
	b, _ := RegisterBundle(bundleID)

	dev, _ := b.CreateDevice()

	// 1. Create and verify SENSOR
	sensor, _ := dev.CreateEntity(sdk.TYPE_SENSOR)
	if _, ok := sensor.(sdk.Sensor); !ok {
		t.Error("Failed to cast to Sensor interface")
	}

	// 2. Create and verify BINARY_SENSOR
	binSensor, _ := dev.CreateEntity(sdk.TYPE_BINARY_SENSOR)
	if _, ok := binSensor.(sdk.BinarySensor); !ok {
		t.Error("Failed to cast to BinarySensor interface")
	}

	// 3. Create and verify COVER
	cover, _ := dev.CreateEntity(sdk.TYPE_COVER)
	cObj, ok := cover.(sdk.Cover)
	if !ok {
		t.Error("Failed to cast to Cover interface")
	}

	// 4. Test Cover Commands reaching Lua
	lua := `
		function onCommand(cmd, payload, ctx)
			ctx.UpdateRaw({ last_cmd = cmd, pos = payload.position })
		end
	`
	cover.UpdateScript(lua)
	time.Sleep(200 * time.Millisecond)

	cObj.Open()
	time.Sleep(100 * time.Millisecond)
	if cover.Raw()["last_cmd"] != "Open" {
		t.Errorf("Cover Open failed to reach Lua. Got: %v", cover.Raw()["last_cmd"])
	}

	cObj.SetPosition(75)
	time.Sleep(100 * time.Millisecond)
	if cover.Raw()["pos"].(float64) != 75 {
		t.Errorf("Cover SetPosition failed to reach Lua. Got: %v", cover.Raw()["pos"])
	}
}
