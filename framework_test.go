package framework

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	lua "github.com/yuin/gopher-lua"

	scriptpkg "github.com/slidebolt/plugin-framework/pkg/script"
	"github.com/slidebolt/plugin-sdk"
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

func TestIntegration_GetRemoteObject_EntityFromDevicePayload_IsEntityOnly(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000101")
	bi := b.(*bundleImpl)

	entityID := "11111111-1111-1111-1111-111111111111"
	deviceID := "22222222-2222-2222-2222-222222222222"

	payload := []byte(`{
		  "uuid":"` + deviceID + `",
		  "metadata":{"id":"` + deviceID + `","name":"Basement Device","source_id":"basement-dev","labels":["Basement"]},
		  "state":{"enabled":true,"status":"active","properties":{"power":true}},
		  "entities":{
		    "` + entityID + `":{
		      "deviceID":"` + deviceID + `",
		      "metadata":{"id":"` + entityID + `","entity_id":"` + entityID + `","name":"Basement Light","source_id":"basement-light","type":"LIGHT","capabilities":["rgb"],"labels":["Basement"]},
		      "state":{"enabled":true,"status":"active","properties":{"power":true}}
		    }
		  }
		}`)

	obj, ok := bi.parseRemoteObject(sdk.UUID(entityID), payload)
	if !ok {
		t.Fatal("expected remote object")
	}
	if _, ok := obj.(sdk.Entity); !ok {
		t.Fatalf("expected sdk.Entity, got %T", obj)
	}
	if _, ok := obj.(sdk.Device); ok {
		t.Fatalf("remote entity proxy must not implement sdk.Device; got %T", obj)
	}
}

func TestIntegration_GetRemoteObject_RemoteCommandRouting(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000102")
	bi := b.(*bundleImpl)

	entityID := "33333333-3333-3333-3333-333333333333"
	deviceID := "44444444-4444-4444-4444-444444444444"

	entityPayload := []byte(`{
	  "uuid":"` + deviceID + `",
	  "metadata":{"id":"` + deviceID + `","name":"Remote Device","source_id":"remote-dev","labels":["Basement"]},
	  "state":{"enabled":true,"status":"active","properties":{}},
	  "entities":{
	    "` + entityID + `":{
	      "deviceID":"` + deviceID + `",
	      "metadata":{"id":"` + entityID + `","entity_id":"` + entityID + `","name":"Remote Light","source_id":"remote-light","type":"LIGHT","capabilities":["rgb"],"labels":["Basement"]},
	      "state":{"enabled":true,"status":"active","properties":{}}
	    }
	  }
	}`)
	devicePayload := []byte(`{
	  "uuid":"` + deviceID + `",
	  "metadata":{"id":"` + deviceID + `","name":"Remote Device","source_id":"remote-dev","labels":["Basement"]},
	  "state":{"enabled":true,"status":"active","properties":{}},
	  "entities":{}
	}`)

	entityMsgCh := make(chan sdk.Message, 1)
	deviceMsgCh := make(chan sdk.Message, 1)
	entCmdSub, _ := sharedNC.Subscribe("entity."+entityID+".command", func(msg *nats.Msg) {
		var m sdk.Message
		_ = json.Unmarshal(msg.Data, &m)
		entityMsgCh <- m
	})
	defer entCmdSub.Unsubscribe()
	devCmdSub, _ := sharedNC.Subscribe("device."+deviceID+".command", func(msg *nats.Msg) {
		var m sdk.Message
		_ = json.Unmarshal(msg.Data, &m)
		deviceMsgCh <- m
	})
	defer devCmdSub.Unsubscribe()

	robj, ok := bi.parseRemoteObject(sdk.UUID(entityID), entityPayload)
	if !ok {
		t.Fatal("expected remote entity object")
	}
	light, ok := robj.(sdk.Light)
	if !ok {
		t.Fatalf("expected sdk.Light remote proxy, got %T", robj)
	}
	if err := light.SetRGB(1, 2, 3); err != nil {
		t.Fatalf("SetRGB failed: %v", err)
	}
	select {
	case m := <-entityMsgCh:
		if m.Subject != "entity."+entityID+".command" {
			t.Fatalf("unexpected entity command subject: %s", m.Subject)
		}
		if cmd, _ := m.Payload["command"].(string); cmd != "SetRGB" {
			t.Fatalf("unexpected entity command payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for remote entity command")
	}

	dobj, ok := bi.parseRemoteObject(sdk.UUID(deviceID), devicePayload)
	if !ok {
		t.Fatal("expected remote device object")
	}
	if _, ok := dobj.(sdk.Entity); ok {
		t.Fatalf("remote device proxy must not implement sdk.Entity; got %T", dobj)
	}
	sender, ok := dobj.(interface {
		SendCommand(string, map[string]interface{}) error
	})
	if !ok {
		t.Fatalf("expected command sender device proxy, got %T", dobj)
	}
	if err := sender.SendCommand("TurnOn", nil); err != nil {
		t.Fatalf("device SendCommand failed: %v", err)
	}
	select {
	case m := <-deviceMsgCh:
		if m.Subject != "device."+deviceID+".command" {
			t.Fatalf("unexpected device command subject: %s", m.Subject)
		}
		if cmd, _ := m.Payload["command"].(string); cmd != "TurnOn" {
			t.Fatalf("unexpected device command payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for remote device command")
	}
}

func TestIntegration_WrapObjectToLua_RemoteEntityProxy_IsEntityAndRoutesEntityCommand(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000103")
	bi := b.(*bundleImpl)

	entityID := sdk.UUID("55555555-5555-5555-5555-555555555555")
	deviceID := sdk.UUID("66666666-6666-6666-6666-666666666666")
	rp := &remoteEntityProxy{
		remoteProxyBase: remoteProxyBase{id: entityID, deviceID: deviceID, bundle: bi},
		metadata: sdk.EntityMetadata{
			ID:       entityID,
			EntityID: entityID,
			Name:     "Remote Light",
			SourceID: "remote-light",
			Type:     sdk.TYPE_LIGHT,
		},
		state: sdk.EntityState{Enabled: true, Status: "active"},
	}

	ch := make(chan sdk.Message, 1)
	sub, err := sharedNC.Subscribe("entity."+string(entityID)+".command", func(msg *nats.Msg) {
		var m sdk.Message
		_ = json.Unmarshal(msg.Data, &m)
		ch <- m
	})
	if err != nil {
		t.Fatalf("subscribe entity command: %v", err)
	}
	defer sub.Unsubscribe()

	L := lua.NewState()
	defer L.Close()
	objTable := scriptpkg.WrapObjectToLua(L, rp)

	if got := L.GetField(objTable, "type").String(); got != "entity" {
		t.Fatalf("expected Lua object type=entity, got %q", got)
	}

	sendFn := L.GetField(objTable, "SendCommand")
	if sendFn.Type() != lua.LTFunction {
		t.Fatalf("SendCommand not exposed as function; got %s", sendFn.Type())
	}
	payload := L.NewTable()
	L.SetField(payload, "r", lua.LNumber(1))
	L.SetField(payload, "g", lua.LNumber(2))
	L.SetField(payload, "b", lua.LNumber(3))
	if err := L.CallByParam(lua.P{Fn: sendFn, NRet: 0, Protect: true}, lua.LString("SetRGB"), payload); err != nil {
		t.Fatalf("Lua SendCommand call failed: %v", err)
	}

	select {
	case m := <-ch:
		if m.Subject != "entity."+string(entityID)+".command" {
			t.Fatalf("unexpected subject: %s", m.Subject)
		}
		if cmd, _ := m.Payload["command"].(string); cmd != "SetRGB" {
			t.Fatalf("unexpected payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for entity command from Lua wrapper")
	}
}

func TestIntegration_ParseRemoteObject_BareEntityPayload_IsEntityOnly(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000104")
	bi := b.(*bundleImpl)

	entityID := "77777777-7777-7777-7777-777777777777"
	deviceID := "88888888-8888-8888-8888-888888888888"
	payload := []byte(`{
	  "id":"` + entityID + `",
	  "deviceID":"` + deviceID + `",
	  "metadata":{"id":"` + entityID + `","entity_id":"` + entityID + `","name":"Bare Remote Light","source_id":"bare-remote-light","type":"LIGHT","capabilities":["rgb"],"labels":["Basement"]},
	  "state":{"enabled":true,"status":"active","properties":{"power":true}}
	}`)

	obj, ok := bi.parseRemoteObject(sdk.UUID(entityID), payload)
	if !ok {
		t.Fatal("expected remote entity object from bare entity payload")
	}
	if _, ok := obj.(sdk.Entity); !ok {
		t.Fatalf("expected sdk.Entity, got %T", obj)
	}
	if _, ok := obj.(sdk.Device); ok {
		t.Fatalf("bare remote entity proxy must not implement sdk.Device; got %T", obj)
	}
}

func TestIntegration_RemoteProxyUnsupportedMutation_ReturnsError(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000105")
	bi := b.(*bundleImpl)

	entityID := sdk.UUID("99999999-9999-9999-9999-999999999999")
	deviceID := sdk.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	re := &remoteEntityProxy{
		remoteProxyBase: remoteProxyBase{id: entityID, deviceID: deviceID, bundle: bi},
		metadata:        sdk.EntityMetadata{ID: entityID, EntityID: entityID, Type: sdk.TYPE_SWITCH},
	}
	rd := &remoteDeviceProxy{
		remoteProxyBase: remoteProxyBase{id: deviceID, deviceID: deviceID, bundle: bi},
		metadata:        sdk.DeviceMetadata{ID: deviceID},
	}

	if err := re.UpdateState("active"); err == nil {
		t.Fatal("expected remote entity UpdateState to return error")
	}
	if err := re.UpdateProperties(map[string]interface{}{"power": true}); err == nil {
		t.Fatal("expected remote entity UpdateProperties to return error")
	}
	if err := rd.UpdateRaw(map[string]interface{}{"x": 1}); err == nil {
		t.Fatal("expected remote device UpdateRaw to return error")
	}
	if err := rd.SetLabels([]string{"Basement"}); err == nil {
		t.Fatal("expected remote device SetLabels to return error")
	}
}

func TestIntegration_LocalObject_SendCommandParityAndLuaWrapperRouting(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000106")

	dev, _ := b.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_LIGHT)
	di := dev.(*deviceImpl)
	entitySender, ok := ent.(interface {
		SendCommand(string, map[string]interface{}) error
	})
	if !ok {
		t.Fatalf("local entity should implement command sender; got %T", ent)
	}

	deviceMsgCh := make(chan sdk.Message, 2)
	entityMsgCh := make(chan sdk.Message, 2)
	devSub, err := sharedNC.Subscribe("device."+string(di.ID())+".command", func(msg *nats.Msg) {
		var m sdk.Message
		_ = json.Unmarshal(msg.Data, &m)
		deviceMsgCh <- m
	})
	if err != nil {
		t.Fatalf("subscribe device command: %v", err)
	}
	defer devSub.Unsubscribe()
	entSub, err := sharedNC.Subscribe("entity."+string(ent.ID())+".command", func(msg *nats.Msg) {
		var m sdk.Message
		_ = json.Unmarshal(msg.Data, &m)
		entityMsgCh <- m
	})
	if err != nil {
		t.Fatalf("subscribe entity command: %v", err)
	}
	defer entSub.Unsubscribe()

	if err := di.SendCommand("TurnOn", nil); err != nil {
		t.Fatalf("local device SendCommand failed: %v", err)
	}
	if err := entitySender.SendCommand("SetRGB", map[string]interface{}{"r": 9, "g": 8, "b": 7}); err != nil {
		t.Fatalf("local entity SendCommand failed: %v", err)
	}

	select {
	case m := <-deviceMsgCh:
		if m.Subject != "device."+string(di.ID())+".command" {
			t.Fatalf("unexpected local device subject: %s", m.Subject)
		}
		if cmd, _ := m.Payload["command"].(string); cmd != "TurnOn" {
			t.Fatalf("unexpected local device payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for local device command")
	}
	select {
	case m := <-entityMsgCh:
		if m.Subject != "entity."+string(ent.ID())+".command" {
			t.Fatalf("unexpected local entity subject: %s", m.Subject)
		}
		if cmd, _ := m.Payload["command"].(string); cmd != "SetRGB" {
			t.Fatalf("unexpected local entity payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for local entity command")
	}

	L := lua.NewState()
	defer L.Close()

	devObj := scriptpkg.WrapObjectToLua(L, di)
	if got := L.GetField(devObj, "type").String(); got != "device" {
		t.Fatalf("expected Lua local device type=device, got %q", got)
	}
	devSendFn := L.GetField(devObj, "SendCommand")
	if err := L.CallByParam(lua.P{Fn: devSendFn, NRet: 0, Protect: true}, lua.LString("Toggle")); err != nil {
		t.Fatalf("Lua local device SendCommand failed: %v", err)
	}

	entObj := scriptpkg.WrapObjectToLua(L, ent)
	if got := L.GetField(entObj, "type").String(); got != "entity" {
		t.Fatalf("expected Lua local entity type=entity, got %q", got)
	}
	entSendFn := L.GetField(entObj, "SendCommand")
	payload := L.NewTable()
	L.SetField(payload, "r", lua.LNumber(1))
	L.SetField(payload, "g", lua.LNumber(2))
	L.SetField(payload, "b", lua.LNumber(3))
	if err := L.CallByParam(lua.P{Fn: entSendFn, NRet: 0, Protect: true}, lua.LString("SetRGB"), payload); err != nil {
		t.Fatalf("Lua local entity SendCommand failed: %v", err)
	}

	select {
	case m := <-deviceMsgCh:
		if cmd, _ := m.Payload["command"].(string); cmd != "Toggle" {
			t.Fatalf("unexpected Lua local device payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Lua local device command")
	}
	select {
	case m := <-entityMsgCh:
		if cmd, _ := m.Payload["command"].(string); cmd != "SetRGB" {
			t.Fatalf("unexpected Lua local entity payload: %+v", m.Payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Lua local entity command")
	}
}

func TestIntegration_GetByLabel_MixedLocalRemote_IdentityAndDedup(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000107")

	dev, _ := b.CreateDevice()
	if err := dev.AddLabel("Basement"); err != nil {
		t.Fatalf("add device label: %v", err)
	}
	ent, _ := dev.CreateEntity(sdk.TYPE_LIGHT)
	if err := ent.AddLabel("Basement"); err != nil {
		t.Fatalf("add entity label: %v", err)
	}

	localEntityID := string(ent.ID())
	remoteEntityID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	remoteDeviceID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	searchHits := make(chan struct{}, 4)
	getObjHits := make(chan string, 4)

	searchSub, err := sharedNC.Subscribe("registry.search_by_label", func(msg *nats.Msg) {
		searchHits <- struct{}{}
		if string(msg.Data) != "Basement" {
			_ = msg.Respond([]byte("[]"))
			return
		}
		ids := []string{localEntityID, remoteEntityID}
		data, _ := json.Marshal(ids)
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("subscribe registry.search_by_label: %v", err)
	}
	defer searchSub.Unsubscribe()

	getObjSub, err := sharedNC.Subscribe("registry.get_object", func(msg *nats.Msg) {
		getObjHits <- string(msg.Data)
		switch string(msg.Data) {
		case remoteEntityID:
			payload := []byte(`{
			  "uuid":"` + remoteDeviceID + `",
			  "metadata":{"id":"` + remoteDeviceID + `","name":"Remote Basement Device","source_id":"remote-basement-dev","labels":["Basement"]},
			  "state":{"enabled":true,"status":"active","properties":{}},
			  "entities":{
			    "` + remoteEntityID + `":{
			      "deviceID":"` + remoteDeviceID + `",
			      "metadata":{"id":"` + remoteEntityID + `","entity_id":"` + remoteEntityID + `","name":"Remote Basement Light","source_id":"remote-basement-light","type":"LIGHT","capabilities":["rgb"],"labels":["Basement"]},
			      "state":{"enabled":true,"status":"active","properties":{"power":true}}
			    }
			  }
			}`)
			_ = msg.Respond(payload)
		default:
			_ = msg.Respond([]byte("{}"))
		}
	})
	if err != nil {
		t.Fatalf("subscribe registry.get_object: %v", err)
	}
	defer getObjSub.Unsubscribe()
	if err := sharedNC.Flush(); err != nil {
		t.Fatalf("flush registry responders: %v", err)
	}

	var objs []interface{}
	for i := 0; i < 20; i++ {
		objs = GetByLabel("Basement")
		if len(objs) >= 3 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(objs) < 3 {
		var seenSearch, seenGetObject bool
		select {
		case <-searchHits:
			seenSearch = true
		default:
		}
		select {
		case <-getObjHits:
			seenGetObject = true
		default:
		}
		t.Fatalf("expected at least 3 basement objects (local device + local entity + remote entity), got %d (searchHit=%v getObjectHit=%v)", len(objs), seenSearch, seenGetObject)
	}

	var localEntityCount int
	var remoteEntityFound bool
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if e, ok := obj.(sdk.Entity); ok {
			if _, ok := obj.(sdk.Device); ok {
				t.Fatalf("entity object should not also implement sdk.Device: %T", obj)
			}
			switch string(e.ID()) {
			case localEntityID:
				localEntityCount++
			case remoteEntityID:
				remoteEntityFound = true
			}
		}
	}

	if localEntityCount != 1 {
		t.Fatalf("expected exactly one local entity after registry dedup, got %d", localEntityCount)
	}
	if !remoteEntityFound {
		t.Fatal("expected remote basement entity in mixed GetByLabel results")
	}
}

func TestIntegration_WrapObjectToLua_RemoteEntityProxy_CommandErrorPropagatesToLua(t *testing.T) {
	setupIntegrationTest(t)

	entityID := sdk.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
	deviceID := sdk.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	rp := &remoteEntityProxy{
		remoteProxyBase: remoteProxyBase{id: entityID, deviceID: deviceID, bundle: nil},
		metadata: sdk.EntityMetadata{
			ID:       entityID,
			EntityID: entityID,
			Name:     "Remote Light No Transport",
			Type:     sdk.TYPE_LIGHT,
		},
		state: sdk.EntityState{Enabled: true, Status: "active"},
	}

	L := lua.NewState()
	defer L.Close()
	objTable := scriptpkg.WrapObjectToLua(L, rp)
	sendFn := L.GetField(objTable, "SendCommand")
	if sendFn.Type() != lua.LTFunction {
		t.Fatalf("SendCommand not exposed; got %s", sendFn.Type())
	}

	err := L.CallByParam(lua.P{Fn: sendFn, NRet: 0, Protect: true}, lua.LString("TurnOn"))
	if err == nil {
		t.Fatal("expected Lua SendCommand to return error when remote transport is unavailable")
	}
	if !strings.Contains(err.Error(), "SendCommand(TurnOn) failed for entity") {
		t.Fatalf("unexpected Lua error: %v", err)
	}
}

func TestIntegration_LocalCommandTransportUnavailable_ReturnsError(t *testing.T) {
	setupIntegrationTest(t)

	deviceID := sdk.UUID("f1111111-1111-1111-1111-111111111111")
	entityID := sdk.UUID("f2222222-2222-2222-2222-222222222222")

	d := &deviceImpl{id: deviceID, bundle: nil}
	if err := d.SendCommand("TurnOn", nil); err == nil {
		t.Fatal("expected local device SendCommand to fail when transport is unavailable")
	}

	e := &entityImpl{id: entityID, deviceID: deviceID, bundle: nil}
	if err := e.SendCommand("SetRGB", map[string]interface{}{"r": 1, "g": 2, "b": 3}); err == nil {
		t.Fatal("expected local entity SendCommand to fail when transport is unavailable")
	}

	li := &lightImpl{entityImpl: e}
	if err := li.SetRGB(1, 2, 3); err == nil {
		t.Fatal("expected local light SetRGB to fail when transport is unavailable")
	}
	if err := li.TurnOn(); err == nil {
		t.Fatal("expected local light TurnOn to fail when transport is unavailable")
	}

	L := lua.NewState()
	defer L.Close()
	objTable := scriptpkg.WrapObjectToLua(L, li)
	sendFn := L.GetField(objTable, "SendCommand")
	if sendFn.Type() != lua.LTFunction {
		t.Fatalf("SendCommand not exposed; got %s", sendFn.Type())
	}
	if err := L.CallByParam(lua.P{Fn: sendFn, NRet: 0, Protect: true}, lua.LString("TurnOn")); err == nil {
		t.Fatal("expected Lua SendCommand to fail for local light when transport is unavailable")
	}
}

func TestIntegration_EntityScriptReload_DoesNotAccumulateCommandHandlers(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000108")

	dev, _ := b.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_SWITCH)
	sw, ok := ent.(*switchImpl)
	if !ok {
		t.Fatalf("expected *switchImpl, got %T", ent)
	}
	ei := sw.entityImpl

	scriptA := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("active") end end`
	scriptB := `function onCommand(cmd,p,ctx) if cmd=="TurnOff" then ctx.UpdateState("idle") end end`

	if err := ent.UpdateScript(scriptA); err != nil {
		t.Fatalf("first UpdateScript failed: %v", err)
	}
	if err := ent.UpdateScript(scriptB); err != nil {
		t.Fatalf("second UpdateScript failed: %v", err)
	}

	ei.mu.RLock()
	defer ei.mu.RUnlock()
	if len(ei.handlers) != 0 {
		t.Fatalf("script reload should not accumulate user handlers; got %d", len(ei.handlers))
	}
	if ei.scriptCmdHandler == nil {
		t.Fatal("expected scriptCmdHandler to be installed after script reload")
	}
}

func TestIntegration_EntityScriptOnly_CommandSubscriptionSurvivesReload(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000109")

	dev, _ := b.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_SWITCH)
	sw, ok := ent.(*switchImpl)
	if !ok {
		t.Fatalf("expected *switchImpl, got %T", ent)
	}
	ei := sw.entityImpl

	scriptA := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("active-a") end end`
	scriptB := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("active-b") end end`

	if err := ent.UpdateScript(scriptA); err != nil {
		t.Fatalf("first UpdateScript failed: %v", err)
	}
	if err := ent.UpdateScript(scriptB); err != nil {
		t.Fatalf("second UpdateScript failed: %v", err)
	}

	b.Publish("entity."+string(ent.ID())+".command", map[string]interface{}{"command": "TurnOn"})

	for i := 0; i < 50; i++ {
		if ei.State().Status == "active-b" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected script-only entity command to be delivered after reload; status=%q", ei.State().Status)
}

func TestIntegration_DeviceScriptOnly_CommandSubscriptionSurvivesReload(t *testing.T) {
	setupIntegrationTest(t)
	b, _ := RegisterBundle("00000000-0000-0000-0000-000000000110")

	dev, _ := b.CreateDevice()
	di := dev.(*deviceImpl)

	scriptA := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("active-a") end end`
	scriptB := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("active-b") end end`

	if err := dev.UpdateScript(scriptA); err != nil {
		t.Fatalf("first UpdateScript failed: %v", err)
	}
	if err := dev.UpdateScript(scriptB); err != nil {
		t.Fatalf("second UpdateScript failed: %v", err)
	}

	b.Publish("device."+string(dev.ID())+".command", map[string]interface{}{"command": "TurnOn"})

	for i := 0; i < 50; i++ {
		if di.State().Status == "active-b" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected script-only device command to be delivered after reload; status=%q", di.State().Status)
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

// TestIntegration_LoadFromDisk verifies that all state written during a
// "first boot" is fully read back into memory on a simulated restart before
// any new devices or entities are created.  This is the core guarantee: read
// existing state first, create new state only when something is genuinely
// absent.
func TestIntegration_LoadFromDisk(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000020"

	// --- First boot: create a device with two entities and set state on each.
	b1, err := RegisterBundle(bundleID)
	if err != nil {
		t.Fatalf("first RegisterBundle: %v", err)
	}

	dev, _ := b1.CreateDevice()
	devID := dev.ID()
	dev.UpdateMetadata("Sensor Hub", "hub-1")
	dev.UpdateRaw(map[string]interface{}{"firmware": "2.0"})

	ent1, _ := dev.CreateEntity(sdk.TYPE_SENSOR)
	ent1.UpdateMetadata("Temperature", "temp-1")
	ent1.UpdateRaw(map[string]interface{}{"value": 21.5})

	ent2, _ := dev.CreateEntity(sdk.TYPE_SWITCH)
	ent2.UpdateMetadata("Power Switch", "switch-1")

	ent1ID := ent1.ID()
	ent2ID := ent2.ID()

	// Simulate process exit: release the bundle lock so a second registration
	// succeeds, and wipe the in-memory registry.
	_ = syscall.Flock(int(b1.(*bundleImpl).lockFile.Fd()), syscall.LOCK_UN)
	_ = b1.(*bundleImpl).lockFile.Close()

	bundlesMu.Lock()
	bundles = make(map[sdk.UUID]*bundleImpl)
	bundlesMu.Unlock()

	// --- Simulated restart: RegisterBundle must read all state from disk
	// before the caller gets a chance to call CreateDevice.
	b2, err := RegisterBundle(bundleID)
	if err != nil {
		t.Fatalf("second RegisterBundle: %v", err)
	}

	// 1. Device must be present in memory immediately — no CreateDevice call needed.
	reloaded, err := b2.GetDevice(devID)
	if err != nil {
		t.Fatalf("device not loaded from disk: %v", err)
	}
	if reloaded.Metadata().Name != "Sensor Hub" {
		t.Errorf("device name: got %q, want %q", reloaded.Metadata().Name, "Sensor Hub")
	}
	if reloaded.Raw()["firmware"] != "2.0" {
		t.Errorf("device raw: got %v, want 2.0", reloaded.Raw()["firmware"])
	}

	// 2. Both entities must be present and their state intact.
	ents, _ := reloaded.GetEntities()
	if len(ents) != 2 {
		t.Errorf("entity count after reload: got %d, want 2", len(ents))
	}

	obj1, ok := b2.GetBySourceID("temp-1")
	if !ok {
		t.Fatal("temp-1 entity not found by source ID after reload")
	}
	if obj1.(sdk.Entity).ID() != ent1ID {
		t.Errorf("temp-1 UUID mismatch after reload")
	}
	if obj1.(sdk.Entity).Raw()["value"] != 21.5 {
		t.Errorf("temp-1 raw not reloaded: %v", obj1.(sdk.Entity).Raw())
	}

	_, ok = b2.GetBySourceID("switch-1")
	if !ok {
		t.Fatal("switch-1 entity not found by source ID after reload")
	}

	// 3. No duplicate devices: GetDevices must return exactly the one reloaded device.
	devs := b2.GetDevices()
	if len(devs) != 1 {
		t.Errorf("device count after reload: got %d, want 1", len(devs))
	}

	// 4. In-memory entity map must be consistent with what's on disk.
	b2impl := b2.(*bundleImpl)
	b2impl.mu.RLock()
	entityCount := len(b2impl.entities)
	b2impl.mu.RUnlock()
	if entityCount != 2 {
		t.Errorf("b.entities count: got %d, want 2", entityCount)
	}

	_ = ent2ID // referenced above via switch-1 lookup
}

func TestIntegration_LoadFromDisk_ScriptOnlyCommandSubscriptions_WorkAfterRestart(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000021"

	// First boot: create scripted device + scripted entity, but no user OnCommand handlers.
	b1, err := RegisterBundle(bundleID)
	if err != nil {
		t.Fatalf("first RegisterBundle: %v", err)
	}

	dev, _ := b1.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_SWITCH)

	entityScript := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("entity-startup-ok") end end`
	deviceScript := `function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("device-startup-ok") end end`
	if err := ent.UpdateScript(entityScript); err != nil {
		t.Fatalf("entity UpdateScript: %v", err)
	}
	if err := dev.UpdateScript(deviceScript); err != nil {
		t.Fatalf("device UpdateScript: %v", err)
	}

	devID := dev.ID()
	entID := ent.ID()

	// Simulate process exit cleanly enough for a restart test:
	// release lock, drop in-memory bundle registry, and tear down the shared NATS
	// connection so old subscriptions cannot satisfy the assertions.
	_ = syscall.Flock(int(b1.(*bundleImpl).lockFile.Fd()), syscall.LOCK_UN)
	_ = b1.(*bundleImpl).lockFile.Close()
	bundlesMu.Lock()
	bundles = make(map[sdk.UUID]*bundleImpl)
	bundlesMu.Unlock()
	if sharedNC != nil {
		sharedNC.Close()
		sharedNC = nil
	}

	// Second boot: bundle must load scripts from disk, init workers, and install command subs.
	b2, err := RegisterBundle(bundleID)
	if err != nil {
		t.Fatalf("second RegisterBundle: %v", err)
	}

	reloadedDev, err := b2.GetDevice(devID)
	if err != nil {
		t.Fatalf("reloaded device missing: %v", err)
	}
	reloadedEntAny, ok := GetByUUID(entID)
	if !ok {
		t.Fatalf("reloaded entity missing: %s", entID)
	}
	reloadedEnt, ok := reloadedEntAny.(sdk.Entity)
	if !ok {
		t.Fatalf("reloaded entity wrong type: %T", reloadedEntAny)
	}

	// Production-style envelopes over NATS subjects.
	if err := b2.Publish("entity."+string(entID)+".command", map[string]interface{}{"command": "TurnOn", "action": "TurnOn"}); err != nil {
		t.Fatalf("publish entity command: %v", err)
	}
	if err := b2.Publish("device."+string(devID)+".command", map[string]interface{}{"command": "TurnOn", "action": "TurnOn"}); err != nil {
		t.Fatalf("publish device command: %v", err)
	}

	// Both scripted command handlers should fire after startup load.
	for i := 0; i < 100; i++ {
		if reloadedEnt.State().Status == "entity-startup-ok" && reloadedDev.State().Status == "device-startup-ok" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected startup-loaded script command handlers to run; entity=%q device=%q",
		reloadedEnt.State().Status, reloadedDev.State().Status)
}

func TestIntegration_BundleHealth_ReportsScriptCommandReadiness(t *testing.T) {
	setupIntegrationTest(t)
	bundleID := "00000000-0000-0000-0000-000000000022"
	b, err := RegisterBundle(bundleID)
	if err != nil {
		t.Fatalf("RegisterBundle: %v", err)
	}

	dev, _ := b.CreateDevice()
	ent, _ := dev.CreateEntity(sdk.TYPE_SWITCH)
	if err := dev.UpdateScript(`function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("ok") end end`); err != nil {
		t.Fatalf("device UpdateScript: %v", err)
	}
	if err := ent.UpdateScript(`function onCommand(cmd,p,ctx) if cmd=="TurnOn" then ctx.UpdateState("ok") end end`); err != nil {
		t.Fatalf("entity UpdateScript: %v", err)
	}

	bi := b.(*bundleImpl)
	resp, err := bi.nc.Request("bundle."+bundleID+".health", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}

	var health struct {
		BundleID  string `json:"bundle_id"`
		Status    string `json:"status"`
		Framework struct {
			NATSConnected         bool `json:"nats_connected"`
			RPCReady              bool `json:"rpc_ready"`
			DevicesTotal          int  `json:"devices_total"`
			EntitiesTotal         int  `json:"entities_total"`
			ScriptedDevicesTotal  int  `json:"scripted_devices_total"`
			ScriptedEntitiesTotal int  `json:"scripted_entities_total"`
		} `json:"framework"`
		CommandRuntime struct {
			ScriptedDevices []struct {
				ID                            string `json:"id"`
				Kind                          string `json:"kind"`
				WorkerLoaded                  bool   `json:"worker_loaded"`
				CommandSubscriptionInstalled  bool   `json:"command_subscription_installed"`
				ScriptCommandHandlerInstalled bool   `json:"script_command_handler_installed"`
			} `json:"scripted_devices"`
			ScriptedEntities []struct {
				ID                            string `json:"id"`
				Kind                          string `json:"kind"`
				WorkerLoaded                  bool   `json:"worker_loaded"`
				CommandSubscriptionInstalled  bool   `json:"command_subscription_installed"`
				ScriptCommandHandlerInstalled bool   `json:"script_command_handler_installed"`
			} `json:"scripted_entities"`
		} `json:"command_runtime"`
	}
	if err := json.Unmarshal(resp.Data, &health); err != nil {
		t.Fatalf("invalid health json: %v", err)
	}

	if health.BundleID != bundleID {
		t.Fatalf("bundle_id mismatch: got=%s want=%s", health.BundleID, bundleID)
	}
	if health.Status != "ok" {
		t.Fatalf("unexpected health status: %s", health.Status)
	}
	if !health.Framework.NATSConnected || !health.Framework.RPCReady {
		t.Fatalf("expected nats/rpc healthy: %+v", health.Framework)
	}
	if health.Framework.ScriptedDevicesTotal < 1 || health.Framework.ScriptedEntitiesTotal < 1 {
		t.Fatalf("expected scripted device/entity counts, got: %+v", health.Framework)
	}

	var foundDev, foundEnt bool
	for _, d := range health.CommandRuntime.ScriptedDevices {
		if d.ID == string(dev.ID()) {
			foundDev = true
			if d.Kind != "device" || !d.WorkerLoaded || !d.CommandSubscriptionInstalled || !d.ScriptCommandHandlerInstalled {
				t.Fatalf("unexpected scripted device health: %+v", d)
			}
		}
	}
	for _, e := range health.CommandRuntime.ScriptedEntities {
		if e.ID == string(ent.ID()) {
			foundEnt = true
			if e.Kind != "entity" || !e.WorkerLoaded || !e.CommandSubscriptionInstalled || !e.ScriptCommandHandlerInstalled {
				t.Fatalf("unexpected scripted entity health: %+v", e)
			}
		}
	}
	if !foundDev || !foundEnt {
		t.Fatalf("expected scripted objects in health response; foundDev=%v foundEnt=%v", foundDev, foundEnt)
	}
}
