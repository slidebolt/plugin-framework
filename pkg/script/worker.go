package script

import (
	"fmt"
	"github.com/slidebolt/plugin-sdk"
	"sync"

	"github.com/yuin/gopher-lua"
)

type Worker struct {
	id     sdk.UUID
	L      *lua.LState
	ctx    *lua.LTable
	mu     sync.Mutex
	logger sdk.Logger
}

type HostBridge interface {
	GetMetadata() map[string]interface{}
	GetState() map[string]interface{}
	GetRaw() map[string]interface{}
	GetDeviceRaw() map[string]interface{}
	UpdateMetadata(name string, sid sdk.SourceID) error
	UpdateState(status string) error
	UpdateProperties(props map[string]interface{}) error
	Disable(status string) error
	UpdateRaw(data map[string]interface{}) error
	Publish(subject string, payload map[string]interface{}) error
	Subscribe(topic string) error

	// Global Lookups
	GetByUUID(id sdk.UUID) (interface{}, bool)
	GetBySourceID(sid sdk.SourceID) (interface{}, bool)

	// Lifecycle
	BeginRun(meta map[string]interface{}) (int, error)
	Progress(percent int, meta map[string]interface{}) error
	Complete(status string, meta map[string]interface{}) error
	Fail(reason string, meta map[string]interface{}) error
}

func PreCompile(code string) (interface{}, error) {
	L := lua.NewState()
	defer L.Close()
	return L.LoadString(code)
}

func NewWorker(id sdk.UUID, code string, logger sdk.Logger, bridge HostBridge) (*Worker, error) {
	L := lua.NewState()

	w := &Worker{
		id:     id,
		L:      L,
		logger: logger,
	}

	// Setup ctx table
	w.ctx = L.NewTable()
	L.SetField(w.ctx, "ID", lua.LString(id))

	// Inject Methods
	L.SetField(w.ctx, "Log", L.NewFunction(func(L *lua.LState) int {
		msg := L.OptString(1, "")
		w.logger.Info("[Lua] %s", msg)
		return 0
	}))

	L.SetField(w.ctx, "GetMetadata", L.NewFunction(func(L *lua.LState) int {
		L.Push(ToLuaValue(L, bridge.GetMetadata()))
		return 1
	}))

	L.SetField(w.ctx, "GetState", L.NewFunction(func(L *lua.LState) int {
		L.Push(ToLuaValue(L, bridge.GetState()))
		return 1
	}))

	L.SetField(w.ctx, "GetRaw", L.NewFunction(func(L *lua.LState) int {
		L.Push(ToLuaValue(L, bridge.GetRaw()))
		return 1
	}))

	L.SetField(w.ctx, "GetDeviceRaw", L.NewFunction(func(L *lua.LState) int {
		L.Push(ToLuaValue(L, bridge.GetDeviceRaw()))
		return 1
	}))

	L.SetField(w.ctx, "UpdateMetadata", L.NewFunction(func(L *lua.LState) int {
		name := L.CheckString(1)
		sid := sdk.SourceID(L.CheckString(2))
		bridge.UpdateMetadata(name, sid)
		return 0
	}))

	L.SetField(w.ctx, "UpdateState", L.NewFunction(func(L *lua.LState) int {
		status := L.CheckString(1)
		bridge.UpdateState(status)
		return 0
	}))
	L.SetField(w.ctx, "UpdateProperties", L.NewFunction(func(L *lua.LState) int {
		payload := FromLuaValue(L.CheckTable(1)).(map[string]interface{})
		bridge.UpdateProperties(payload)
		return 0
	}))
	L.SetField(w.ctx, "Disable", L.NewFunction(func(L *lua.LState) int {
		status := L.CheckString(1)
		bridge.Disable(status)
		return 0
	}))

	L.SetField(w.ctx, "UpdateRaw", L.NewFunction(func(L *lua.LState) int {
		payload := FromLuaValue(L.CheckTable(1)).(map[string]interface{})
		bridge.UpdateRaw(payload)
		return 0
	}))

	L.SetField(w.ctx, "Publish", L.NewFunction(func(L *lua.LState) int {
		subject := L.CheckString(1)
		payload := FromLuaValue(L.CheckTable(2)).(map[string]interface{})
		bridge.Publish(subject, payload)
		return 0
	}))

	L.SetField(w.ctx, "Subscribe", L.NewFunction(func(L *lua.LState) int {
		topic := L.CheckString(1)
		err := bridge.Subscribe(topic)
		if err != nil {
			w.logger.Error("[Lua] Subscribe(%s) failed: %v", topic, err)
			L.Push(lua.LFalse)
		} else {
			L.Push(lua.LTrue)
		}
		return 1
	}))

	L.SetField(w.ctx, "GetByUUID", L.NewFunction(func(L *lua.LState) int {
		uuid := sdk.UUID(L.CheckString(1))
		obj, ok := bridge.GetByUUID(uuid)
		if !ok {
			fmt.Printf("[DEBUG-LUA] GetByUUID failed for: %s\n", uuid)
			L.Push(lua.LNil)
			return 1
		}
		L.Push(WrapObjectToLua(L, obj))
		return 1
	}))

	L.SetField(w.ctx, "GetBySourceID", L.NewFunction(func(L *lua.LState) int {
		sid := sdk.SourceID(L.CheckString(1))
		obj, ok := bridge.GetBySourceID(sid)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		L.Push(WrapObjectToLua(L, obj))
		return 1
	}))

	// Lifecycle API
	L.SetField(w.ctx, "BeginRun", L.NewFunction(func(L *lua.LState) int {
		meta := make(map[string]interface{})
		if L.GetTop() >= 1 {
			meta = FromLuaValue(L.CheckTable(1)).(map[string]interface{})
		}
		runID, _ := bridge.BeginRun(meta)
		L.Push(lua.LNumber(runID))
		return 1
	}))
	L.SetField(w.ctx, "Progress", L.NewFunction(func(L *lua.LState) int {
		percent := L.CheckInt(1)
		meta := make(map[string]interface{})
		if L.GetTop() >= 2 {
			meta = FromLuaValue(L.CheckTable(2)).(map[string]interface{})
		}
		bridge.Progress(percent, meta)
		return 0
	}))
	L.SetField(w.ctx, "Complete", L.NewFunction(func(L *lua.LState) int {
		status := L.OptString(1, "completed")
		meta := make(map[string]interface{})
		if L.GetTop() >= 2 {
			meta = FromLuaValue(L.CheckTable(2)).(map[string]interface{})
		}
		bridge.Complete(status, meta)
		return 0
	}))
	L.SetField(w.ctx, "Fail", L.NewFunction(func(L *lua.LState) int {
		reason := L.CheckString(1)
		meta := make(map[string]interface{})
		if L.GetTop() >= 2 {
			meta = FromLuaValue(L.CheckTable(2)).(map[string]interface{})
		}
		bridge.Fail(reason, meta)
		return 0
	}))

	// Execute the code to define functions
	if err := L.DoString(code); err != nil {
		L.Close()
		return nil, err
	}

	return w, nil
}

func (w *Worker) OnLoad() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn := w.L.GetGlobal("OnLoad")
	if fn.Type() != lua.LTFunction {
		return nil // Optional
	}
	return w.L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, w.ctx)
}

func (w *Worker) OnEvent(msg sdk.Message) error {

	w.mu.Lock()

	defer w.mu.Unlock()

	fn := w.L.GetGlobal("onEvent")

	if fn.Type() != lua.LTFunction {

		return nil

	}

	// Convert msg to table

	msgTable := w.L.NewTable()
	w.L.SetField(msgTable, "subject", lua.LString(msg.Subject))
	w.L.SetField(msgTable, "source", lua.LString(msg.Source))
	w.L.SetField(msgTable, "payload", ToLuaValue(w.L, msg.Payload))

	err := w.L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, msgTable, w.ctx)
	if err != nil {
		w.logger.Error("Lua OnEvent Error [%s]: %v", msg.Subject, err)
	}
	return err
}

func (w *Worker) OnCommand(cmd string, payload map[string]interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn := w.L.GetGlobal("onCommand")
	if fn.Type() != lua.LTFunction {
		return nil
	}

	return w.L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, lua.LString(cmd), ToLuaValue(w.L, payload), w.ctx)
}

func (w *Worker) Close() {
	w.L.Close()
}

func WrapObjectToLua(L *lua.LState, obj interface{}) *lua.LTable {
	t := L.NewTable()
	if dev, ok := obj.(sdk.Device); ok {
		id := dev.ID()
		L.SetField(t, "ID", lua.LString(id))
		L.SetField(t, "type", lua.LString("device"))
		L.SetField(t, "SendCommand", L.NewFunction(func(L *lua.LState) int {
			cmd := L.CheckString(1)
			payload := FromLuaValue(L.CheckTable(2)).(map[string]interface{})
			if payload == nil {
				payload = make(map[string]interface{})
			}
			payload["command"] = cmd
			dev.Publish("device."+string(id)+".command", payload)
			return 0
		}))
		return t
	}
	if ent, ok := obj.(sdk.Entity); ok {
		id := ent.ID()
		meta := ent.Metadata()
		L.SetField(t, "ID", lua.LString(id))
		L.SetField(t, "type", lua.LString("entity"))
		L.SetField(t, "HasCapability", L.NewFunction(func(L *lua.LState) int {
			cap := L.CheckString(1)
			for _, c := range meta.Capabilities {
				if c == cap {
					L.Push(lua.LTrue)
					return 1
				}
			}
			L.Push(lua.LFalse)
			return 1
		}))
		L.SetField(t, "SendCommand", L.NewFunction(func(L *lua.LState) int {
			cmd := L.CheckString(1)
			payload := FromLuaValue(L.CheckTable(2)).(map[string]interface{})
			if payload == nil {
				payload = make(map[string]interface{})
			}
			payload["command"] = cmd
			ent.Publish("entity."+string(id)+".command", payload)
			return 0
		}))
		return t
	}
	return t
}

// Helpers
func ToLuaValue(L *lua.LState, v interface{}) lua.LValue {
	switch val := v.(type) {
	case string:
		return lua.LString(val)
	case bool:
		return lua.LBool(val)
	case float64:
		return lua.LNumber(val)
	case int:
		return lua.LNumber(val)
	case sdk.UUID:
		return lua.LString(val)
	case map[string]interface{}:
		t := L.NewTable()
		for k, v2 := range val {
			L.SetField(t, k, ToLuaValue(L, v2))
		}
		return t
	case []interface{}:
		t := L.NewTable()
		for i, v2 := range val {
			L.RawSetInt(t, i+1, ToLuaValue(L, v2))
		}
		return t
	case []string:
		t := L.NewTable()
		for i, v2 := range val {
			L.RawSetInt(t, i+1, lua.LString(v2))
		}
		return t
	default:
		return lua.LNil
	}
}

func FromLuaValue(v lua.LValue) interface{} {
	switch val := v.(type) {
	case lua.LString:
		return string(val)
	case lua.LBool:
		return bool(val)
	case lua.LNumber:
		return float64(val)
	case *lua.LTable:
		// Check if it's an array or map
		isArr := true
		maxIdx := 0
		val.ForEach(func(k, v2 lua.LValue) {
			if k.Type() != lua.LTNumber {
				isArr = false
			} else {
				idx := int(k.(lua.LNumber))
				if idx > maxIdx {
					maxIdx = idx
				}
			}
		})

		if isArr && maxIdx > 0 {
			arr := make([]interface{}, maxIdx)
			for i := 1; i <= maxIdx; i++ {
				arr[i-1] = FromLuaValue(val.RawGetInt(i))
			}
			return arr
		}

		m := make(map[string]interface{})
		val.ForEach(func(k, v2 lua.LValue) {
			m[k.String()] = FromLuaValue(v2)
		})
		return m
	default:
		return nil
	}
}
