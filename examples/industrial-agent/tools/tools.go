package tools

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SensorDataPoint represents one industrial sensor reading.
type SensorDataPoint struct {
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature"` // Celsius
	CoolantFlow float64 `json:"coolantFlow"` // L/min (normal: 40-50)
	SpindleRPM  int     `json:"spindleRpm"`  // RPM
	Vibration   float64 `json:"vibration"`   // mm/s (normal: < 2.5)
}

// SensorQueryResult is returned by industrial.sensor.query.
type SensorQueryResult struct {
	EquipmentID      string            `json:"equipmentId"`
	EquipmentName    string            `json:"equipmentName"`
	EquipmentType    string            `json:"equipmentType"`
	TimeRangeMinutes int               `json:"timeRangeMinutes"`
	CurrentStatus    string            `json:"currentStatus"`
	Readings         []SensorDataPoint `json:"readings"`
	Anomalies        []string          `json:"anomalies"`
}

// AlarmInfo is returned by industrial.alarm.lookup.
type AlarmInfo struct {
	AlarmCode        string   `json:"alarmCode"`
	AlarmTitle       string   `json:"alarmTitle"`
	Severity         string   `json:"severity"` // CRITICAL, WARNING, INFO
	Component        string   `json:"component"`
	TriggerCondition string   `json:"triggerCondition"`
	HistoricalStats  string   `json:"historicalStats"`
	PossibleCauses   []string `json:"possibleCauses"`
}

// SOPGuideline is returned by industrial.sop.search.
type SOPGuideline struct {
	DocID        string   `json:"docId"`
	DocTitle     string   `json:"docTitle"`
	ApplicableTo string   `json:"applicableTo"`
	Steps        []string `json:"steps"`
	SafetyNotice string   `json:"safetyNotice"`
}

// IndustrialToolServer serves industrial tools as webhooks for Fenced Tool Gateway.
type IndustrialToolServer struct{}

func NewIndustrialToolServer() *IndustrialToolServer {
	return &IndustrialToolServer{}
}

func (s *IndustrialToolServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Action   string          `json:"action"`
		Resource string          `json:"resource"`
		Args     json.RawMessage `json:"args"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	action := req.Action
	if action == "invoke" {
		switch {
		case strings.HasPrefix(req.Resource, "industrial:sensor"):
			action = "sensor-query"
		case strings.HasPrefix(req.Resource, "industrial:alarm"):
			action = "alarm-lookup"
		case strings.HasPrefix(req.Resource, "industrial:sop"):
			action = "sop-search"
		case strings.HasPrefix(req.Resource, "industrial:stop"):
			action = "emergency-stop"
		default:
			http.Error(w, "unknown resource: "+req.Resource, http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	switch action {
	case "sensor-query":
		s.handleSensorQuery(w, req.Args)
	case "alarm-lookup":
		s.handleAlarmLookup(w, req.Args)
	case "sop-search":
		s.handleSOPSearch(w, req.Args)
	case "emergency-stop":
		s.handleEmergencyStop(w, req.Args)
	default:
		http.Error(w, "unknown action: "+req.Action, http.StatusBadRequest)
	}
}

func (s *IndustrialToolServer) handleSensorQuery(w http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		EquipmentID      string `json:"equipmentId"`
		TimeRangeMinutes int    `json:"timeRangeMinutes"`
	}
	_ = json.Unmarshal(raw, &args)
	if args.EquipmentID == "" {
		args.EquipmentID = "CNC-03"
	}
	if args.TimeRangeMinutes <= 0 {
		args.TimeRangeMinutes = 20
	}

	now := time.Now()
	readings := make([]SensorDataPoint, 0, 5)
	readings = append(readings,
		SensorDataPoint{Timestamp: now.Add(-20 * time.Minute).Format("15:04:05"), Temperature: 74.2, CoolantFlow: 46.5, SpindleRPM: 12000, Vibration: 1.1},
		SensorDataPoint{Timestamp: now.Add(-15 * time.Minute).Format("15:04:05"), Temperature: 79.5, CoolantFlow: 42.0, SpindleRPM: 12000, Vibration: 1.3},
		SensorDataPoint{Timestamp: now.Add(-10 * time.Minute).Format("15:04:05"), Temperature: 84.8, CoolantFlow: 37.5, SpindleRPM: 12000, Vibration: 1.6},
		SensorDataPoint{Timestamp: now.Add(-5 * time.Minute).Format("15:04:05"), Temperature: 87.6, CoolantFlow: 35.8, SpindleRPM: 12000, Vibration: 1.9},
		SensorDataPoint{Timestamp: now.Format("15:04:05"), Temperature: 89.4, CoolantFlow: 35.1, SpindleRPM: 12000, Vibration: 2.1},
	)

	result := SensorQueryResult{
		EquipmentID:      args.EquipmentID,
		EquipmentName:    "5-Axis CNC Machining Center #3 (CNC-03)",
		EquipmentType:    "5-Axis CNC Milling Center",
		TimeRangeMinutes: args.TimeRangeMinutes,
		CurrentStatus:    "ALARM_CRITICAL",
		Readings:         readings,
		Anomalies: []string{
			"Spindle temperature exceedance: climbed from 74.2C to 89.4C over the last 20 min (threshold: 85.0C, exceedance: +4.4C)",
			"Coolant flow decay: flow dropped from 46.5 L/min baseline to 35.1 L/min (-24.5% decay)",
			"Spindle vibration slight upward trend (1.1 mm/s to 2.1 mm/s), within safe envelope (<2.8 mm/s)",
		},
	}
	_ = json.NewEncoder(w).Encode(result)
}

func (s *IndustrialToolServer) handleAlarmLookup(w http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		AlarmCode string `json:"alarmCode"`
	}
	_ = json.Unmarshal(raw, &args)
	if args.AlarmCode == "" {
		args.AlarmCode = "E102"
	}

	info := AlarmInfo{
		AlarmCode:        args.AlarmCode,
		AlarmTitle:       "Spindle Motor & Cooling Circuit Thermal Overheat Fault",
		Severity:         "CRITICAL",
		Component:        "High-Speed Spindle Motor / Constant-Temperature Cooling Jacket",
		TriggerCondition: "Spindle stator RTD sensor measured temperature > 85.0C continuously for > 300 seconds",
		HistoricalStats:  "Workshop failure statistics: 63% clogged coolant filter/line scaling; 27% pump impeller wear/bypass valve failure; 10% bearing lubrication deficit",
		PossibleCauses: []string{
			"Coolant filter mesh clogged by fine metal chips, obstructing circulation flow rate",
			"Chiller circulation pump output pressure deficit, flow falling below 38 L/min threshold",
			"High-load duty cycle spindle bearing oil-air lubrication deficit generating friction heat",
		},
	}
	_ = json.NewEncoder(w).Encode(info)
}

func (s *IndustrialToolServer) handleSOPSearch(w http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Query string `json:"query"`
	}
	_ = json.Unmarshal(raw, &args)

	sop := SOPGuideline{
		DocID:        "SOP-CNC-MNT-2026-E102",
		DocTitle:     "CNC Spindle Thermal Overheat (E102) Emergency Protocol & Diagnostics SOP",
		ApplicableTo: "CNC-01 to CNC-12 5-Axis Machining Centers",
		Steps: []string{
			"Step 1: Immediately pause feed cycle (Cycle Pause), maintain low idle rotation (500 RPM) for 3 minutes for air transitional cooling; do not perform hard e-stop to avoid bearing thermal seizure.",
			"Step 2: Inspect chiller fluid reservoir sight glass (must exceed green level marker); check return filter for particulate fouling; initiate backwash if delta-P exceeds threshold.",
			"Step 3: Monitor chiller pressure gauge (nominal: 0.25 - 0.40 MPa); if reading < 0.18 MPa, inspect circulation pump solenoid valve and mechanical coupling.",
			"Step 4: Once spindle naturally returns below 65C, jog spindle manually with vibrometer on drive/free ends to verify absence of mechanical rubbing.",
		},
		SafetyNotice: "Never spray cold water directly onto spindle casing when temperature > 80C; thermal shock will cause catastrophic sleeve cracking and rotor balance failure!",
	}
	_ = json.NewEncoder(w).Encode(sop)
}

func (s *IndustrialToolServer) handleEmergencyStop(w http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		EquipmentID string `json:"equipmentId"`
		Reason      string `json:"reason"`
	}
	_ = json.Unmarshal(raw, &args)

	res := map[string]any{
		"equipmentId": args.EquipmentID,
		"status":      "INTERLOCKED_SAFE_STOP",
		"timestamp":   time.Now().Format(time.RFC3339),
		"message":     fmt.Sprintf("Equipment %s safe deceleration interlock triggered. Reason: %s", args.EquipmentID, args.Reason),
		"requiresAck": true,
	}
	_ = json.NewEncoder(w).Encode(res)
}
