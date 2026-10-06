// Package version is the single source of truth for product and public
// contract versions. Build pipelines may replace Commit and BuildTime with
// -ldflags; the release identity itself is immutable source data.
package version

const (
	Product             = "Fenced"
	ProductVersion      = "1.3.0.0"
	SemVer              = "1.3.0"
	ReleaseStage        = "GA"
	Manifest            = "fenced.dev/v1"
	RuntimeProtocol     = "fenced.runtime.v1"
	RuntimeInterface    = "fenced.runtime.interface/v1"
	GatewayProtocol     = "fenced.gateway.v1"
	ModelProtocol       = "fenced.model.v1"
	ControlAPI          = "v1"
	SyscallABI          = "1.0.0"
	IPCProtocol         = "fenced.ipc.v1"
	ServiceProtocol     = "fenced.service.v1"
	EffectProtocol      = "fenced.effect.v1"
	LegacyRemovalBefore = "2027-02-17"
)

var (
	Commit    = "development"
	BuildTime = "unknown"
)

type Info struct {
	Product             string `json:"product"`
	ProductVersion      string `json:"productVersion"`
	SemVer              string `json:"semver"`
	ReleaseStage        string `json:"releaseStage"`
	Commit              string `json:"commit"`
	BuildTime           string `json:"buildTime"`
	Manifest            string `json:"manifest"`
	RuntimeProtocol     string `json:"runtimeProtocol"`
	RuntimeInterface    string `json:"runtimeInterface"`
	GatewayProtocol     string `json:"gatewayProtocol"`
	ModelProtocol       string `json:"modelProtocol"`
	ControlAPI          string `json:"controlApi"`
	SyscallABI          string `json:"syscallAbi"`
	IPCProtocol         string `json:"ipcProtocol"`
	ServiceProtocol     string `json:"serviceProtocol"`
	EffectProtocol      string `json:"effectProtocol"`
	LegacyRemovalBefore string `json:"legacyRemovalBefore"`
}

func Current() Info {
	return Info{
		Product: Product, ProductVersion: ProductVersion, SemVer: SemVer,
		ReleaseStage: ReleaseStage, Commit: Commit, BuildTime: BuildTime,
		Manifest: Manifest, RuntimeProtocol: RuntimeProtocol,
		RuntimeInterface: RuntimeInterface, GatewayProtocol: GatewayProtocol,
		ModelProtocol: ModelProtocol, ControlAPI: ControlAPI,
		SyscallABI: SyscallABI, IPCProtocol: IPCProtocol,
		ServiceProtocol: ServiceProtocol, EffectProtocol: EffectProtocol,
		LegacyRemovalBefore: LegacyRemovalBefore,
	}
}
