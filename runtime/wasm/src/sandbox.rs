use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::time::Duration;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

pub const PROVIDER_NAME: &str = "wasmtime";
pub const RUNTIME_ABI: &str = "fenced.wasm-component/v1";
pub const CHECKPOINT_SCHEMA: &str = "fenced.wasm-logical-state/v1";
const MAX_COMPONENT_BYTES: u64 = 64 << 20;

pub struct Sandbox {
    engine: Engine,
    package_root: PathBuf,
    fuel: u64,
    timeout: Duration,
    memory_bytes: usize,
}

struct HostState {
    limits: StoreLimits,
}

struct EpochWatchdog {
    cancel: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl EpochWatchdog {
    fn new(engine: Engine, timeout: Duration) -> Self {
        let (cancel, cancelled) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            if cancelled.recv_timeout(timeout).is_err() {
                engine.increment_epoch();
            }
        });
        Self {
            cancel,
            thread: Some(thread),
        }
    }
}

impl Drop for EpochWatchdog {
    fn drop(&mut self) {
        let _ = self.cancel.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Sandbox {
    pub fn new(
        package_root: impl AsRef<Path>,
        fuel: u64,
        timeout: Duration,
        memory_bytes: usize,
    ) -> Result<Self> {
        if fuel == 0 || timeout.is_zero() || memory_bytes == 0 {
            bail!("fuel, timeout, and memory limit must be positive");
        }
        let package_root = package_root
            .as_ref()
            .canonicalize()
            .context("canonicalize package root")?;
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.consume_fuel(true);
        config.epoch_interruption(true);
        config.max_wasm_stack(2 << 20);
        let engine = Engine::new(&config)
            .map_err(|error| anyhow::anyhow!("create Wasmtime engine: {error}"))?;
        Ok(Self {
            engine,
            package_root,
            fuel,
            timeout,
            memory_bytes,
        })
    }

    pub fn execute(&self, relative_component: &str, input: &str) -> Result<String> {
        let component_path = self.resolve_component(relative_component)?;
        let component_size = std::fs::metadata(&component_path)
            .context("read component metadata")?
            .len();
        if component_size == 0 || component_size > MAX_COMPONENT_BYTES {
            bail!("component size must be between 1 byte and {MAX_COMPONENT_BYTES} bytes");
        }
        let component = Component::from_file(&self.engine, &component_path).map_err(|error| {
            anyhow::anyhow!("compile component {}: {error}", component_path.display())
        })?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_bytes)
            // The wit-component adapter encodes every agent component as an
            // adapter core module plus the agent core module, so exactly two
            // core instances are required; the host controls all imports, so
            // untrusted code cannot grow this count itself.
            .instances(2)
            .tables(8)
            .build();
        let mut store = Store::new(&self.engine, HostState { limits });
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(self.fuel)
            .map_err(|error| anyhow::anyhow!("set component fuel: {error}"))?;
        store.set_epoch_deadline(1);

        let _watchdog = EpochWatchdog::new(self.engine.clone(), self.timeout);
        let linker = Linker::<HostState>::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &component)
            .map_err(|error| anyhow::anyhow!("instantiate capability-free component: {error}"))?;
        let run = instance
            .get_typed_func::<(String,), (Result<String, String>,)>(&mut store, "run")
            .map_err(|error| {
                anyhow::anyhow!(
                    "component must export run(string) -> result<string, string>: {error}"
                )
            })?;
        let (output,) = run
            .call(&mut store, (input.to_owned(),))
            .map_err(|error| anyhow::anyhow!("execute component: {error}"))?;
        output.map_err(anyhow::Error::msg)
    }

    pub fn interrupt(&self) {
        self.engine.increment_epoch();
    }

    fn resolve_component(&self, relative_component: &str) -> Result<PathBuf> {
        let requested = Path::new(relative_component);
        if requested.is_absolute() || relative_component.trim().is_empty() {
            bail!("component path must be non-empty and relative");
        }
        let canonical = self
            .package_root
            .join(requested)
            .canonicalize()
            .context("canonicalize component path")?;
        if !canonical.starts_with(&self.package_root) {
            bail!("component path escapes the configured package root");
        }
        Ok(canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_escape() {
        let root = std::env::temp_dir().join(format!("fenced-wasm-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).expect("create test root");
        let sandbox = Sandbox::new(&root, 10_000, Duration::from_millis(100), 1 << 20)
            .expect("create sandbox");
        let error = sandbox
            .resolve_component("../outside.wasm")
            .expect_err("path escape was accepted");
        assert!(
            error.to_string().contains("canonicalize") || error.to_string().contains("escapes")
        );
        std::fs::remove_dir_all(root).expect("remove test root");
    }

    #[test]
    fn executes_the_conformance_component() {
        let root = std::env::temp_dir().join(format!("fenced-wasm-exec-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).expect("create package root");
        let component = crate::fixture::build_agent_component().expect("build fixture component");
        std::fs::write(root.join("agent.wasm"), component).expect("stage component");
        let sandbox = Sandbox::new(&root, 10_000_000, Duration::from_secs(30), 32 << 20)
            .expect("create sandbox");
        let input = r#"{"agentVersionRef":"conformance@1","goal":"echo me"}"#;
        let output = sandbox
            .execute("agent.wasm", input)
            .expect("execute conformance component");
        assert_eq!(output, input, "component must echo its input verbatim");
        std::fs::remove_dir_all(root).expect("remove package root");
    }
}
