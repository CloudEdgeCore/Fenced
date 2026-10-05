//! Emits the deterministic conformance component for the Wasmtime provider.
//!
//! Usage: component-fixture --out <path>
//!
//! The produced component implements the agent world by echoing its bounded
//! JSON input, so conformance runs observe identical kernel-visible behavior
//! on every provider.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out: Option<PathBuf> = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--out" => {
                out = Some(PathBuf::from(
                    arguments.next().ok_or("--out requires a path")?,
                ));
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    let out = out.ok_or("--out <path> is required")?;
    let component = fenced_runtime_wasm::fixture::build_agent_component()?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out, component)?;
    eprintln!("wrote conformance component to {}", out.display());
    Ok(())
}
