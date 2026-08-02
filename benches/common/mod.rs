#![allow(dead_code)]

use anyhow::{Context, Result, ensure};
use hdrhistogram::Histogram;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

pub const CPU_ENV: &str = "BCAST_BENCH_CPUS";
pub const WARMUP_ENV: &str = "BCAST_BENCH_WARMUP";

#[derive(Clone, Copy)]
pub struct BenchClock {
    epoch: Instant,
}

impl BenchClock {
    pub fn new() -> Self {
        Self { epoch: Instant::now() }
    }

    #[inline]
    pub fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

#[derive(Clone)]
pub struct CpuAffinity {
    cpus: Option<Arc<[usize]>>,
}

impl CpuAffinity {
    pub fn from_env(required_cpus: usize) -> Result<Self> {
        let Some(value) = std::env::var_os(CPU_ENV) else {
            return Ok(Self { cpus: None });
        };
        let value = value
            .into_string()
            .map_err(|_| anyhow::anyhow!("{CPU_ENV} is not valid UTF-8"))?;
        let cpus = value
            .split(',')
            .map(|value| {
                value
                    .trim()
                    .parse::<usize>()
                    .with_context(|| format!("invalid logical CPU in {CPU_ENV}: {value:?}"))
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(cpus.len() >= required_cpus, "{CPU_ENV} needs at least {required_cpus} logical CPUs");
        let unique = cpus.iter().copied().collect::<HashSet<_>>();
        ensure!(unique.len() == cpus.len(), "{CPU_ENV} must not contain duplicate logical CPUs");
        validate_available_cpus(&cpus)?;

        Ok(Self {
            cpus: Some(cpus.into()),
        })
    }

    pub fn pin_current(&self, slot: usize, role: &str) -> Result<()> {
        let Some(cpus) = &self.cpus else {
            return Ok(());
        };
        let cpu = cpus[slot];
        pin_current_thread(cpu).with_context(|| format!("pin {role} to logical CPU {cpu}"))
    }

    pub fn print(&self) {
        match &self.cpus {
            Some(cpus) => println!("logical CPU assignment: {cpus:?}"),
            None => {
                println!("logical CPU assignment: unpinned (set {CPU_ENV}=producer,consumer,... for stable results)")
            }
        }
    }
}

pub fn env_usize(name: &str, default: usize) -> Result<usize> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{name} is not valid UTF-8"))?
        .parse::<usize>()
        .with_context(|| format!("invalid integer in {name}"))
}

pub fn print_histogram(name: &str, histogram: &Histogram<u64>, unit: &str) {
    println!("{name}");
    println!("  min: {}{unit}", histogram.min());
    println!("  p50: {}{unit}", histogram.value_at_percentile(50.0));
    println!("  p90: {}{unit}", histogram.value_at_percentile(90.0));
    println!("  p99: {}{unit}", histogram.value_at_percentile(99.0));
    println!("  p99.9: {}{unit}", histogram.value_at_percentile(99.9));
    println!("  p99.99: {}{unit}", histogram.value_at_percentile(99.99));
    println!("  max: {}{unit}", histogram.max());
    println!("  samples: {}", histogram.len());
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: usize) -> Result<()> {
    ensure!(cpu < libc::CPU_SETSIZE as usize, "logical CPU {cpu} exceeds CPU_SETSIZE");

    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }
    let result =
        unsafe { libc::pthread_setaffinity_np(libc::pthread_self(), std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if result != 0 {
        return Err(std::io::Error::from_raw_os_error(result).into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_cpu: usize) -> Result<()> {
    anyhow::bail!("per-thread CPU affinity is only supported on Linux")
}

#[cfg(target_os = "linux")]
fn validate_available_cpus(cpus: &[usize]) -> Result<()> {
    let mut available = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    let result = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut available) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("read process CPU affinity");
    }

    for &cpu in cpus {
        ensure!(cpu < libc::CPU_SETSIZE as usize, "logical CPU {cpu} exceeds CPU_SETSIZE");
        ensure!(unsafe { libc::CPU_ISSET(cpu, &available) }, "logical CPU {cpu} is not available to this process");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_available_cpus(_cpus: &[usize]) -> Result<()> {
    anyhow::bail!("{CPU_ENV} is only supported on Linux")
}
