use anyhow::{anyhow, Result};
use futures::future::ok;
use log::info;
use std::fs;
use std::process::Command;

fn get_device_from_mount(mount_point: &str) -> Result<String> {
    let output = Command::new("findmnt")
        .args(["-n", "-o", "SOURCE", "--target", mount_point])
        .output()?;

    if !output.status.success() {
        return Err(anyhow!(
            "Failed to run findmnt: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let dev = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(dev)
}

fn get_device_lifetime(mount_point: &str) -> Result<usize> {
    let device = get_device_from_mount(mount_point)?;

    let output = Command::new("smartctl").args(["-A", &device]).output()?;

    if !output.status.success() {
        return Err(anyhow!(
            "Failed to run smartctl: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    for line in stdout.lines() {
        if line.contains("Percentage_Used") || line.contains("Media_Wearout_Indicator") {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if let Some(value_str) = tokens.last() {
                if let Ok(value) = value_str.parse::<u64>() {
                    return Ok(value as usize);
                }
            }
        }
    }

    Err(anyhow!("Failed to get [{mount_point}] device lifetime"))
}

pub fn disk_availability_detect(mount_point: &str, lifetime_threshold: usize) -> Result<()> {
    let percent = get_device_lifetime(mount_point)?;
    if percent > lifetime_threshold {
        return Err(anyhow!(
            "❌ Disk {} appears worn out: {}% used",
            mount_point,
            percent
        ));
    } else {
        info!("✅ Disk {} health ok: {}% used", mount_point, percent);
    }
    Ok(())
}
