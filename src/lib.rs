extern crate ceph;
#[macro_use]
extern crate log;

use std::path::Path;
use std::str::FromStr;

use ceph::ceph::*;
use ceph::cmd::{osd_pool_get, PoolOption};
use ceph::rados::{Struct_rados_cluster_stat_t, Struct_rados_pool_stat_t};

#[derive(Debug)]
pub struct PoolInfo {
    pub name: String,
    pub usage: Struct_rados_pool_stat_t,
    pub pool_size: u32,
}

#[derive(Debug)]
pub struct UsageInfo {
    pub cluster_usage: Struct_rados_cluster_stat_t,
    pub pool_usage: Vec<PoolInfo>,
}

pub fn get_pool_size(handle: &Rados, pool: &str) -> Result<u32, String> {
    let pool_size_str = osd_pool_get(handle, &pool, &PoolOption::Size).map_err(|e| e.to_string())?;
    if let Some(s) = pool_size_str.split_whitespace().last() {
        let pool_size = u32::from_str(&s).map_err(|e| e.to_string())?;
        debug!("pool_size: {}", pool_size);
        return Ok(pool_size);
    }
    Err(format!(
        "Invalid size string returned from librados: {}",
        pool_size_str,
    ))
}

pub fn get_cluster_usage(user: &str, conf_file: &Path) -> Result<UsageInfo, String> {
    let mut pool_usage: Vec<PoolInfo> = Vec::new();

    debug!("Connecting to ceph");
    let h = connect_to_ceph(user, &format!("{}", conf_file.display())).map_err(|e| e.to_string())?;

    debug!("Running stat against the cluster");
    let cluster_stats = h.rados_stat_cluster().map_err(|e| e.to_string())?;

    debug!("Listing pools");
    let pools = h.rados_pools().map_err(|e| e.to_string())?;
    debug!("pools: {:?}", pools);

    for p in pools {
        debug!("Getting an ioctx to: {}", p);
        let i = h.get_rados_ioctx(&p).map_err(|e| e.to_string())?;
        debug!("Running stat against the pool");
        let pool_stats = i.rados_stat_pool().map_err(|e| e.to_string())?;
        pool_usage.push(PoolInfo {
            name: p.clone(),
            usage: pool_stats,
            pool_size: get_pool_size(&h, &p)?,
        });
    }
    Ok(UsageInfo {
        cluster_usage: cluster_stats,
        pool_usage: pool_usage,
    })
}
