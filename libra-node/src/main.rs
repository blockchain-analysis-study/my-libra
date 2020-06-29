// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

use libra_config::config::NodeConfig;
use libra_types::PeerId;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Libra Node")]
struct Args {
    #[structopt(short = "f", long, parse(from_os_str))]
    /// Path to NodeConfig
    config: PathBuf,
    #[structopt(short = "d", long)]
    /// Disable logging
    no_logging: bool,
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// TODO 节点主入口
fn main() {

    // 读取命令行参数
    let args = Args::from_args();

    // 加载 配置文件
    let mut config = NodeConfig::load(args.config).expect("Failed to load node config");
    println!("Using node config {:?}", &config);
    crash_handler::setup_panic_handler();

    if !args.no_logging {
        libra_logger::Logger::new()
            .channel_size(config.logger.chan_size)
            .is_async(config.logger.is_async)
            .level(config.logger.level)
            .init();
        libra_logger::init_struct_log_from_env().expect("Failed to initialize structured logging");
    }

    if config.metrics.enabled {
        for network in &config.full_node_networks {
            let peer_id = network.peer_id();
            setup_metrics(peer_id, &config);
        }

        if let Some(network) = config.validator_network.as_ref() {
            let peer_id = network.peer_id();
            setup_metrics(peer_id, &config);
        }
    }

    // 根据配置文件, 设置整个node运行所需的环境.
    //
    // 设置环境则会为节点创建存储、网络、共识、内存、线程池等一些列的句柄.
    let _node_handle = libra_node::main_node::setup_environment(&mut config);

    let term = Arc::new(AtomicBool::new(false));

    while !term.load(Ordering::Acquire) {
        std::thread::park();
    }
}

fn setup_metrics(peer_id: PeerId, config: &NodeConfig) {
    libra_metrics::dump_all_metrics_to_file_periodically(
        &config.metrics.dir(),
        &format!("{}.metrics", peer_id),
        config.metrics.collection_interval_ms,
    );
}
