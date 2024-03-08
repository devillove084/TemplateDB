use actix::{Actor, System};
use storage_engine::{
    memtable::{key_format::InternalKeyComparator, memtable::MemTable},
    servers::memtable_server::MemTableServer,
    services::memtable_service::MemtableServiceHandler,
    util::comparator::BytewiseComparator,
};

fn main() {
    let system = System::new();
    system.block_on(async {
        let add = "[::1]:50051".to_string();
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        let mem = MemTable::new(1 << 32, icmp);
        let memtable_handler = MemtableServiceHandler::new_with_memtable(mem);
        let memtable_server = MemTableServer::new(memtable_handler, add);
        memtable_server.start();
        futures::future::pending::<()>().await;
    });
    system.run().unwrap();
}
