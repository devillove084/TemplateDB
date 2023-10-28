// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use actix::{Actor, System};
use engine::BytewiseComparator;
use engine::db::format::InternalKeyComparator;
use engine::mem::MemTable;
use engine::mem::handler::MemtableServiceHandler;
use engine::mem::memtable_actor::MemTableActor;

fn main() {
    let system = System::new();
    system.block_on(async {
        let add = "[::1]:50051".to_string();
        let icmp = InternalKeyComparator::new(BytewiseComparator::default());
        let mem = MemTable::new(1 << 32, icmp);
        let memtable_handler = MemtableServiceHandler::new_with_memtable(mem);
        let memtable_actor = MemTableActor::new(memtable_handler, add);
        memtable_actor.start();
        futures::future::pending::<()>().await;
    });
    system.run().unwrap();
}
