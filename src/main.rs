use std::io::{self, BufRead};
use std::path::Path;
use clap::{Arg, App};
use rocksdb::{DB, DBVector, IteratorMode};

#[macro_use]
extern crate clap;

fn get(db: &DB, key: &[u8]) -> Option<DBVector> {
	match db.get(key) {
		Ok(value) => value,
		Err(e) => panic!("rocksdb operational problem encountered: {}", e),
	}
}

fn count_keys(db: &DB) -> usize {
	db.iterator(IteratorMode::Start).count()
}

fn main() {
    let matches = App::new("item-maker")
                          .version("1.0")
                          .about("generates items of N lines that have not previously appeared in another item")
                          .arg(Arg::with_name("WORKSPACE")
                               .help("Directory to use as the workspace")
                               .required(true)
                               .index(1))
                          .arg(Arg::with_name("ITEM_SIZE")
                               .help("Number of lines to put in each new item")
                               .required(true)
                               .index(2))
                          .get_matches();

	let workspace = Path::new(matches.value_of("WORKSPACE").unwrap());
	let item_size = value_t!(matches.value_of("ITEM_SIZE"), u32).unwrap();

	let db_path = workspace.join("database");
	let queue_path = workspace.join("queue");
	let db = DB::open_default(db_path).unwrap();
	let queue = DB::open_default(queue_path).unwrap();

	let stdin = io::stdin();
	// TODO: estimate keys in db
	// db->GetProperty("rocksdb.estimate-num-keys", &num)
	println!("Starting with {} keys in database and {} in queue", count_keys(&db), count_keys(&queue));
	for line in stdin.lock().lines() {
		let line = line.unwrap();
		let key = line.as_bytes();
		match get(&db, &key) {
			None => queue.put(&key, b"").unwrap(),
			Some(_) => {}
		}
	}
}
