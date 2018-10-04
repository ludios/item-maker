use std::io::{self, BufRead};
use std::path::Path;
use clap::{Arg, App};
use rocksdb::{DB, DBVector, SeekKey};
use rocksdb::rocksdb::Writable;

#[macro_use]
extern crate clap;

fn get(db: &DB, key: &[u8]) -> Option<DBVector> {
	match db.get(key) {
		Ok(value) => value,
		Err(e) => panic!("rocksdb operational problem encountered: {}", e),
	}
}

fn count_keys(db: &DB) -> usize {
	let mut iter = db.iter();
	assert!(iter.seek(SeekKey::Start));
	iter.count()
}

fn estimate_keys(db: &DB) -> u64 {
	db.get_property_int("rocksdb.estimate-num-keys").unwrap()
}

fn main() {
	let matches =
		App::new("item-maker")
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

	let db_path    = workspace.join("database");
	let queue_path = workspace.join("queue");
	let db         = DB::open_default(db_path.to_str().unwrap()).unwrap();
	let queue      = DB::open_default(queue_path.to_str().unwrap()).unwrap();

	let stdin = io::stdin();
	println!("Starting with ~{} keys in database and {} in queue", estimate_keys(&db), count_keys(&queue));
	for line in stdin.lock().lines() {
		let line = line.unwrap();
		let key  = line.as_bytes();
		match get(&db, &key) {
			None => queue.put(&key, b"").unwrap(),
			Some(_) => {}
		}
	}
}
