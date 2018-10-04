use std::io::{self, BufRead, Cursor};
use std::path::Path;
use clap::{Arg, App};
use rocksdb::{DB, DBVector, SeekKey};
use rocksdb::rocksdb::Writable;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[macro_use]
extern crate clap;

fn get(db: &DB, key: &[u8]) -> Option<DBVector> {
	match db.get(key) {
		Ok(value) => value,
		Err(e) => panic!("rocksdb operational problem encountered: {}", e),
	}
}

fn count_keys(db: &DB) -> u64 {
	let mut iter = db.iter();
	iter.seek(SeekKey::Start);
	iter.count() as u64
}

fn estimate_keys(db: &DB) -> u64 {
	db.get_property_int("rocksdb.estimate-num-keys").unwrap()
}

const NEXT_ITEM_KEY: &'static [u8] = b"\x00\nitem-maker.next_item";

fn get_current_item(db: &DB) -> u32 {
	match db.get(NEXT_ITEM_KEY) {
		Ok(Some(v)) => {
			let mut reader = Cursor::new(&*v);
			reader.read_u32::<BigEndian>().unwrap()
		},
		Ok(None) => 1,
		_ => panic!("db error")
	}
}

fn item_as_vec(item: u32) -> Vec<u8> {
	let mut writer = vec![];
	writer.write_u32::<BigEndian>(item).unwrap();
	writer
}

fn set_current_item(db: &DB, item: u32) {
	db.put(NEXT_ITEM_KEY, &item_as_vec(item)).unwrap();
}

fn process_queue(queue: &DB, db: &DB, keys_in_queue: u64, items_path: &Path, item_size: u64) {
	println!("Processing queue with ~{} keys in database and {} in queue", estimate_keys(&db), keys_in_queue);
	let mut keys_in_queue = keys_in_queue;
	loop {
		if keys_in_queue < item_size {
			break;
		}
		let mut iter = queue.iter();
		assert!(iter.seek(SeekKey::Start));
		let item     = get_current_item(&db);
		let item_vec = item_as_vec(item);
		for (k, _v) in &mut iter {
			db.put(&k, &item_vec).unwrap();
			queue.delete(&k).unwrap();
			keys_in_queue -= 1;
		}
		set_current_item(&db, item + 1);
	}
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

	let workspace  = Path::new(matches.value_of("WORKSPACE").unwrap());
	let item_size  = value_t!(matches.value_of("ITEM_SIZE"), u64).unwrap();
	let db_path    = workspace.join("database");
	let queue_path = workspace.join("queue");
	let items_path = workspace.join("items");
	let queue      = DB::open_default(queue_path.to_str().unwrap()).unwrap();
	let db         = DB::open_default(db_path.to_str().unwrap()).unwrap();
	let stdin      = io::stdin();

	println!("Starting with ~{} keys in database and {} in queue", estimate_keys(&db), count_keys(&queue));

	let mut keys_in_queue = count_keys(&queue);
	// Process the queue even if we get no input, because item_size may be
	// smaller than it was before.
	process_queue(&queue, &db, keys_in_queue, &items_path, item_size);

	for line in stdin.lock().lines() {
		let line = line.unwrap();
		let key  = line.as_bytes();
		match get(&db, &key) {
			None => {
				queue.put(&key, b"").unwrap();
				keys_in_queue += 1;
				if keys_in_queue >= item_size {
					process_queue(&queue, &db, keys_in_queue, &items_path, item_size);
				}
			},
			Some(_) => {}
		}
	}
}
