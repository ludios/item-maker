use std::io::{self, BufRead};
use std::path::Path;
use clap::{Arg, App};
use rocksdb::DB;

#[macro_use]
extern crate clap;

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

	let rocksdb_path = workspace.join("rocksdb");
	let db = DB::open_default(rocksdb_path).unwrap();

	// TODO: write to `queue` and when that becomes full, write to database and item

	let stdin = io::stdin();
	for line in stdin.lock().lines() {
		println!("{:?}", line);
		//db.put(line, b"").unwrap();
	}
}
