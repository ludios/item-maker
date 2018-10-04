item-maker
===

`item-maker` collects lines (of anything, e.g. URLs) into a RocksDB database
and write "item files" containing N lines per item, with no redundant lines
within or across items.

It is motivated by a common use case when archiving the web: URLs are
discovered by some process, often redundantly, but each URL needs to be
archived just once using some batch process that accepts a list of URLs.

Install
---

1. Install Rust Nightly using https://rustup.rs/
2. Run: 
    ```
    git clone https://github.com/ludios/item-maker
    cd item-maker
    cargo build --release
    ```
3. Copy or link `./target/release/item-maker` to somewhere in your `PATH`.

Usage
---

```
item-maker --help

item-maker ~/workspace 100

item-maker ~/workspace 100 --prefix item-filename-prefix-

# --force creates an item after processing stdin even if queue has < 100 lines
item-maker ~/workspace 100 --prefix item-filename-prefix- --force
```

Pipe lines into `item-maker`.  Lines are added to the queue.  When the
queue reaches `ITEM_SIZE`, lines are moved to the database and a new item
file is written to `~/workspace/items`.

You can safely pipe the redundant lines into `item-maker` and it will not
write the redundant lines to an item file.
