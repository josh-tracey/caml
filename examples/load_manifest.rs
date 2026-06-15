use std::{env, fs::File};

use caml::CamlManifest;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .ok_or("usage: cargo run --example load_manifest <manifest.yaml>")?;
    let file = File::open(path)?;
    let manifest = CamlManifest::from_reader(file)?;

    println!("{manifest:#?}");
    Ok(())
}
