fn main() {
    use mongreldb_ffi::*;
    println!("Rust CValue size: {}", std::mem::size_of::<CValue>());
    println!("Rust CValue align: {}", std::mem::align_of::<CValue>());
    println!("Rust CValueTag size: {}", std::mem::size_of::<CValueTag>());
    println!("Rust CValuePayload size: {}", std::mem::size_of::<CValuePayload>());
    println!("Rust mongreldb_cell_input size: {}", std::mem::size_of::<mongreldb_cell_input>());
}
