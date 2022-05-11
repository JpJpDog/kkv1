#![feature(int_roundings)]
#![feature(slice_as_chunks)]

mod error;
mod page;
mod persistencer;
#[cfg(test)]
mod test;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
