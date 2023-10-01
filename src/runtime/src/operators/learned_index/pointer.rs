

#[derive(Debug, Default)]
pub struct Pointer {
    pub pref: usize,
    pub size: usize,
    pub cap: Option<usize>,
}