#[cfg(feature = "std")]
use conquer_once::spin::OnceCell;
#[cfg(not(feature = "std"))]
use conquer_once::OnceCell;

const DEFAULT_CHECK_THRESHOLD: u32 = 100;
const DEFAULT_ADVANCE_THRESHOLD: u32 = 100;



pub static CONFIG: OnceCell<Config> = OnceCell::new(Config::new());






#[derive(Copy, Clone, Debug)]
pub struct Config {
    check_threshold: u32,
    advance_threshold: u32,
}



impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}



impl Config {
    
    #[inline]
    pub const fn new() -> Self {
        Self {
            check_threshold: DEFAULT_CHECK_THRESHOLD,
            advance_threshold: DEFAULT_ADVANCE_THRESHOLD,
        }
    }

    
    #[inline]
    pub fn with_params(check_threshold: u32, advance_threshold: u32) -> Self {
        assert!(check_threshold > 0, "the check threshold must be larger than 0");
        Self { check_threshold, advance_threshold }
    }

    #[inline]
    
    pub fn check_threshold(self) -> u32 {
        self.check_threshold
    }

    
    #[inline]
    pub fn advance_threshold(self) -> u32 {
        self.advance_threshold
    }
}






#[derive(Copy, Clone, Debug, Default)]
pub struct ConfigBuilder {
    check_threshold: Option<u32>,
    advance_threshold: Option<u32>,
}



impl ConfigBuilder {
    
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    
    #[inline]
    pub fn check_threshold(mut self, check_threshold: u32) -> Self {
        self.check_threshold = Some(check_threshold);
        self
    }

    
    #[inline]
    pub fn advance_threshold(mut self, advance_threshold: u32) -> Self {
        self.advance_threshold = Some(advance_threshold);
        self
    }

    
    
    #[inline]
    pub fn build(self) -> Config {
        Config::with_params(
            self.check_threshold.unwrap_or(DEFAULT_CHECK_THRESHOLD),
            self.advance_threshold.unwrap_or(DEFAULT_ADVANCE_THRESHOLD),
        )
    }
}
