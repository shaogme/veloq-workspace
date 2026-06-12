#[derive(Debug)]
pub enum BlockingOps {}

impl BlockingOps {
    pub fn run(self) {
        match self {}
    }
}
