use veloq_runtime::{join, runtime::Runtime};

struct NotCopy(&'static str);

fn main() {
    Runtime::<(), _>::scope(async |_| {
        let (left, right) = join!(async { NotCopy("left") }, async { NotCopy("right") });
        assert_eq!(left.0, "left");
        assert_eq!(right.0, "right");
    })
    .unwrap();
}
