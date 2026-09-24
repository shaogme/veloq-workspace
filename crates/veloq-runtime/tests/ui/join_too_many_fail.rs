use veloq_runtime::join;

fn main() {
    let _ = join!(
        async { 0usize },
        async { 1usize },
        async { 2usize },
        async { 3usize },
        async { 4usize },
        async { 5usize },
        async { 6usize },
        async { 7usize },
        async { 8usize },
        async { 9usize },
        async { 10usize },
        async { 11usize },
        async { 12usize },
        async { 13usize },
        async { 14usize },
        async { 15usize },
        async { 16usize },
    );
}
