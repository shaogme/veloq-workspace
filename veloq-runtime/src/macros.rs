use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A utility enum for the `select!` macro.
#[doc(hidden)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// A utility future for the `select!` macro that polls two futures preferentially.
/// Branch A is polled first. If it is ready, it returns `Left(A::Output)`.
/// If A is pending, B is polled. If B is ready, it returns `Right(B::Output)`.
/// If both are pending, it returns `Pending`.
#[doc(hidden)]
pub struct Select2<A, B> {
    pub a: A,
    pub b: B,
}

impl<A, B> Future for Select2<A, B>
where
    A: Future,
    B: Future,
{
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY:
        // We are projecting Pin<&mut Self> to Pin<&mut A> and Pin<&mut B>.
        // This is safe because we never move A or B out of Self, and Self is Pinned.
        unsafe {
            let this = self.get_unchecked_mut();

            // Poll A (Biased)
            let a = Pin::new_unchecked(&mut this.a);
            if let Poll::Ready(v) = a.poll(cx) {
                return Poll::Ready(Either::Left(v));
            }

            // Poll B
            let b = Pin::new_unchecked(&mut this.b);
            if let Poll::Ready(v) = b.poll(cx) {
                return Poll::Ready(Either::Right(v));
            }
        }
        Poll::Pending
    }
}

/// A macro to wait on multiple futures simultaneously, returning the output of the first one that completes.
///
/// This implementation uses **Biased Polling**: branches are polled in the order they are written.
/// If the first branch is ready, the second branch is never polled. This is efficient for
/// I/O completion models where polling may trigger a syscall or resource allocation.
///
/// # Example
/// ```ignore
/// select! {
///     val = rx.recv() => {
///         println!("Received: {:?}", val);
///     },
///     _ = timer.tick() => {
///         println!("Timed out");
///     }
/// }
/// ```
#[macro_export]
macro_rules! select {
    // Case: Single branch
    ($pat:pat = $fut:expr => $handler:expr $(,)?) => {
        {
            let $pat = $fut.await;
            $handler
        }
    };

    // Case: Multiple branches
    ($pat:pat = $fut:expr => $handler:expr, $($r_pat:pat = $r_fut:expr => $r_handler:expr),+ $(,)?) => {
        {
            use $crate::macros::{Select2, Either};
            // Construct the composed future
            let cmd = Select2 {
                a: $fut,
                b: $crate::select!(@recurse_future $($r_pat = $r_fut => $r_handler),+)
            };

            match cmd.await {
                Either::Left(res) => {
                    let $pat = res;
                    $handler
                },
                Either::Right(res) => {
                    $crate::select!(@recurse_match res, $($r_pat = $r_fut => $r_handler),+)
                }
            }
        }
    };

    // Internal Helper: Build future chain
    // Base case: last one
    (@recurse_future $pat:pat = $fut:expr => $handler:expr) => {
        $fut
    };
    // Recursive case
    (@recurse_future $pat:pat = $fut:expr => $handler:expr, $($rest:tt)*) => {
        $crate::macros::Select2 {
            a: $fut,
            b: $crate::select!(@recurse_future $($rest)*)
        }
    };

    // Internal Helper: Match chain
    // Base case: One remaining
    (@recurse_match $val:ident, $pat:pat = $fut:expr => $handler:expr) => {
        {
            let $pat = $val;
            $handler
        }
    };
    // Recursive case
    (@recurse_match $val:ident, $pat:pat = $fut:expr => $handler:expr, $($rest:tt)*) => {
        match $val {
            $crate::macros::Either::Left(res) => {
                 let $pat = res;
                 $handler
            },
            $crate::macros::Either::Right(res) => {
                 $crate::select!(@recurse_match res, $($rest)*)
            }
        }
    };
}
