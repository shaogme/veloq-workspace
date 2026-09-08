use std::{convert::Infallible, fmt::Debug, result::Result as StdResult};

/// Describes how a scope body completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Outcome<T, E> {
    /// The scope body completed successfully with a value.
    Ok(T),
    /// The scope body returned an error and its children should be cancelled.
    Err(E),
}

impl<T, E> Outcome<T, E> {
    /// Returns `true` if the outcome is [`Outcome::Ok`].
    #[inline]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Outcome::Ok(_))
    }

    /// Returns `true` if the outcome is [`Outcome::Ok`] and the value inside of it matches a predicate.
    #[inline]
    pub fn is_ok_and<F: FnOnce(&T) -> bool>(&self, f: F) -> bool {
        match self {
            Outcome::Ok(x) => f(x),
            Outcome::Err(_) => false,
        }
    }

    /// Returns `true` if the outcome is [`Outcome::Err`].
    #[inline]
    pub const fn is_err(&self) -> bool {
        matches!(self, Outcome::Err(_))
    }

    /// Returns `true` if the outcome is [`Outcome::Err`] and the value inside of it matches a predicate.
    #[inline]
    pub fn is_err_and<F: FnOnce(&E) -> bool>(&self, f: F) -> bool {
        match self {
            Outcome::Ok(_) => false,
            Outcome::Err(x) => f(x),
        }
    }

    /// Converts from `Outcome<T, E>` to `Option<T>`.
    #[inline]
    pub fn ok(self) -> Option<T> {
        match self {
            Outcome::Ok(x) => Some(x),
            Outcome::Err(_) => None,
        }
    }

    /// Converts from `Outcome<T, E>` to `Option<E>`.
    #[inline]
    pub fn err(self) -> Option<E> {
        match self {
            Outcome::Ok(_) => None,
            Outcome::Err(x) => Some(x),
        }
    }

    /// Converts from `&Outcome<T, E>` to `Outcome<&T, &E>`.
    #[inline]
    pub const fn as_ref(&self) -> Outcome<&T, &E> {
        match *self {
            Outcome::Ok(ref x) => Outcome::Ok(x),
            Outcome::Err(ref x) => Outcome::Err(x),
        }
    }

    /// Converts from `&mut Outcome<T, E>` to `Outcome<&mut T, &mut E>`.
    #[inline]
    pub fn as_mut(&mut self) -> Outcome<&mut T, &mut E> {
        match *self {
            Outcome::Ok(ref mut x) => Outcome::Ok(x),
            Outcome::Err(ref mut x) => Outcome::Err(x),
        }
    }

    /// Maps an `Outcome<T, E>` to `Outcome<U, E>` by applying a function to a contained [`Outcome::Ok`] value,
    /// leaving an [`Outcome::Err`] value untouched.
    #[inline]
    pub fn map<U, F: FnOnce(T) -> U>(self, op: F) -> Outcome<U, E> {
        match self {
            Outcome::Ok(x) => Outcome::Ok(op(x)),
            Outcome::Err(e) => Outcome::Err(e),
        }
    }

    /// Returns the provided default (if [`Outcome::Err`]), or applies a function to the contained value (if [`Outcome::Ok`]).
    #[inline]
    pub fn map_or<U, F: FnOnce(T) -> U>(self, default: U, f: F) -> U {
        match self {
            Outcome::Ok(t) => f(t),
            Outcome::Err(_) => default,
        }
    }

    /// Maps an `Outcome<T, E>` to `U` by applying fallback function `default` to a contained [`Outcome::Err`] value,
    /// or function `f` to a contained [`Outcome::Ok`] value.
    #[inline]
    pub fn map_or_else<U, D: FnOnce(E) -> U, F: FnOnce(T) -> U>(self, default: D, f: F) -> U {
        match self {
            Outcome::Ok(t) => f(t),
            Outcome::Err(e) => default(e),
        }
    }

    /// Maps an `Outcome<T, E>` to `Outcome<T, F>` by applying a function to a contained [`Outcome::Err`] value,
    /// leaving an [`Outcome::Ok`] value untouched.
    #[inline]
    pub fn map_err<F, O: FnOnce(E) -> F>(self, op: O) -> Outcome<T, F> {
        match self {
            Outcome::Ok(t) => Outcome::Ok(t),
            Outcome::Err(e) => Outcome::Err(op(e)),
        }
    }

    /// Calls `op` if the outcome is [`Outcome::Ok`], otherwise returns the [`Outcome::Err`] value of `self`.
    #[inline]
    pub fn and_then<U, F: FnOnce(T) -> Outcome<U, E>>(self, op: F) -> Outcome<U, E> {
        match self {
            Outcome::Ok(t) => op(t),
            Outcome::Err(e) => Outcome::Err(e),
        }
    }

    /// Calls `op` if the outcome is [`Outcome::Err`], otherwise returns the [`Outcome::Ok`] value of `self`.
    #[inline]
    pub fn or_else<F, O: FnOnce(E) -> Outcome<T, F>>(self, op: O) -> Outcome<T, F> {
        match self {
            Outcome::Ok(t) => Outcome::Ok(t),
            Outcome::Err(e) => op(e),
        }
    }

    /// Returns the contained [`Outcome::Ok`] value or a provided default.
    #[inline]
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            Outcome::Ok(t) => t,
            Outcome::Err(_) => default,
        }
    }

    /// Returns the contained [`Outcome::Ok`] value or computes it from a closure.
    #[inline]
    pub fn unwrap_or_else<F: FnOnce(E) -> T>(self, op: F) -> T {
        match self {
            Outcome::Ok(t) => t,
            Outcome::Err(e) => op(e),
        }
    }

    /// Converts `self` into a standard [`Result<T, E>`].
    #[inline]
    pub fn into_result(self) -> StdResult<T, E> {
        match self {
            Outcome::Ok(t) => Ok(t),
            Outcome::Err(e) => Err(e),
        }
    }
}

impl<T, E> Outcome<T, E>
where
    E: Debug,
{
    /// Returns the contained [`Outcome::Ok`] value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Outcome::Err`], with a panic message including the
    /// passed message, and the content of the [`Outcome::Err`].
    #[track_caller]
    #[inline]
    pub fn expect(self, msg: &str) -> T {
        match self {
            Outcome::Ok(t) => t,
            Outcome::Err(e) => panic!("{msg}: {e:?}"),
        }
    }

    /// Returns the contained [`Outcome::Ok`] value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Outcome::Err`], with a panic message provided by the
    /// [`Outcome::Err`]'s value.
    #[track_caller]
    #[inline]
    pub fn unwrap(self) -> T {
        match self {
            Outcome::Ok(t) => t,
            Outcome::Err(e) => panic!("called `Outcome::unwrap()` on an `Err` value: {e:?}"),
        }
    }
}

impl<T, E> Outcome<T, E>
where
    T: Debug,
{
    /// Returns the contained [`Outcome::Err`] value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Outcome::Ok`], with a panic message including the
    /// passed message, and the content of the [`Outcome::Ok`].
    #[track_caller]
    #[inline]
    pub fn expect_err(self, msg: &str) -> E {
        match self {
            Outcome::Ok(t) => panic!("{msg}: {t:?}"),
            Outcome::Err(e) => e,
        }
    }

    /// Returns the contained [`Outcome::Err`] value, consuming `self`.
    ///
    /// # Panics
    ///
    /// Panics if the value is an [`Outcome::Ok`], with a panic message provided by the
    /// [`Outcome::Ok`]'s value.
    #[track_caller]
    #[inline]
    pub fn unwrap_err(self) -> E {
        match self {
            Outcome::Ok(t) => panic!("called `Outcome::unwrap_err()` on an `Ok` value: {t:?}"),
            Outcome::Err(e) => e,
        }
    }
}

impl<T, E> Outcome<T, E>
where
    T: Default,
{
    /// Returns the contained [`Outcome::Ok`] value or a default.
    #[inline]
    pub fn unwrap_or_default(self) -> T {
        match self {
            Outcome::Ok(t) => t,
            Outcome::Err(_) => T::default(),
        }
    }
}

impl<T, E> From<Outcome<T, E>> for StdResult<T, E> {
    #[inline]
    fn from(outcome: Outcome<T, E>) -> Self {
        outcome.into_result()
    }
}

impl<T, E> From<StdResult<T, E>> for Outcome<T, E> {
    #[inline]
    fn from(result: StdResult<T, E>) -> Self {
        match result {
            Ok(t) => Outcome::Ok(t),
            Err(e) => Outcome::Err(e),
        }
    }
}

/// Converts a scope body result into an explicit [`Outcome`].
pub trait IntoOutcome {
    type Output;
    type Error;

    fn into_outcome(self) -> Outcome<Self::Output, Self::Error>;
}

impl<T, E> IntoOutcome for StdResult<T, E> {
    type Output = T;
    type Error = E;

    fn into_outcome(self) -> Outcome<T, E> {
        match self {
            Ok(value) => Outcome::Ok(value),
            Err(error) => Outcome::Err(error),
        }
    }
}

impl IntoOutcome for () {
    type Output = ();
    type Error = Infallible;

    fn into_outcome(self) -> Outcome<(), Infallible> {
        Outcome::Ok(())
    }
}

impl IntoOutcome for Infallible {
    type Output = ();
    type Error = Infallible;

    fn into_outcome(self) -> Outcome<(), Infallible> {
        Outcome::Ok(())
    }
}

impl<T, E> IntoOutcome for Outcome<T, E> {
    type Output = T;
    type Error = E;

    fn into_outcome(self) -> Outcome<T, E> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_unit_to_ok() {
        assert_eq!(().into_outcome(), Outcome::Ok(()));
    }

    #[test]
    fn converts_result_variants() {
        assert_eq!(Ok::<_, &str>(42).into_outcome(), Outcome::Ok(42));
        assert_eq!(
            Err::<i32, _>("failed").into_outcome(),
            Outcome::Err("failed")
        );
    }

    #[test]
    fn preserves_explicit_outcomes() {
        assert_eq!(Outcome::Ok::<_, &str>(42).into_outcome(), Outcome::Ok(42));
    }

    #[test]
    fn test_queries_and_conversions() {
        let ok: Outcome<i32, &str> = Outcome::Ok(42);
        let err: Outcome<i32, &str> = Outcome::Err("err");

        assert!(ok.is_ok());
        assert!(ok.is_ok_and(|&x| x == 42));
        assert!(!ok.is_err());

        assert!(err.is_err());
        assert!(err.is_err_and(|&e| e == "err"));
        assert!(!err.is_ok());

        assert_eq!(ok.ok(), Some(42));
        assert_eq!(ok.err(), None);
        assert_eq!(err.ok(), None);
        assert_eq!(err.err(), Some("err"));

        assert_eq!(ok.as_ref(), Outcome::Ok(&42));
        assert_eq!(err.as_ref(), Outcome::Err(&"err"));

        let mut mut_ok: Outcome<i32, &str> = Outcome::Ok(10);
        if let Outcome::Ok(v) = mut_ok.as_mut() {
            *v = 20;
        }
        assert_eq!(mut_ok, Outcome::Ok(20));

        let res: StdResult<i32, &str> = ok.into();
        assert_eq!(res, Ok(42));
        let outcome_back: Outcome<i32, &str> = res.into();
        assert_eq!(outcome_back, Outcome::Ok(42));
    }

    #[test]
    fn test_combinators_and_unwraps() {
        let ok: Outcome<i32, &str> = Outcome::Ok(2);
        let err: Outcome<i32, &str> = Outcome::Err("error");

        assert_eq!(ok.map(|x| x * 2), Outcome::Ok(4));
        assert_eq!(err.map(|x| x * 2), Outcome::Err("error"));

        assert_eq!(ok.map_or(0, |x| x * 2), 4);
        assert_eq!(err.map_or(0, |x| x * 2), 0);

        assert_eq!(ok.map_or_else(|_| 0, |x| x * 2), 4);
        assert_eq!(err.map_or_else(|e| e.len(), |_| 0), 5);

        assert_eq!(ok.map_err(|e| e.len()), Outcome::Ok(2));
        assert_eq!(err.map_err(|e| e.len()), Outcome::Err(5));

        assert_eq!(ok.and_then(|x| Outcome::Ok(x + 1)), Outcome::Ok(3));
        assert_eq!(err.and_then(|x| Outcome::Ok(x + 1)), Outcome::Err("error"));

        assert_eq!(
            ok.or_else(|_: &str| Outcome::<i32, usize>::Ok(100)),
            Outcome::Ok(2)
        );
        assert_eq!(err.or_else(|e| Outcome::Err(e.len())), Outcome::Err(5));

        assert_eq!(ok.unwrap_or(0), 2);
        assert_eq!(err.unwrap_or(0), 0);

        assert_eq!(ok.unwrap_or_else(|_| 0), 2);
        assert_eq!(err.unwrap_or_else(|e| e.len() as i32), 5);

        assert_eq!(ok.unwrap_or_default(), 2);
        assert_eq!(err.unwrap_or_default(), 0);

        assert_eq!(ok.unwrap(), 2);
        assert_eq!(ok.expect("should be ok"), 2);
        assert_eq!(err.unwrap_err(), "error");
        assert_eq!(err.expect_err("should be err"), "error");
    }

    #[test]
    #[should_panic(expected = "custom panic: \"error\"")]
    fn test_expect_panics() {
        let err: Outcome<i32, &str> = Outcome::Err("error");
        err.expect("custom panic");
    }
}
