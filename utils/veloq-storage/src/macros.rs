
macro_rules! impl_state_int {
    ($ty:ty, $self:ident, $order:ident, $val:ident, $curr:ident, $new:ident, $success:ident, $failure:ident,
     new($new_val:ident) $new_expr:block,
     load() $load_expr:block,
     store($store_val:ident) $store_expr:block,
     fetch_add($add_val:ident) $add_expr:block,
     fetch_sub($sub_val:ident) $sub_expr:block,
     fetch_and($and_val:ident) $and_expr:block,
     fetch_or($or_val:ident) $or_expr:block,
     compare_exchange($ce_curr:ident, $ce_new:ident, $ce_s:ident, $ce_f:ident) $ce_expr:block,
     compare_exchange_weak($cew_curr:ident, $cew_new:ident, $cew_s:ident, $cew_f:ident) $cew_expr:block
    ) => {
        impl $crate::StateInt for $ty {
            fn new($new_val: usize) -> Self { $new_expr }
            fn load(&$self, $order: ::std::sync::atomic::Ordering) -> usize { $load_expr }
            fn store(&$self, $store_val: usize, $order: ::std::sync::atomic::Ordering) { $store_expr }
            fn fetch_add(&$self, $add_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $add_expr }
            fn fetch_sub(&$self, $sub_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $sub_expr }
            fn fetch_and(&$self, $and_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $and_expr }
            fn fetch_or(&$self, $or_val: usize, $order: ::std::sync::atomic::Ordering) -> usize { $or_expr }
            fn compare_exchange(&$self, $ce_curr: usize, $ce_new: usize, $ce_s: ::std::sync::atomic::Ordering, $ce_f: ::std::sync::atomic::Ordering) -> Result<usize, usize> { $ce_expr }
            fn compare_exchange_weak(&$self, $cew_curr: usize, $cew_new: usize, $cew_s: ::std::sync::atomic::Ordering, $cew_f: ::std::sync::atomic::Ordering) -> Result<usize, usize> { $cew_expr }
        }
    };
}

macro_rules! impl_ptr_state_wrapper {
    ($name:ident, $trait:ident, $val:ty, $inner_ty:ty, $self:ident, $order:ident,
     new($new_ptr:ident) $new_expr:block,
     load() $load_expr:block,
     store($store_ptr:ident) $store_expr:block,
     swap($swap_ptr:ident) $swap_expr:block,
     compare_exchange($ce_curr:ident, $ce_new:ident, $ce_s:ident, $ce_f:ident) $ce_expr:block,
     compare_exchange_weak($cew_curr:ident, $cew_new:ident, $cew_s:ident, $cew_f:ident) $cew_expr:block,
     $(unsafe_impl $unsafe_impl:item)*
    ) => {
        pub struct $name<T>($inner_ty);
        $( $unsafe_impl )*
        impl<T> $crate::$trait<T> for $name<T> {
            fn new(ptr: $val) -> Self { let $new_ptr = ptr; $new_expr }
            fn load(&$self, $order: ::std::sync::atomic::Ordering) -> $val { $load_expr }
            fn store(&$self, ptr: $val, $order: ::std::sync::atomic::Ordering) { let $store_ptr = ptr; $store_expr }
            fn swap(&$self, ptr: $val, $order: ::std::sync::atomic::Ordering) -> $val { let $swap_ptr = ptr; $swap_expr }
            fn compare_exchange(&$self, $ce_curr: $val, $ce_new: $val, $ce_s: ::std::sync::atomic::Ordering, $ce_f: ::std::sync::atomic::Ordering) -> Result<$val, $val> { $ce_expr }
            fn compare_exchange_weak(&$self, $cew_curr: $val, $cew_new: $val, $cew_s: ::std::sync::atomic::Ordering, $cew_f: ::std::sync::atomic::Ordering) -> Result<$val, $val> { $cew_expr }
        }
    };
}

macro_rules! impl_cell_opt_methods {
    ($val:ty) => {
        fn new(opt: Option<$val>) -> Self {
            Self(::std::cell::Cell::new(opt))
        }
        fn take(&self, _order: ::std::sync::atomic::Ordering) -> Option<$val> {
            self.0.take()
        }
        fn store(&self, val: Option<$val>, _order: ::std::sync::atomic::Ordering) {
            self.0.set(val);
        }
        fn compare_exchange_none(
            &self,
            new: $val,
            _success: ::std::sync::atomic::Ordering,
            _failure: ::std::sync::atomic::Ordering,
        ) -> Result<(), $val> {
            let old = self.0.take();
            if old.is_none() {
                self.0.set(Some(new));
                Ok(())
            } else {
                self.0.set(old);
                Err(new)
            }
        }
    };
}
