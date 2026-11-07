macro_rules! generated_user_storage {
    (@collect [$provided:ident $accepted:ident] [$($output:tt)*]) => {
        pub(crate) enum UringUserPayloadStorage {
            $($output)*
        }
    };
    (
        @collect [$provided:ident $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType), ProvidedBuf($record),]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [provided accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident no_accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided accepted]
            [$($output)* $user_variant($OpType), AcceptedSocket($record),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided $accepted]
            [$($output)* $user_variant($OpType), $record_variant($record),]
            $($tail)*
        );
    };
}

macro_rules! generated_kernel_storage {
    (@collect [$($output:tt)*]) => {
        pub(crate) enum UringKernelPayloadStorage {
            $($output)*
        }
    };
    (
        @collect [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_kernel_storage!(@collect
            [$($output)* $kernel_variant($kernel_type),]
            $($tail)*
        );
    };
}

macro_rules! generated_user_ids {
    (@collect [$provided:ident $accepted:ident] [$($arms:tt)*]) => {
        impl UringUserPayloadStorage {
            pub(super) const fn payload_id(&self) -> PayloadId {
                match self {
                    $($arms)*
                }
            }
        }
    };
    (
        @collect [$provided:ident $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: UdpRecvPacket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided $accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::UdpRecvPacket(_) => PayloadId::Record("UdpRecvPacket"),
            ]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::ProvidedBuf(_) => PayloadId::Record("ProvidedBuf"),
            ]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident no_accepted] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::AcceptedSocket(_) => PayloadId::Record("AcceptedSocket"),
            ]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident accepted] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided $accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::$record_variant(_) => PayloadId::Record(stringify!($record_variant)),
            ]
            $($tail)*
        );
    };
}

macro_rules! generated_kernel_ids {
    (@collect [$($arms:tt)*]) => {
        impl UringKernelPayloadStorage {
            pub(super) const fn payload_id(&self) -> PayloadId {
                match self {
                    $($arms)*
                }
            }
        }
    };
    (
        @collect [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_kernel_ids!(@collect
            [$($arms)* Self::$kernel_variant(_) => PayloadId::Kernel(stringify!($kernel_variant)),]
            $($tail)*
        );
    };
}

macro_rules! declare_payload_storage {
    ($($operations:tt)*) => {
        generated_user_storage!(@collect [no_provided no_accepted] [] $($operations)*);
        generated_kernel_storage!(@collect [] $($operations)*);
        generated_user_ids!(@collect [no_provided no_accepted] [] $($operations)*);
        generated_kernel_ids!(@collect [] $($operations)*);

        impl UringUserPayload {
            pub(super) fn from_storage(storage: UringUserPayloadStorage) -> Self {
                Self { storage }
            }

            pub(super) fn into_storage(self) -> UringUserPayloadStorage {
                self.storage
            }

            pub(super) const fn payload_id(&self) -> PayloadId {
                self.storage.payload_id()
            }
        }
    };
}

macro_rules! declare_uring_operations {
    ($($operations:tt)*) => {
        declare_payload_storage!($($operations)*);
        #[cfg(test)]
        const OPERATION_COVERAGE: &[OperationCoverage] =
            operation_coverage_entries!(@collect [$($operations)*] []);
        declare_uring_operations_impl!($($operations)*);
    };
}

macro_rules! declare_uring_operations_impl {
    () => {};
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_spec!(
            $OpType,
            $kernel_type,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory
        );
        impl_uring_single_shot_op!($OpType, $completion);
        impl_uring_operation_descriptor! {
            $OpType {
                user: $user_variant,
                kernel: $kernel_variant,
                kernel_type: $kernel_type,
                kind: $kind,
                completion: $completion,
                record: submit,
                strategy: $strategy,
                cardinality: $cardinality,
                new_kernel: $new_kernel,
                make_sqe: $make_sqe,
                resolve_chunks: $resolve_chunks,
                on_complete: $on_complete,
                get_timeout: $get_timeout,
                record_factory: $record_factory,
                cleanup: $cleanup,
                map: $map,
            };
        }
        declare_uring_operations_impl!($($rest)*);
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_spec!(
            $OpType,
            $kernel_type,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory
        );
        impl_uring_record_payload_op!($OpType, $record_variant, $record, $completion);
        impl_uring_single_shot_marker!($cardinality, $OpType);
        impl_uring_operation_descriptor! {
            $OpType {
                user: $user_variant,
                kernel: $kernel_variant,
                kernel_type: $kernel_type,
                kind: $kind,
                completion: $completion,
                record: $record_variant($record),
                strategy: $strategy,
                cardinality: $cardinality,
                new_kernel: $new_kernel,
                make_sqe: $make_sqe,
                resolve_chunks: $resolve_chunks,
                on_complete: $on_complete,
                get_timeout: $get_timeout,
                record_factory: $record_factory,
                cleanup: $cleanup,
                map: $map,
            };
        }
        declare_uring_operations_impl!($($rest)*);
    };
}
