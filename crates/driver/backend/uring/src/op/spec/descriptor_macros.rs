macro_rules! descriptor_cardinality {
    (single) => {
        CompletionCardinality::Single
    };
    (multi) => {
        CompletionCardinality::Multi
    };
}

macro_rules! descriptor_resolve_chunks {
    (none) => {
        DescriptorCallbacks::descriptor_resolve_chunks_default
    };
    (read_fixed) => {
        submit::FileSubmission::resolve_chunks_read_fixed
    };
    (read_raw) => {
        submit::FileSubmission::resolve_chunks_read_raw
    };
    (write_fixed) => {
        submit::FileSubmission::resolve_chunks_write_fixed
    };
    (write_raw) => {
        submit::FileSubmission::resolve_chunks_write_raw
    };
}

macro_rules! descriptor_on_complete {
    (default) => {
        DescriptorCallbacks::descriptor_on_complete_default
    };
    (accept) => {
        submit::NetSubmission::on_complete_accept
    };
}

macro_rules! descriptor_get_timeout {
    (default) => {
        DescriptorCallbacks::descriptor_get_timeout_default
    };
    (timeout) => {
        DescriptorCallbacks::descriptor_get_timeout_timeout
    };
    ($get_timeout:ident) => {
        $get_timeout
    };
}

macro_rules! descriptor_record_factory {
    (use_submit) => {
        DescriptorCallbacks::descriptor_record_item_submit
    };
    (provided_buffer) => {
        DescriptorCallbacks::descriptor_record_item_provided
    };
    (accepted_socket) => {
        DescriptorCallbacks::descriptor_record_item_accepted
    };
    (udp_packet) => {
        DescriptorCallbacks::descriptor_record_item_udp_packet
    };
}

macro_rules! descriptor_resume_factory {
    (udp_packet) => {
        DescriptorCallbacks::descriptor_resume_item_udp_packet
    };
    ($other:ident) => {
        DescriptorCallbacks::descriptor_resume_item_none
    };
}

macro_rules! descriptor_generation_factory {
    (udp_packet) => {
        DescriptorCallbacks::descriptor_receive_generation_udp
    };
    ($other:ident) => {
        DescriptorCallbacks::descriptor_receive_generation_none
    };
}

macro_rules! descriptor_cleanup {
    (none) => {
        DescriptorCallbacks::descriptor_cleanup_none
    };
    (close_raw_fd) => {
        submit::SubmissionSupport::completion_cleanup_close_raw_fd
    };
}

macro_rules! descriptor_cleanup_hint {
    (none) => {
        None
    };
    (close_raw_fd) => {
        Some(submit::SubmissionSupport::completion_cleanup_close_raw_fd)
    };
}

macro_rules! descriptor_try_record {
    (submit, $OpType:ty, $payload:ident) => {
        <$OpType as UringOperationDescriptor>::try_user_payload($payload)
    };
    ($record_variant:ident, $OpType:ty, $payload:ident) => {{
        let actual = $payload.payload_id();
        match $payload.into_storage() {
            UringUserPayloadStorage::$record_variant($payload) => Ok($payload),
            _ => Err(projection_mismatch_report(ProjectionMismatch {
                scope: "uring.op.spec.try_record_from_erased",
                operation: stringify!($OpType),
                token: None,
                expected_kernel: None,
                actual_kernel: None,
                expected_user: Some(PayloadId::Record(stringify!($record_variant))),
                actual_user: Some(actual),
            })),
        }
    }};
}

macro_rules! impl_uring_op_spec {
    (
        $OpType:ty,
        $kernel_type:ty,
        $make_sqe:path,
        $resolve_chunks:ident,
        $on_complete:ident,
        $get_timeout:ident,
        $record_factory:ident
    ) => {
        impl UringOpSpec for $OpType {
            type KernelPayload = $kernel_type;

            fn make_sqe(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                env: &SqeEnv<'_>,
                token: SubmitTokenContext,
            ) -> UringResult<squeue::Entry> {
                // SAFETY: the operation row selects the callback for this exact kernel and user
                // payload pair. The callback's pointer validity is upheld by the pinned slot.
                // 安全性：操作表行将此回调与精确的 kernel/user payload 配对；固定 slot 保证回调使用的指针有效。
                unsafe { $make_sqe(kernel, payload, env, token) }
            }

            fn on_complete(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                result: i32,
            ) -> UringResult<usize> {
                // SAFETY: the operation row selects the callback for this exact kernel and user
                // payload pair.
                // 安全性：操作表行将此回调与精确的 kernel/user payload 配对。
                unsafe { descriptor_on_complete!($on_complete)(kernel, payload, result) }
            }

            fn get_timeout(
                kernel: Pin<&Self::KernelPayload>,
                payload: &Self,
            ) -> UringResult<Option<Duration>> {
                descriptor_get_timeout!($get_timeout)(kernel, payload)
            }

            fn resolve_chunks(
                kernel: Pin<&Self::KernelPayload>,
                payload: &Self,
                chunks: &mut [ChunkId],
            ) -> UringResult<usize> {
                Ok(descriptor_resolve_chunks!($resolve_chunks)(
                    kernel, payload, chunks,
                ))
            }

            fn record_item(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                context: RecordItemContext<'_, '_, '_>,
            ) -> UringResult<UringRecordItem> {
                descriptor_record_factory!($record_factory)(kernel, payload, context)
            }

            fn resume_item(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                token: OpToken,
                logical_receiver_generation: u32,
                env: &mut CqeEnv<'_, '_>,
            ) -> UringResult<UringRecordItem> {
                descriptor_resume_factory!($record_factory)(
                    kernel,
                    payload,
                    token,
                    logical_receiver_generation,
                    env,
                )
            }

            fn receive_generation(payload: &Self) -> Option<u32> {
                descriptor_generation_factory!($record_factory)(payload)
            }
        }
    };
}

macro_rules! impl_uring_operation_descriptor {
    (@impl
        $OpType:ty,
        $user_variant:ident,
        $kernel_variant:ident,
        $record:ty,
        $record_projection:ident,
        $record_policy:expr,
        $kind:path,
        $completion:ty,
        $strategy:path,
        $cardinality:ident,
        $new_kernel:path,
        $make_sqe:path,
        $resolve_chunks:ident,
        $on_complete:ident,
        $get_timeout:ident,
        $record_factory:ident,
        $cleanup:ident,
        $map:path
    ) => {
        impl UringOperationDescriptor for $OpType {
            type Completion = $completion;
            type RecordPayload = $record;

            const PAYLOAD_KIND: OpKind = $kind;
            const NAME: &'static str = stringify!($OpType);
            const USER_PAYLOAD_ID: PayloadId = PayloadId::User(stringify!($user_variant));
            const KERNEL_PAYLOAD_ID: PayloadId = PayloadId::Kernel(stringify!($kernel_variant));

            fn encode_kernel_payload(payload: Self::KernelPayload) -> UringKernelPayloadStorage {
                UringKernelPayloadStorage::$kernel_variant(payload)
            }

            fn encode_user_payload(payload: Self) -> UringUserPayload {
                UringUserPayload::from_storage(UringUserPayloadStorage::$user_variant(payload))
            }

            fn try_user_payload(payload: UringUserPayload) -> UringResult<Self> {
                let actual = payload.payload_id();
                match payload.into_storage() {
                    UringUserPayloadStorage::$user_variant(payload) => Ok(payload),
                    _ => Err(projection_mismatch_report(ProjectionMismatch {
                        scope: "uring.op.spec.try_user_payload",
                        operation: Self::NAME,
                        token: None,
                        expected_kernel: None,
                        actual_kernel: None,
                        expected_user: Some(Self::USER_PAYLOAD_ID),
                        actual_user: Some(actual),
                    })),
                }
            }

            fn try_record_from_erased(
                payload: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                descriptor_try_record!($record_projection, $OpType, payload)
            }

            #[cfg(test)]
            fn kernel_payload_ref(
                payload: &UringKernelPayloadStorage,
            ) -> Option<&Self::KernelPayload> {
                match payload {
                    UringKernelPayloadStorage::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self> {
                match &payload.storage {
                    UringUserPayloadStorage::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            unsafe fn with_projected_access<F, R>(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: Option<OpToken>,
                scope: &'static str,
                f: F,
            ) -> UringResult<R>
            where
                F: FnOnce(Pin<&mut Self::KernelPayload>, &mut Self) -> R,
            {
                let (mut op, payload) = access
                    .operation_and_payload_mut()
                    .map_err(|err: SlotAccessError| {
                        projection_access_report(scope, token, err)
                    })?;
                let actual_kernel = op.as_ref().get_ref().payload.payload_id();
                let actual_user = payload.payload_id();
                let op = unsafe { op.as_mut().get_unchecked_mut() };
                let kernel = match &mut op.payload {
                    UringKernelPayloadStorage::$kernel_variant(kernel) => {
                        // SAFETY: `op` is borrowed through `SlotAccess`, so this variant
                        // remains at its stable slot address for the duration of the callback.
                        // 安全性：`op` 通过 `SlotAccess` 借用，因此回调期间该变体始终位于稳定的 slot 地址。
                        unsafe { Pin::new_unchecked(kernel) }
                    }
                    _ => {
                        return Err(projection_mismatch_report(ProjectionMismatch {
                            scope,
                            operation: Self::NAME,
                            token,
                            expected_kernel: Some(Self::KERNEL_PAYLOAD_ID),
                            actual_kernel: Some(actual_kernel),
                            expected_user: Some(Self::USER_PAYLOAD_ID),
                            actual_user: Some(actual_user),
                        }));
                    }
                };
                let user = match &mut payload.storage {
                    UringUserPayloadStorage::$user_variant(user) => user,
                    _ => {
                        return Err(projection_mismatch_report(ProjectionMismatch {
                            scope,
                            operation: Self::NAME,
                            token,
                            expected_kernel: Some(Self::KERNEL_PAYLOAD_ID),
                            actual_kernel: Some(actual_kernel),
                            expected_user: Some(Self::USER_PAYLOAD_ID),
                            actual_user: Some(actual_user),
                        }));
                    }
                };
                Ok(f(kernel, user))
            }

            unsafe fn make_sqe_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                env: &SqeEnv<'_>,
                token: SubmitTokenContext,
            ) -> UringResult<squeue::Entry> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token.op_token()),
                        "uring.op.spec.make_sqe",
                        |kernel, user| Self::make_sqe(kernel, user, env, token),
                    )
                }?
            }

            unsafe fn on_complete_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                result: i32,
            ) -> UringResult<usize> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.on_complete",
                        |kernel, user| Self::on_complete(kernel, user, result),
                    )
                }?
            }

            unsafe fn get_timeout_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
            ) -> UringResult<Option<Duration>> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.get_timeout",
                        |kernel, user| Self::get_timeout(kernel.as_ref(), user),
                    )
                }?
            }

            unsafe fn resolve_chunks_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                chunks: &mut [ChunkId],
            ) -> UringResult<usize> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.resolve_chunks",
                        |kernel, user| Self::resolve_chunks(kernel.as_ref(), user, chunks),
                    )
                }?
            }

            unsafe fn record_item_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                result: i32,
                flags: u32,
                env: &mut CqeEnv<'_, '_>,
            ) -> UringResult<UringRecordItem> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.record_item",
                        |kernel, user| {
                            Self::record_item(
                                kernel,
                                user,
                                RecordItemContext {
                                    result,
                                    flags,
                                    env,
                                },
                            )
                        },
                    )
                }?
            }

            unsafe fn resume_item_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                logical_receiver_generation: u32,
                env: &mut CqeEnv<'_, '_>,
            ) -> UringResult<UringRecordItem> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.resume_item",
                        |kernel, user| {
                            Self::resume_item(
                                kernel,
                                user,
                                token,
                                logical_receiver_generation,
                                env,
                            )
                        },
                    )
                }?
            }

            unsafe fn receive_generation_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
            ) -> Option<u32> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        None,
                        "uring.op.spec.receive_generation",
                        |_, user| Self::receive_generation(user),
                    )
                }
                .ok()
                .flatten()
            }

            fn descriptor() -> &'static OperationDescriptor<Self> {
                static DESCRIPTOR: OperationDescriptor<$OpType> = OperationDescriptor {
                    new_kernel: $new_kernel,
                    encode_kernel: <$OpType as UringOperationDescriptor>::encode_kernel_payload,
                    encode_submit: <$OpType as UringOperationDescriptor>::encode_user_payload,
                    try_record: <$OpType as UringOperationDescriptor>::try_record_from_erased,
                    map_completion: $map,
                    erased: ErasedOperationDescriptor {
                        name: <$OpType as UringOperationDescriptor>::NAME,
                        strategy: $strategy,
                        cardinality: descriptor_cardinality!($cardinality),
                        record_policy: $record_policy,
                        make_sqe: <$OpType as UringOperationDescriptor>::make_sqe_dispatch,
                        on_complete: <$OpType as UringOperationDescriptor>::on_complete_dispatch,
                        completion_cleanup: descriptor_cleanup!($cleanup),
                        completion_cleanup_hint: descriptor_cleanup_hint!($cleanup),
                        orphan_cleanup: descriptor_cleanup!($cleanup),
                        get_timeout: <$OpType as UringOperationDescriptor>::get_timeout_dispatch,
                        resolve_chunks:
                            <$OpType as UringOperationDescriptor>::resolve_chunks_dispatch,
                        record_item: <$OpType as UringOperationDescriptor>::record_item_dispatch,
                        resume_item: <$OpType as UringOperationDescriptor>::resume_item_dispatch,
                        receive_generation:
                            <$OpType as UringOperationDescriptor>::receive_generation_dispatch,
                    },
                };
                &DESCRIPTOR
            }
        }
    };
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
    ) => {
        impl_uring_operation_descriptor!(@impl
            $OpType,
            $user_variant,
            $kernel_variant,
            $OpType,
            submit,
            RecordPolicy::UseSubmitPayload,
            $kind,
            $completion,
            $strategy,
            $cardinality,
            $new_kernel,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory,
            $cleanup,
            $map
        );
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
    ) => {
        impl_uring_operation_descriptor!(@impl
            $OpType,
            $user_variant,
            $kernel_variant,
            $record,
            $record_variant,
            descriptor_record_policy!($record_variant),
            $kind,
            $completion,
            $strategy,
            $cardinality,
            $new_kernel,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory,
            $cleanup,
            $map
        );
    };
}

macro_rules! descriptor_record_policy {
    (ProvidedBuf) => {
        RecordPolicy::NewProvidedBuffer
    };
    (AcceptedSocket) => {
        RecordPolicy::NewAcceptedSocket
    };
    (UdpRecvPacket) => {
        RecordPolicy::UdpMultishot
    };
}

/// 记录 payload 与提交 payload **不是同一个类型**的操作的 [`IntoPlatformOp`]。
///
/// 走这一支的有两类，它们是正交的两件事，只是恰好都不能用上面那个宏：
///
/// - 提交 payload 必须留在 slot 里（multishot：`AcceptMulti` / `RecvMulti`），因为内核还要
///   拿它继续产出完成；
/// - 产物在提交时还不存在（provided buffer：`RecvProvided` / `RecvMulti`），因为 buffer 是
///   内核在数据到达时才从环里挑的。
///
/// descriptor row 中的 completion mapper 把 CQE 的结果变成这个操作的 `Completion`。它
/// 拿不到 payload——记录 payload 里没有提交时的信息，这正是两者分家的意思。
/// Used when submit and record payload types differ: multishot operations retain the submit payload, while provided buffers exist only after the kernel selects one; the completion mapper receives only the result.
macro_rules! impl_uring_record_payload_op {
    ($OpType:ty, $record_variant:ident, $record:ty, $completion:ty) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOperationDescriptor>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let kernel_payload = (descriptor.new_kernel)(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.encode_submit)(payload)
            }

            fn try_record_from_erased(
                erased: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.try_record)(erased)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                OpCompletion::new((descriptor.map_completion)(res), payload)
            }

            /// 提交同步失败时 slot 里躺着的是**提交** payload，不是任何一条完成的产物——
            /// 没有 item 可以交给用户，也没有用户交出来的资源要还。默认实现会把它当记录
            /// payload 去投影，那必然失败并给出一个含义完全错误的 `PayloadTypeMismatch`。
            /// On synchronous submission failure there is no completion item or user resource to return; projecting the submit payload as a record payload would report a misleading type mismatch.
            fn submit_failed(
                erased: UringUserPayload,
                report: Report<UringError>,
            ) -> OpResult<Self::Output, UringError, Self::Completion> {
                drop(erased);
                OpResult::ResourceLost(OpError::new(LostReason::Other, report))
            }
        }
    };
}

/// 单发操作的 [`IntoPlatformOp`]：提交 payload 就是记录 payload，就是操作自己。
/// `IntoPlatformOp` implementation for single-shot operations, where the operation itself serves as both submit and record payload.
macro_rules! impl_uring_single_shot_op {
    ($OpType:ty, $completion:ty) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $OpType;
            type Output = $OpType;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOperationDescriptor>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let kernel_payload = (descriptor.new_kernel)(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.encode_submit)(payload)
            }

            fn try_record_from_erased(
                payload: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.try_record)(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let completion = (descriptor.map_completion)(res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
}

macro_rules! impl_uring_single_shot_marker {
    (single, $OpType:ty) => {
        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
    (multi, $OpType:ty) => {};
}
