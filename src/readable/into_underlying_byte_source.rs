use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;

use futures_util::future::{AbortHandle, TryFutureExt, abortable};
use futures_util::io::{AsyncRead, AsyncReadExt};
use js_sys::{Error as JsError, Promise, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::util::{checked_cast_to_u32, clamp_to_usize};

use super::sys;

pub(crate) struct IntoUnderlyingByteSource {
    inner: Rc<RefCell<Inner>>,
    default_buffer_len: usize,
    controller: Option<sys::ReadableByteStreamController>,
    pull_handle: Option<AbortHandle>,
}

impl IntoUnderlyingByteSource {
    pub fn new(async_read: Box<dyn AsyncRead>, default_buffer_len: usize) -> Self {
        IntoUnderlyingByteSource {
            inner: Rc::new(RefCell::new(Inner::new(async_read))),
            default_buffer_len,
            controller: None,
            pull_handle: None,
        }
    }

    /// Converts into a raw [`UnderlyingSource`](sys::UnderlyingSource) object,
    /// with `start`, `pull` and `cancel` backed by imported closures.
    /// The closures (and thus the source) live as long as the JS object,
    /// and are deallocated through GC finalization.
    pub fn into_raw(self) -> sys::UnderlyingSource {
        let raw = sys::UnderlyingSource::new();
        raw.set_type(sys::ReadableStreamType::Bytes);
        raw.set_auto_allocate_chunk_size(checked_cast_to_u32(self.default_buffer_len) as f64);
        let source = Rc::new(RefCell::new(Some(self)));

        let start = {
            // SAFETY: start() only stores the controller, which cannot panic.
            let source = AssertUnwindSafe(source.clone());
            Closure::<dyn FnMut(sys::ReadableByteStreamController)>::new(move |controller| {
                source
                    .try_borrow_mut()
                    .unwrap_throw()
                    .as_mut()
                    .unwrap_throw()
                    .start(controller)
            })
        };
        raw.set_start(start.into_js_value().unchecked_ref());

        let pull = {
            // SAFETY: Inner::pull() uses the take-and-replace pattern to remain
            // in a clean state if a panic is caught across this closure.
            let source = AssertUnwindSafe(source.clone());
            Closure::<dyn FnMut(sys::ReadableByteStreamController) -> Promise>::new(
                move |controller| {
                    // This mutable borrow can never panic, since the ReadableStream
                    // always queues each operation on the underlying source.
                    source
                        .try_borrow_mut()
                        .unwrap_throw()
                        .as_mut()
                        .unwrap_throw()
                        .pull(controller)
                },
            )
        };
        raw.set_pull(pull.into_js_value().unchecked_ref());

        let cancel = {
            // SAFETY: cancel() only drops the source, which cannot panic.
            let source = AssertUnwindSafe(source);
            Closure::<dyn FnMut()>::new(move || {
                // The stream has been canceled, drop everything.
                *source.try_borrow_mut().unwrap_throw() = None;
            })
        };
        raw.set_cancel(cancel.into_js_value().unchecked_ref());

        raw
    }

    fn start(&mut self, controller: sys::ReadableByteStreamController) {
        self.controller = Some(controller);
    }

    #[allow(clippy::await_holding_refcell_ref)]
    fn pull(&mut self, controller: sys::ReadableByteStreamController) -> Promise {
        let inner = self.inner.clone();
        let fut = async move {
            // This mutable borrow can never panic, since the ReadableStream always queues
            // each operation on the underlying source.
            let mut inner = inner.try_borrow_mut().unwrap_throw();
            inner.pull(controller).await
        };

        // Allow aborting the future from cancel().
        let (fut, handle) = abortable(fut);
        // Ignore errors from aborting the future.
        let fut = fut.unwrap_or_else(|_| Ok(JsValue::undefined()));

        self.pull_handle = Some(handle);
        // SAFETY: We use the take-and-replace pattern in Inner::pull() to ensure
        // that if a panic occurs, the async_read is already taken out of the Option,
        // leaving it in a clean None state. This prevents use of corrupted state
        // after a panic is caught.
        future_to_promise(AssertUnwindSafe(fut))
    }
}

impl Drop for IntoUnderlyingByteSource {
    fn drop(&mut self) {
        // Abort the pending pull, if any.
        if let Some(handle) = self.pull_handle.take() {
            handle.abort();
        }
    }
}

struct Inner {
    async_read: Option<Pin<Box<dyn AsyncRead>>>,
    buffer: Vec<u8>,
}

impl Inner {
    fn new(async_read: Box<dyn AsyncRead>) -> Self {
        Inner {
            async_read: Some(async_read.into()),
            buffer: Vec::new(),
        }
    }

    async fn pull(
        &mut self,
        controller: sys::ReadableByteStreamController,
    ) -> Result<JsValue, JsValue> {
        // We set autoAllocateChunkSize, so there should always be a BYOB request.
        let request = controller.byob_request().unwrap_throw();
        // Resize the buffer to fit the BYOB request.
        let request_view = request.view().unwrap_throw().unchecked_into::<Uint8Array>();
        let request_len = clamp_to_usize(request_view.byte_length());
        if self.buffer.len() < request_len {
            self.buffer.resize(request_len, 0);
        }

        // Take the async_read out before the fallible/panickable operation.
        // This ensures that if a panic occurs, self.async_read is already None,
        // so any subsequent call will fail cleanly instead of using corrupted state.
        let mut async_read = self.async_read.take().unwrap_throw();

        match async_read.read(&mut self.buffer[0..request_len]).await {
            Ok(0) => {
                // Stream closed: don't put it back, clear buffer, close controller
                self.buffer = Vec::new();
                controller.close()?;
                request.respond_with_u32(0)?;
            }
            Ok(bytes_read) => {
                // Success: put the async_read back for reuse
                self.async_read = Some(async_read);
                // Copy read bytes from buffer to BYOB request view
                debug_assert!(bytes_read <= request_len);
                let bytes_read_u32 = checked_cast_to_u32(bytes_read);
                let dest = Uint8Array::new_with_byte_offset_and_length(
                    &request_view.buffer(),
                    request_view.byte_offset(),
                    bytes_read_u32,
                );
                dest.copy_from(&self.buffer[0..bytes_read]);
                // Respond to BYOB request
                request.respond_with_u32(bytes_read_u32)?;
            }
            Err(err) => {
                // Error: don't put it back, clear buffer, return error
                self.buffer = Vec::new();
                return Err(JsError::new(&err.to_string()).into());
            }
        };
        // Panic: async_read is dropped during unwind, self.async_read remains None
        Ok(JsValue::undefined())
    }
}
