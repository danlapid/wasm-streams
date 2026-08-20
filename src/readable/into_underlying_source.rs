use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;

use futures_util::future::{AbortHandle, TryFutureExt, abortable};
use futures_util::stream::{Stream, TryStreamExt};
use js_sys::Promise;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use super::sys;

type JsValueStream = dyn Stream<Item = Result<JsValue, JsValue>>;

pub(crate) struct IntoUnderlyingSource {
    inner: Rc<RefCell<Inner>>,
    pull_handle: Option<AbortHandle>,
}

impl IntoUnderlyingSource {
    pub fn new(stream: Box<JsValueStream>) -> Self {
        IntoUnderlyingSource {
            inner: Rc::new(RefCell::new(Inner::new(stream))),
            pull_handle: None,
        }
    }

    /// Converts into a raw [`UnderlyingSource`](sys::UnderlyingSource) object,
    /// with `pull` and `cancel` backed by imported closures.
    /// The closures (and thus the source) live as long as the JS object,
    /// and are deallocated through GC finalization.
    pub fn into_raw(self) -> sys::UnderlyingSource {
        let source = Rc::new(RefCell::new(Some(self)));
        let raw = sys::UnderlyingSource::new();

        let pull = {
            // SAFETY: Inner::pull() uses the take-and-replace pattern to remain
            // in a clean state if a panic is caught across this closure.
            let source = AssertUnwindSafe(source.clone());
            Closure::<dyn FnMut(sys::ReadableStreamDefaultController) -> Promise>::new(
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

    #[allow(clippy::await_holding_refcell_ref)]
    fn pull(&mut self, controller: sys::ReadableStreamDefaultController) -> Promise {
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
        // that if a panic occurs, the stream is already taken out of the Option,
        // leaving it in a clean None state. This prevents use of corrupted state
        // after a panic is caught.
        future_to_promise(AssertUnwindSafe(fut))
    }
}

impl Drop for IntoUnderlyingSource {
    fn drop(&mut self) {
        // Abort the pending pull, if any.
        if let Some(handle) = self.pull_handle.take() {
            handle.abort();
        }
    }
}

struct Inner {
    stream: Option<Pin<Box<JsValueStream>>>,
}

impl Inner {
    fn new(stream: Box<JsValueStream>) -> Self {
        Inner {
            stream: Some(stream.into()),
        }
    }

    async fn pull(
        &mut self,
        controller: sys::ReadableStreamDefaultController,
    ) -> Result<JsValue, JsValue> {
        // Take the stream out before the fallible/panickable operation.
        // This ensures that if a panic occurs, self.stream is already None,
        // so any subsequent call will fail cleanly instead of using corrupted state.
        let mut stream = self.stream.take().unwrap_throw();

        match stream.try_next().await {
            Ok(Some(chunk)) => {
                // Success with chunk: put the stream back and enqueue
                self.stream = Some(stream);
                controller.enqueue_with_chunk(&chunk)?;
            }
            Ok(None) => {
                // Stream closed: don't put it back (it's exhausted), close controller
                controller.close()?;
            }
            Err(err) => {
                // Error: don't put it back, return the error
                return Err(err);
            }
        };
        // Panic: stream is dropped during unwind, self.stream remains None
        Ok(JsValue::undefined())
    }
}
