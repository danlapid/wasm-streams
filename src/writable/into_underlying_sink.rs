use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;

use futures_util::{Sink, SinkExt};
use js_sys::Promise;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use super::sys;

pub(crate) struct IntoUnderlyingSink {
    inner: Rc<RefCell<Inner>>,
}

impl IntoUnderlyingSink {
    pub fn new(sink: Box<dyn Sink<JsValue, Error = JsValue>>) -> Self {
        IntoUnderlyingSink {
            inner: Rc::new(RefCell::new(Inner::new(sink))),
        }
    }

    /// Converts into a raw [`UnderlyingSink`](sys::UnderlyingSink) object,
    /// with `write`, `close` and `abort` backed by imported closures.
    /// The closures (and thus the sink) live as long as the JS object,
    /// and are deallocated through GC finalization.
    pub fn into_raw(self) -> sys::UnderlyingSink {
        let sink = Rc::new(RefCell::new(Some(self)));
        let raw = sys::UnderlyingSink::new();

        let write = {
            // SAFETY: Inner::write() uses the take-and-replace pattern to remain
            // in a clean state if a panic is caught across this closure.
            let sink = AssertUnwindSafe(sink.clone());
            Closure::<dyn FnMut(JsValue) -> Promise>::new(move |chunk| {
                // This mutable borrow can never panic, since the WritableStream
                // always queues each operation on the underlying sink.
                sink.try_borrow_mut()
                    .unwrap_throw()
                    .as_mut()
                    .unwrap_throw()
                    .write(chunk)
            })
        };
        raw.set_write(write.into_js_value().unchecked_ref());

        let close = {
            // SAFETY: close() takes the sink out before any fallible operation.
            let sink = AssertUnwindSafe(sink.clone());
            Closure::<dyn FnMut() -> Promise>::new(move || {
                sink.try_borrow_mut()
                    .unwrap_throw()
                    .take()
                    .unwrap_throw()
                    .close()
            })
        };
        raw.set_close(close.into_js_value().unchecked_ref());

        let abort = {
            // SAFETY: abort() only drops the sink, which cannot panic.
            let sink = AssertUnwindSafe(sink);
            Closure::<dyn FnMut(JsValue) -> Promise>::new(move |reason| {
                sink.try_borrow_mut()
                    .unwrap_throw()
                    .take()
                    .unwrap_throw()
                    .abort(reason)
            })
        };
        raw.set_abort(abort.into_js_value().unchecked_ref());

        raw
    }

    #[allow(clippy::await_holding_refcell_ref)]
    fn write(&mut self, chunk: JsValue) -> Promise {
        let inner = self.inner.clone();
        // SAFETY: We use the take-and-replace pattern in Inner::write() to ensure
        // that if a panic occurs, the sink is already taken out of the Option,
        // leaving it in a clean None state. This prevents use of corrupted state
        // after a panic is caught.
        future_to_promise(AssertUnwindSafe(async move {
            // This mutable borrow can never panic, since the WritableStream always queues
            // each operation on the underlying sink.
            let mut inner = inner.try_borrow_mut().unwrap_throw();
            inner.write(chunk).await.map(|_| JsValue::undefined())
        }))
    }

    #[allow(clippy::await_holding_refcell_ref)]
    fn close(self) -> Promise {
        // SAFETY: Inner::close() takes the sink before the fallible operation.
        future_to_promise(AssertUnwindSafe(async move {
            let mut inner = self.inner.try_borrow_mut().unwrap_throw();
            inner.close().await.map(|_| JsValue::undefined())
        }))
    }

    #[allow(clippy::await_holding_refcell_ref)]
    fn abort(self, reason: JsValue) -> Promise {
        // SAFETY: Inner::abort() just sets sink to None, no fallible operation.
        future_to_promise(AssertUnwindSafe(async move {
            let mut inner = self.inner.try_borrow_mut().unwrap_throw();
            inner.abort(reason).await.map(|_| JsValue::undefined())
        }))
    }
}

struct Inner {
    sink: Option<Pin<Box<dyn Sink<JsValue, Error = JsValue>>>>,
}

impl Inner {
    fn new(sink: Box<dyn Sink<JsValue, Error = JsValue>>) -> Self {
        Inner {
            sink: Some(sink.into()),
        }
    }

    async fn write(&mut self, chunk: JsValue) -> Result<(), JsValue> {
        // Take the sink out before the fallible/panickable operation.
        // This ensures that if a panic occurs, self.sink is already None,
        // so any subsequent call will fail cleanly instead of using corrupted state.
        let mut sink = self.sink.take().unwrap_throw();

        match sink.send(chunk).await {
            Ok(()) => {
                // Success: put the sink back for reuse
                self.sink = Some(sink);
                Ok(())
            }
            Err(err) => {
                // Error: the sink is dropped, self.sink remains None
                Err(err)
            }
        }
        // Panic: sink is dropped during unwind, self.sink remains None
    }

    async fn close(&mut self) -> Result<(), JsValue> {
        // Take ownership and close - sink is dropped after close completes
        self.sink.take().unwrap_throw().close().await
    }

    async fn abort(&mut self, _reason: JsValue) -> Result<(), JsValue> {
        // Take and drop the sink immediately
        self.sink = None;
        Ok(())
    }
}
