use std::{
    future::Future,
    marker::PhantomPinned,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use pin_project_lite::pin_project;

use crate::BoxError;

const CAPACITY: usize = 4096;
type BufferLock = Arc<std::sync::Mutex<Vec<u8>>>;

#[derive(Debug)]
pub struct Writer {
    buf: BufferLock,
    _pin: PhantomPinned,
}

impl futures_io::AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        mut buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut lock = self.buf.lock().unwrap();
        let available_space = CAPACITY - lock.len();
        if available_space == 0 {
            return Poll::Pending;
        }
        if let Some(x) = buf.get(0..available_space) {
            buf = x;
        }
        lock.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pin_project! {
    pub(super) struct Stream<TFactory, TFut> {
        #[pin] state: StreamState<TFactory, TFut>,
        buf: BufferLock,
        _pin: PhantomPinned
    }
}

impl<TFactory, TFut, E> Stream<TFactory, TFut>
where
    TFactory: FnOnce(Writer) -> TFut,
    TFut: Future<Output = Result<(), E>>,
    E: Into<BoxError>,
{
    pub(super) fn new(factory: TFactory) -> Self {
        Self {
            state: StreamState::InitOrFinish {
                factory: Some(factory),
            },
            buf: Arc::new(Vec::with_capacity(CAPACITY).into()),
            _pin: PhantomPinned,
        }
    }
}
pin_project! {
    #[project = StreamStateProj]
    #[project_replace = StreamStateProjRepl]
    enum StreamState<TFactory, TFut> {
        // Some if init, none if Finish
        InitOrFinish { factory: Option<TFactory> },
        Running { #[pin] future: TFut},
    }
}

impl<TFactory, TFut, E> futures_core::Stream for Stream<TFactory, TFut>
where
    TFactory: FnOnce(Writer) -> TFut,
    TFut: Future<Output = Result<(), E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let that = self.project();
        that.state.poll_next(cx, that.buf)
    }
}
impl<TFactory, TFut, E> StreamState<TFactory, TFut>
where
    TFactory: FnOnce(Writer) -> TFut,
    TFut: Future<Output = Result<(), E>>,
{
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &BufferLock,
    ) -> Poll<Option<Result<Bytes, E>>> {
        loop {
            match self.as_mut().project() {
                StreamStateProj::InitOrFinish { factory } => match factory.take() {
                    Some(factory) => {
                        let writer = Writer {
                            buf: buf.clone(),
                            _pin: PhantomPinned,
                        };
                        let stream_state = Self::Running {
                            future: factory(writer),
                        };
                        self.as_mut().project_replace(stream_state);
                    }
                    None => return Poll::Ready(None),
                },
                StreamStateProj::Running { mut future } => {
                    return match future.as_mut().poll(cx) {
                        Poll::Pending => {
                            let mut lock = buf.lock().unwrap();
                            if lock.len() >= CAPACITY {
                                let data = std::mem::take(lock.deref_mut());
                                Poll::Ready(Some(Ok(data.into())))
                            } else {
                                Poll::Pending
                            }
                        }
                        Poll::Ready(Ok(_)) => {
                            let mut lock = buf.lock().unwrap();
                            if !lock.is_empty() {
                                let data = std::mem::take(lock.deref_mut());
                                self.as_mut()
                                    .project_replace(Self::InitOrFinish { factory: None });
                                Poll::Ready(Some(Ok(data.into())))
                            } else {
                                Poll::Ready(None)
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            self.as_mut()
                                .project_replace(Self::InitOrFinish { factory: None });
                            Poll::Ready(Some(Err(e)))
                        }
                    };
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_core::Stream;
    use futures_io::AsyncWrite;

    use super::*;

    #[tokio::test]
    async fn write_nothing() {
        let stream =
            super::Stream::new(|_| std::future::ready(Result::<_, std::io::Error>::Ok(())));
        let item = next(stream).await;

        assert!(item.is_none(), "{item:?}");
    }

    #[tokio::test]
    async fn write_double_u8() {
        let stream = super::Stream::new(|w: Writer| async move {
            let mut w = std::pin::pin!(w);
            write_all(&mut w, &[42]).await?;
            write_all(w, &[42]).await
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(&mut stream).await;

        assert_eq!(Bytes::from_static(&[42, 42]), item.unwrap().unwrap());
        assert!(next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn write_single_u8() {
        let stream = super::Stream::new(|w: Writer| async move {
            let w = std::pin::pin!(w);
            write_all(w, &[42]).await
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(&mut stream).await;

        assert_eq!(Bytes::from_static(&[42]), item.unwrap().unwrap());
        assert!(next(&mut stream).await.is_none());
    }
    #[tokio::test]
    async fn write_more_than_buffer_capacity_at_once() {
        let stream = super::Stream::new(|w: Writer| async move {
            let w = std::pin::pin!(w);
            write_all(w, &vec![42; CAPACITY + 1]).await
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(&mut stream).await;
        assert_eq!(Bytes::from(vec![42; CAPACITY]), item.unwrap().unwrap());
        let item = next(&mut stream).await;
        assert_eq!(Bytes::from_static(&[42]), item.unwrap().unwrap());
        assert!(next(&mut stream).await.is_none());
    }

    #[tokio::test]
    async fn error_if_future_errors() {
        let stream = super::Stream::new(|_: Writer| async move {
            Err(crate::Error::new(std::io::Error::other("IDK")))
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(&mut stream).await;
        let error = item.unwrap().unwrap_err();
        assert!(format!("{error:?}").contains("IDK"), "Error: {error:?}");
        assert!(next(&mut stream).await.is_none());
    }

    async fn write_all<T: AsyncWrite + Unpin>(
        mut stream: T,
        mut data: &[u8],
    ) -> std::io::Result<()> {
        loop {
            let written =
                std::future::poll_fn(|cx| std::pin::pin!(&mut stream).poll_write(cx, data)).await?;
            if data.len() == written {
                return Ok(());
            } else {
                data = &data[written..];
            }
        }
    }
    async fn next<T: Stream + Unpin>(mut stream: T) -> Option<T::Item>
    where
        T::Item: std::fmt::Debug,
    {
        std::future::poll_fn(|cx| std::pin::pin!(&mut stream).poll_next(cx)).await
    }
}
