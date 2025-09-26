use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use pin_project_lite::pin_project;

const CAPACITY: usize = 4096;
type BufferLock = Arc<std::sync::Mutex<Vec<u8>>>;
type StreamResult = Result<Bytes, crate::error::Error>;

#[derive(Debug)]
pub struct Writer {
    buf: BufferLock,
}

impl Deref for Writer {
    type Target = Self;

    fn deref(&self) -> &Self::Target {
        self
    }
}

impl DerefMut for Writer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self
    }
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
    }
}

impl<TFactory, TFut> Stream<TFactory, TFut> {
    pub(super) fn new(factory: TFactory) -> Self {
        Self {
            state: StreamState::InitOrFinish {
                factory: Some(factory),
            },
            buf: Arc::new(Vec::with_capacity(CAPACITY).into()),
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

impl<TFactory, TFut> futures_core::Stream for Stream<TFactory, TFut>
where
    TFactory: FnOnce(Writer) -> TFut,
    TFut: Future<Output = Result<(), crate::Error>>,
{
    type Item = Result<Bytes, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let that = self.project();
        that.state.poll_next(cx, that.buf)
    }
}
impl<TFactory, TFut> StreamState<TFactory, TFut>
where
    TFactory: FnOnce(Writer) -> TFut,
    TFut: Future<Output = Result<(), crate::Error>>,
{
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &BufferLock,
    ) -> Poll<Option<StreamResult>> {
        loop {
            match self.as_mut().project() {
                StreamStateProj::InitOrFinish { factory } => match factory.take() {
                    Some(factory) => {
                        let writer = Writer { buf: buf.clone() };
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
                        Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
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
        let stream = super::Stream::new(|_| async move { Result::<_, crate::Error>::Ok(()) });
        let mut stream = std::pin::pin!(stream);
        let item = next(stream.as_mut()).await;

        assert!(item.is_none(), "{item:?}");
    }

    #[tokio::test]
    async fn write_double_u8() {
        let stream = super::Stream::new(|w: Writer| async move {
            let mut w = std::pin::Pin::new(w);
            write_all(Pin::as_mut(&mut w), &[42]).await.unwrap();
            write_all(Pin::as_mut(&mut w), &[42]).await.unwrap();
            Result::<_, crate::Error>::Ok(())
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(stream.as_mut()).await;

        assert_eq!(Bytes::from_static(&[42, 42]), item.unwrap().unwrap());
    }

    #[tokio::test]
    async fn write_single_u8() {
        let stream = super::Stream::new(|w: Writer| async move {
            let mut w = std::pin::Pin::new(w);
            write_all(Pin::as_mut(&mut w), &[42]).await.unwrap();
            Result::<_, crate::Error>::Ok(())
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(stream.as_mut()).await;

        assert_eq!(Bytes::from_static(&[42]), item.unwrap().unwrap());
    }
    #[tokio::test]
    async fn write_more_than_buffer_capacity_at_once() {
        let stream = super::Stream::new(|w: Writer| async move {
            let mut w = std::pin::Pin::new(w);
            write_all(Pin::as_mut(&mut w), &vec![42; CAPACITY + 1])
                .await
                .unwrap();
            Result::<_, crate::Error>::Ok(())
        });
        let mut stream = std::pin::pin!(stream);
        let item = next(stream.as_mut()).await;
        assert_eq!(Bytes::from(vec![42; CAPACITY]), item.unwrap().unwrap());
        let item = next(stream.as_mut()).await;
        assert_eq!(Bytes::from_static(&[42]), item.unwrap().unwrap());
    }

    async fn write_all<T: AsyncWrite>(
        mut stream: Pin<&mut T>,
        mut data: &[u8],
    ) -> std::io::Result<usize> {
        loop {
            let written = std::future::poll_fn(|cx| stream.as_mut().poll_write(cx, data)).await?;
            if data.len() == written {
                return Ok(data.len());
            } else {
                data = &data[written..];
            }
        }
    }
    async fn next<T: Stream>(mut stream: Pin<&mut T>) -> Option<T::Item>
    where
        T::Item: std::fmt::Debug,
    {
        std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
    }
}
