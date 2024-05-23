use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    str::FromStr,
    thread,
};

use crate::Error;

use crossbeam_channel::{bounded, unbounded, Sender};
use futures_channel::oneshot;
use rusqlite::{Connection, OpenFlags};

/// A `ClientBuilder` can be used to create a [`Client`] with custom
/// configuration.
///
/// For more information on creating a sqlite connection, see the
/// [rusqlite docs](rusqlite::Connection::open()).
///
/// # Examples
///
/// ```rust
/// # use async_sqlite::ClientBuilder;
/// # async fn run() -> Result<(), async_sqlite::Error> {
/// let client = ClientBuilder::new().path("path/to/db.sqlite3").open().await?;
///
/// // ...
///
/// client.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
    pub(crate) path: Option<PathBuf>,
    pub(crate) flags: OpenFlags,
    pub(crate) journal_mode: Option<JournalMode>,
    pub(crate) synchronous: Option<Synchronous>,
    pub(crate) vfs: Option<String>,
    pub(crate) pragmas: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

impl ClientBuilder {
    /// Returns a new [`ClientBuilder`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Specify the path of the sqlite3 database to open.
    ///
    /// By default, an in-memory database is used.
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().into());
        self
    }

    /// Specify the [`OpenFlags`] to use when opening a new connection.
    ///
    /// By default, [`OpenFlags::default()`] is used.
    pub fn flags(mut self, flags: OpenFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Specify the [`JournalMode`] to set when opening a new connection.
    ///
    /// By default, no `journal_mode` is explicity set.
    pub fn journal_mode(mut self, journal_mode: JournalMode) -> Self {
        self.journal_mode = Some(journal_mode);
        self
    }

    /// Specify the [`SyncMode`] to set when opening a new connection.
    ///
    /// By default, no `sync_mode` is explicity set.
    pub fn synchronous(mut self, synchronous: Synchronous) -> Self {
        self.synchronous = Some(synchronous);
        self
    }

    /// Specify the name of the [vfs](https://www.sqlite.org/vfs.html) to use.
    pub fn vfs(mut self, vfs: &str) -> Self {
        self.vfs = Some(vfs.to_owned());
        self
    }

    /// Returns a new [`Client`] that uses the `ClientBuilder` configuration.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use async_sqlite::ClientBuilder;
    /// # async fn run() -> Result<(), async_sqlite::Error> {
    /// let client = ClientBuilder::new().open().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(self) -> Result<Client, Error> {
        Client::open_async(self).await
    }

    /// Returns a new [`Client`] that uses the `ClientBuilder` configuration,
    /// blocking the current thread.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use async_sqlite::ClientBuilder;
    /// # fn run() -> Result<(), async_sqlite::Error> {
    /// let client = ClientBuilder::new().open_blocking()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_blocking(self) -> Result<Client, Error> {
        Client::open_blocking(self)
    }
}

enum Command {
    Func(Box<dyn FnOnce(&mut Connection) + Send>),
    Shutdown(Box<dyn FnOnce(Result<(), Error>) + Send>),
}

/// Client represents a single sqlite connection that can be used from async
/// contexts.
#[derive(Clone)]
pub struct Client {
    conn_tx: Sender<Command>,
}

impl Client {
    /// Returns the number of messages in the channel.
    pub(crate) fn len(&self) -> usize {
        self.conn_tx.len()
    }

    async fn open_async(builder: ClientBuilder) -> Result<Self, Error> {
        let (open_tx, open_rx) = oneshot::channel();
        Self::open(builder, |res| {
            _ = open_tx.send(res);
        });
        open_rx.await?
    }

    fn open_blocking(builder: ClientBuilder) -> Result<Self, Error> {
        let (conn_tx, conn_rx) = bounded(1);
        Self::open(builder, move |res| {
            _ = conn_tx.send(res);
        });
        conn_rx.recv()?
    }

    fn open<F>(builder: ClientBuilder, func: F)
    where
        F: FnOnce(Result<Self, Error>) + Send + 'static,
    {
        thread::spawn(move || {
            let (conn_tx, conn_rx) = unbounded();

            let mut conn = match Client::create_conn(builder) {
                Ok(conn) => conn,
                Err(err) => {
                    func(Err(err));
                    return;
                }
            };

            let client = Self { conn_tx };
            func(Ok(client));

            while let Ok(cmd) = conn_rx.recv() {
                match cmd {
                    Command::Func(func) => func(&mut conn),
                    Command::Shutdown(func) => match conn.close() {
                        Ok(()) => {
                            func(Ok(()));
                            return;
                        }
                        Err((c, e)) => {
                            conn = c;
                            func(Err(e.into()));
                        }
                    },
                }
            }
        });
    }

    fn create_conn(mut builder: ClientBuilder) -> Result<Connection, Error> {
        let path = builder.path.take().unwrap_or_else(|| ":memory:".into());
        let conn = if let Some(vfs) = builder.vfs.take() {
            Connection::open_with_flags_and_vfs(path, builder.flags, &vfs)?
        } else {
            Connection::open_with_flags(path, builder.flags)?
        };

        if let Some(journal_mode) = builder.journal_mode.take() {
            let val = journal_mode.as_str();
            let out: String =
                conn.pragma_update_and_check(None, "journal_mode", val, |row| row.get(0))?;
            if !out.eq_ignore_ascii_case(val) {
                return Err(Error::PragmaUpdate {
                    name: "journal_mode".into(),
                    exp: val.into(),
                    got: out,
                });
            }
        }

        if let Some(synchronous) = builder.synchronous.take() {
            let val = synchronous.as_str();

            conn.pragma_update(None, "synchronous", val)?;
            let out: i32 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
            if out != synchronous as i32 {
                let got = Synchronous::from_i32(out)
                    .map(|s| s.as_str().into())
                    .unwrap_or_else(|invalid| format!("Invalid: {invalid}").into());

                return Err(Error::PragmaUpdate {
                    name: "synchronous".into(),
                    exp: val.into(),
                    got,
                });
            }
        }

        for (pragma_name, pragma_value) in builder.pragmas {
            // Only a few pragmas automatically return the updated value, and the
            // types are not all the same, so we don't check that pragmas match.
            conn.pragma_update(None, &pragma_name, &pragma_value)?;
        }

        Ok(conn)
    }

    /// Invokes the provided function with a [`rusqlite::Connection`].
    pub async fn conn<F, T, E>(&self, func: F) -> Result<Result<T, E>, Error>
    where
        F: FnOnce(&Connection) -> Result<T, E> + Send + 'static,
        E: From<rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.conn_tx
            .send(Command::Func(Box::new(move |conn| {
                _ = tx.send(func(conn));
            })))
            .map_err(Error::from)?;
        let out = rx.await.map_err(Error::from)?;
        Ok(out)
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`].
    pub async fn conn_mut<F, T, E>(&self, func: F) -> Result<Result<T, E>, Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, E> + Send + 'static,
        E: From<rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.conn_tx
            .send(Command::Func(Box::new(move |conn| {
                _ = tx.send(func(conn));
            })))
            .map_err(Error::from)?;
        let out = rx.await.map_err(Error::from)?;
        Ok(out)
    }

    /// Closes the underlying sqlite connection.
    ///
    /// After this method returns, all calls to `self::conn()` or
    /// `self::conn_mut()` will return an [`Error::Closed`] error.
    pub async fn close(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let func = Box::new(|res| _ = tx.send(res));
        if self.conn_tx.send(Command::Shutdown(func)).is_err() {
            // If the worker thread has already shut down, return Ok here.
            return Ok(());
        }
        // If receiving fails, the connection is already closed.
        rx.await.unwrap_or(Ok(()))
    }

    /// Invokes the provided function with a [`rusqlite::Connection`], blocking
    /// the current thread until completion.
    pub fn conn_blocking<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = bounded(1);
        self.conn_tx.send(Command::Func(Box::new(move |conn| {
            _ = tx.send(func(conn));
        })))?;
        Ok(rx.recv()??)
    }

    /// Invokes the provided function with a mutable [`rusqlite::Connection`],
    /// blocking the current thread until completion.
    pub fn conn_mut_blocking<F, T>(&self, func: F) -> Result<T, Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = bounded(1);
        self.conn_tx.send(Command::Func(Box::new(move |conn| {
            _ = tx.send(func(conn));
        })))?;
        Ok(rx.recv()??)
    }

    /// Closes the underlying sqlite connection, blocking the current thread
    /// until complete.
    ///
    /// After this method returns, all calls to `self::conn_blocking()` or
    /// `self::conn_mut_blocking()` will return an [`Error::Closed`] error.
    pub fn close_blocking(&self) -> Result<(), Error> {
        let (tx, rx) = bounded(1);
        let func = Box::new(move |res| _ = tx.send(res));
        if self.conn_tx.send(Command::Shutdown(func)).is_err() {
            return Ok(());
        }
        // If receiving fails, the connection is already closed.
        rx.recv().unwrap_or(Ok(()))
    }
}

/// The possible sqlite journal modes.
///
/// For more information, please see the [sqlite docs](https://www.sqlite.org/pragma.html#pragma_journal_mode).
#[derive(Clone, Copy, Debug)]
pub enum JournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

impl JournalMode {
    /// Returns the appropriate string representation of the journal mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Persist => "PERSIST",
            Self::Memory => "MEMORY",
            Self::Wal => "WAL",
            Self::Off => "OFF",
        }
    }
}

/// The sqlite synchronous mode.
///
/// For more information, please see the [sqlite docs](https://www.sqlite.org/pragma.html#pragma_synchronous)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Synchronous {
    EXTRA = 3,
    FULL = 2,
    NORMAL = 1,
    OFF = 0,
}

impl Synchronous {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EXTRA => "EXTRA",
            Self::FULL => "FULL",
            Self::NORMAL => "NORMAL",
            Self::OFF => "OFF",
        }
    }

    fn from_i32(value: i32) -> Result<Self, i32> {
        match value {
            3 => Ok(Self::EXTRA),
            2 => Ok(Self::FULL),
            1 => Ok(Self::NORMAL),
            0 => Ok(Self::OFF),
            _ => Err(value),
        }
    }
}

impl FromStr for Synchronous {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EXTRA" => Ok(Self::EXTRA),
            "FULL" => Ok(Self::FULL),
            "NORMAL" => Ok(Self::NORMAL),
            "OFF" => Ok(Self::OFF),
            _ => return Err(()),
        }
    }
}
