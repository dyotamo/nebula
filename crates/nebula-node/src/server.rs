use std::{io, sync::Arc};

use nebula_protocol::{read_request, write_response};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, warn};

use crate::{engine::CacheStore, handler};

pub async fn run(listener: TcpListener, store: Arc<CacheStore>) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache node listening");

    loop {
        let (socket, peer) = listener.accept().await?;
        handler::log_connection(peer);
        let store = Arc::clone(&store);

        tokio::spawn(async move {
            if let Err(err) = serve_connection(socket, store).await {
                warn!(%peer, error = %err, "connection ended");
            }
        });
    }
}

async fn serve_connection(mut socket: TcpStream, store: Arc<CacheStore>) -> io::Result<()> {
    loop {
        let request = match read_request(&mut socket).await {
            Ok(request) => request,
            Err(nebula_protocol::ProtocolError::Io(err))
                if err.kind() == io::ErrorKind::UnexpectedEof =>
            {
                return Ok(());
            }
            Err(err) => {
                error!(error = %err, "invalid request frame");
                let _ = socket.shutdown().await;
                return Ok(());
            }
        };

        let response = handler::handle(request, Arc::clone(&store)).await;
        if let Err(err) = write_response(&mut socket, &response).await {
            return Err(io::Error::other(err));
        }
    }
}
