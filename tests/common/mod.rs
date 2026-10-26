#![allow(dead_code)]

use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::types::WsRole;
use engineering_challenge::websocket::ws::Ws;

/// Creates a mock Ws connected to a local TCP loopback server without network I/O.
pub async fn mock_ws(status: WsStatus, role: WsRole) -> Ws {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (server_stream, _) = listener.accept().await.unwrap();

    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1024];
        let mut s = server_stream;
        while let Ok(n) = s.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    });

    let stream = MaybeTlsStream::Plain(client_stream);
    let socket = WebSocketStream::from_raw_socket(stream, Role::Client, None).await;
    Ws::new(socket, status, role, 0)
}

/// Creates a mock Ws connected to a full duplex mock WebSocket server.
pub async fn mock_ws_with_server(
    status: WsStatus,
    role: WsRole,
) -> (Ws, WebSocketStream<tokio::net::TcpStream>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move {
        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let client_stream = MaybeTlsStream::Plain(client_stream);
        let (client_ws, _) = tokio_tungstenite::client_async("ws://localhost", client_stream)
            .await
            .unwrap();
        client_ws
    });

    let (server_stream, _) = listener.accept().await.unwrap();
    let server_ws = tokio_tungstenite::accept_async(server_stream)
        .await
        .unwrap();

    let client_ws = client_task.await.unwrap();
    (Ws::new(client_ws, status, role, 0), server_ws)
}

/// Drains mock server frames in a background task.
pub fn drain_mock_server_background(mut server: WebSocketStream<tokio::net::TcpStream>) {
    tokio::spawn(async move {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        while let Some(msg) = server.next().await {
            match msg {
                Ok(Message::Ping(p)) => {
                    let _ = server.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });
}
