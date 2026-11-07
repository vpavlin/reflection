//! A headless Reflection: open one document, optionally type a line into it, and print the text
//! as it changes. Uses whatever transport `REFLECTION_BACKEND` selects (`p2panda`, `logos`,
//! `bridge`, ...), so it can stand in for the GTK app in network tests.
//!
//!   cargo run -p reflection-doc --features logos-ffi --example note -- <doc-id-hex> [line] [seconds]
use std::time::Duration;

use reflection_doc::document::DocumentId;
use reflection_doc::identity::SigningKey;
use reflection_doc::service::Service;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .init();
    let args: Vec<String> = std::env::args().collect();
    let id_hex = args
        .get(1)
        .expect("usage: note <doc-id-hex> [line] [seconds]");
    let line = args.get(2).filter(|l| !l.is_empty()).cloned();
    let seconds: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(120);

    let bytes: [u8; 32] = hex::decode(id_hex)
        .expect("hex id")
        .try_into()
        .expect("32 bytes");
    let context = glib::MainContext::default();
    context.clone().block_on(async move {
        let service = Service::new(&SigningKey::generate(), None);
        service.startup().await.expect("startup");
        let document = service.join_document_with_main_context(&DocumentId::from(bytes), &context);
        document.subscribe().await;
        println!("[note] subscribed to {id_hex}");

        let mut last = String::new();
        let mut typed = false;
        for tick in 0..seconds {
            glib::timeout_future(Duration::from_secs(1)).await;
            if !typed && tick >= 5 {
                if let Some(line) = &line {
                    let end = document.text().chars().count() as i32;
                    document
                        .insert_text(end, &format!("{line}\n"))
                        .expect("insert");
                    println!("[note] typed {line:?}");
                }
                typed = true;
            }
            let text = document.text();
            if text != last {
                println!("[note] +{tick}s text = {text:?}");
                last = text;
            }
        }
        document.unsubscribe().await;
        service.shutdown().await;
    });
}
