use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use lazy_static::lazy_static;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use rand::Rng;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::time::{Duration, sleep};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Serialize, Deserialize)]
pub struct FederatedInstance {
    pub federated_instances: FederatedInstancesData,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FederatedInstancesData {
    pub allowed: Vec<serde_json::Value>,
    pub blocked: Vec<serde_json::Value>,
    pub linked: Vec<LinkedInstance>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LinkedInstance {
    pub domain: String,
    pub id: u32,
    pub published: String,
    pub software: Option<String>,
    pub updated: Option<String>,
    pub version: Option<String>,
}

lazy_static! {
    static ref SEEN_DOMAINS: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    static ref LEMMY_COUNTER: AtomicUsize = AtomicUsize::new(0);
}


async fn send_domain_to_workers(tx: &Sender<String>, domain: &str) {
    let mut seen = SEEN_DOMAINS.lock().await;

    if !seen.contains(domain) {
        tx.send(domain.to_string()).await.expect("Error sending domain to channel");
        println!("Sending domain to workers: {}", domain);
    }
}

fn get_counter_value() -> usize {
    LEMMY_COUNTER.load(Ordering::Relaxed)
}

async fn remember_domain(domain: &str) -> bool {
    let mut seen = SEEN_DOMAINS.lock().await;
    seen.insert(domain.to_string())
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(5000);
    let (fi_tx, mut fi_rx) = mpsc::channel(5000); // New channel for FederatedInstance data

    let workers = spawn_workers(10, fi_tx);

    tx.send("lemmy.world".to_string()).await.expect("Couldnt send initial message.");
    spawn_federated_instance_handler(tx.clone(), fi_rx);
    dispatcher(&mut rx, workers).await;

    // Keep the main function from exiting immediately
    // This is a simple solution to keep the program running indefinitely.
    // Replace it with your actual application logic as needed.
    // let _: () = tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
}

fn spawn_federated_instance_handler(tx: Sender<String>, mut fi_rx: Receiver<FederatedInstance>) {
    tokio::spawn(async move {
        while let Some(federated_instance) = fi_rx.recv().await {
            for item in federated_instance.federated_instances.linked {
                if let Some(software) = item.software {
                    if software == "lemmy" {
                        send_domain_to_workers(&tx, &item.domain.clone()).await;
                    }
                }

                remember_domain(&item.domain).await; // Remember all domains, non-lemmy domains too
            }
        }
    });
}

async fn dispatcher(rx: &mut Receiver<String>, workers: Vec<Sender<String>>) {
// Dispatcher: As tasks come in, send them to workers.
    let mut current_worker = 0;
    while let Some(task) = rx.recv().await {
        workers[current_worker].send(task).await.expect("Failed to send to worker");
        current_worker = (current_worker + 1) % workers.len();
    }
}

fn spawn_workers(num_workers: usize, fi_tx: Sender<FederatedInstance>) -> Vec<Sender<String>> {
    let mut worker_channels: Vec<mpsc::Sender<String>> = Vec::new();
    let client = Arc::new(reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap());

    // Spawn workers and save their senders.
    for id in 0..num_workers {
        let (worker_tx, mut worker_rx) = mpsc::channel::<String>(1000);
        worker_channels.push(worker_tx);

        let client_clone = client.clone();
        let fi_tx_clone = fi_tx.clone();

        tokio::spawn(async move {
            while let Some(task) = worker_rx.recv().await {
                match do_work(id, task, &client_clone).await {
                    Some(x) => {
                        match fi_tx_clone.send(x).await {
                            Ok(_) => {
                                // println!("Send was fine.");
                                // Sent the data successfully. Do nothing or add logging if desired.
                            }
                            Err(err) => {
                                eprintln!("Failed to send FederatedInstance data. Error: {:?}", err);
                            }
                        }
                    }
                    None => {
                        // println!("No work to be done.");
                    }
                }
            }

        });
    }
    worker_channels
}

async fn do_work(id: usize, domain: String, client: &Arc<Client>) -> Option<FederatedInstance> {

    // Do the work here...
    let instance_url = format!("https://{}/api/v3/federated_instances", domain);
    let response = client.get(&instance_url).send().await;
    match response {
        Ok(resp) => {
            let instance = resp.json::<FederatedInstance>().await;
            match instance {
                Ok(instance) => {
                    println!("Worker {} finished task: {}", id, domain);
                    LEMMY_COUNTER.fetch_add(1, Ordering::Relaxed);
                    println!("Lemmy instances processed: {}", get_counter_value());
                    return Some(instance);
                },
                Err(e) => {
                    eprintln!("{} - Error deserializing response: {:?}", domain, e.source().unwrap().to_string());
                    None
                }
            }
        },
        Err(e) => {
            eprintln!("{} - Error sending request: {:?}", domain, e.source().unwrap().to_string());
            None
        }
    }
}