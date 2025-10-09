use osunbitdb::{OsunbitDB, json};

#[tokio::test]
async fn batch_operations_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Connecting to TiKV PD...");
    let db = OsunbitDB::new(&["http://127.0.0.1:2379"]).await?;
    println!("✅ Connected to TiKV PD.");

    // --------------------------------------------------------------------
    // 🧩 CLIENT MODE — Auto-commit on each call
    // --------------------------------------------------------------------
    println!("\n================ CLIENT MODE TEST ================");

    let batch_docs = json!({
        "tx1": { "amount": 100, "type": "send", "status": "success" },
        "tx2": { "amount": 200, "type": "receive", "status": "success" },
        "tx3": { "amount": 50,  "type": "withdraw", "status": "pending" }
    });

    println!("➕ Adding 3 batch items via client...");
    db.batch_add("transactions:u1", &batch_docs).await?;
    println!("✅ Batch add complete.");

    println!("🔍 Fetching all 3 items...");
    let ids = json!(["tx1", "tx2", "tx3"]);
    let fetched = db.batch_get("transactions:u1", &ids).await?;
    println!("📦 Batch fetch result: {:#?}", fetched);

    assert_eq!(fetched["tx1"]["amount"], 100);
    assert_eq!(fetched["tx2"]["type"], "receive");
    assert_eq!(fetched["tx3"]["status"], "pending");
    println!("✅ All fetched values match.");

    println!("❌ Deleting all 3 items...");
    db.batch_delete("transactions:u1", &ids).await?;
    println!("✅ Batch delete done.");

    let after_delete = db.batch_get("transactions:u1", &ids).await?;
    assert!(after_delete.as_object().unwrap().is_empty());
    println!("✅ Confirmed all items deleted.");

    // --------------------------------------------------------------------
    // 🔒 TRANSACTION MODE — Manual control (commit/rollback)
    // --------------------------------------------------------------------
    println!("\n================ TRANSACTION MODE TEST ================");

    let mut tx = db.transaction().await?;
    println!("🟢 Started new transaction.");

    // Add multiple docs in one atomic block
    let batch_docs_tx = json!({
        "t1": { "product": "Book", "price": 50 },
        "t2": { "product": "Laptop", "price": 1200 },
        "t3": { "product": "Phone", "price": 600 }
    });

    println!("➕ Adding docs atomically...");
    tx.batch_add("store:cart", &batch_docs_tx).await?;

    // Get them (still uncommitted)
    println!("🔍 Fetching before commit...");
    let ids_tx = json!(["t1", "t2", "t3"]);
    let before_commit = tx.batch_get("store:cart", &ids_tx).await?;
    println!("📦 Transaction (uncommitted) fetch: {:#?}", before_commit);
    assert_eq!(before_commit["t2"]["product"], "Laptop");

    println!("💾 Committing transaction...");
    tx.commit().await?;
    println!("✅ Transaction committed successfully.");

    // Verify from client (outside transaction)
    println!("🔍 Re-fetching after commit from client...");
    let after_commit = db.batch_get("store:cart", &ids_tx).await?;
    println!("📦 After commit fetch: {:#?}", after_commit);
    assert_eq!(after_commit["t3"]["price"], 600);

    println!("🧹 Cleaning up (deleting store:cart docs)...");
    db.batch_delete("store:cart", &ids_tx).await?;
    println!("✅ Cleanup done.");

    println!("\n🎉 All batch + transaction tests completed successfully!");
    Ok(())
}
