use osunbitdb::{OsunbitDB, json};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn scan_test_straightforward() -> Result<(), Box<dyn std::error::Error>> {
    let db = OsunbitDB::new(&["http://127.0.0.1:2379"]).await?;
    let batch_size = 5;
    let inbox_col = "inbox_test:u1";
    let txn_col = "transaction_test:u1";

    println!("\n🚀 Starting clean scan test flow\n============================================================");

    // ============================================================
    // 🏗️ CREATE COLLECTIONS
    // ============================================================
    println!("\n📦 Creating 50 inbox docs (padded IDs)...");
    let mut inbox_docs = serde_json::Map::new();
    for i in 1..=50 {
        let id = format!("inbox_{:03}", i); // ✅ padded
        inbox_docs.insert(id.clone(), json!({
            "index": i,
            "label": format!("Inbox message {}", i)
        }));
        sleep(Duration::from_millis(2)).await;
    }
    db.batch_add(inbox_col, &json!(inbox_docs)).await?;
    println!("✅ Inserted 50 inbox docs.\n");

    println!("\n📦 Creating 50 transaction docs (padded IDs)...");
    let mut txn_docs = serde_json::Map::new();
    for i in 1..=50 {
        let id = format!("transaction_{:03}", i); // ✅ padded
        txn_docs.insert(id.clone(), json!({
            "index": i,
            "label": format!("Transaction record {}", i)
        }));
        sleep(Duration::from_millis(2)).await;
    }
    db.batch_add(txn_col, &json!(txn_docs)).await?;
    println!("✅ Inserted 50 transaction docs.\n");

    // ============================================================
    // 🔍 SCAN INBOX COLLECTION (ASCENDING)
    // ============================================================
    println!("\n🔼 Scanning `{inbox_col}` in ASCENDING order (5 docs per batch)");
    println!("------------------------------------------------------------");

    let mut cursor = String::new();
    for batch_no in 1..=10 {
        let res = db.scan(inbox_col, batch_size, &cursor, "a").await?;
        let obj = res.as_object().cloned().unwrap_or_default();
        if obj.is_empty() {
            println!("⏹️  No more docs (ASC) after batch {batch_no}");
            break;
        }

        let mut entries: Vec<(String, i64)> = obj.iter()
            .filter_map(|(k, v)| v["index"].as_i64().map(|i| (k.clone(), i)))
            .collect();
        entries.sort_by_key(|(_, idx)| *idx);

        let start = entries.first().unwrap().1;
        let end = entries.last().unwrap().1;

        println!("🧩 ASC Batch {batch_no:02} → index {start} → {end}");
        for (k, v) in &entries {
            println!("   🔸 {k:<20} | index={v}");
        }

        cursor = entries.last().unwrap().0.clone();
            println!("   ↪️ Cursor (last entry mode) = \"{cursor}\"\n");

        if entries.len() < batch_size as usize {
            println!("✅ ASC scan completed after {batch_no} batches.\n");
            break;
        }
    }

    // ============================================================
    // 🔽 SCAN INBOX COLLECTION (DESCENDING)
    // ============================================================
    println!("\n🔽 Scanning `{inbox_col}` in DESCENDING order (5 docs per batch)");
    println!("------------------------------------------------------------");

    let mut cursor = String::new();
    for batch_no in 1..=10 {
        let res = db.scan(inbox_col, batch_size, &cursor, "d").await?;
        let obj = res.as_object().cloned().unwrap_or_default();
        if obj.is_empty() {
            println!("⏹️  No more docs (DESC) after batch {batch_no}");
            break;
        }

        let mut entries: Vec<(String, i64)> = obj.iter()
            .filter_map(|(k, v)| v["index"].as_i64().map(|i| (k.clone(), i)))
            .collect();
        entries.sort_by_key(|(_, idx)| -(*idx));

        let start = entries.first().unwrap().1;
        let end = entries.last().unwrap().1;

        println!("🧩 DESC Batch {batch_no:02} → index {start} → {end}");
        for (k, v) in &entries {
            println!("   🔹 {k:<20} | index={v}");
        }

         cursor = entries.last().unwrap().0.clone();
            println!("   ↪️ Cursor (last entry mode) = \"{cursor}\"\n");

        if entries.len() < batch_size as usize {
            println!("✅ DESC scan completed after {batch_no} batches.\n");
            break;
        }
    }

    // ============================================================
    // 🔍 SCAN TRANSACTION COLLECTION (ASCENDING)
    // ============================================================
    println!("\n🔼 Scanning `{txn_col}` in ASCENDING order (5 docs per batch)");
    println!("------------------------------------------------------------");

    let mut cursor = String::new();
    for batch_no in 1..=10 {
        let res = db.scan(txn_col, batch_size, &cursor, "a").await?;
        let obj = res.as_object().cloned().unwrap_or_default();
        if obj.is_empty() {
            println!("⏹️  No more docs (ASC) after batch {batch_no}");
            break;
        }

        let mut entries: Vec<(String, i64)> = obj.iter()
            .filter_map(|(k, v)| v["index"].as_i64().map(|i| (k.clone(), i)))
            .collect();
        entries.sort_by_key(|(_, idx)| *idx);

        let start = entries.first().unwrap().1;
        let end = entries.last().unwrap().1;

        println!("🧩 ASC Batch {batch_no:02} → index {start} → {end}");
        for (k, v) in &entries {
            println!("   🔸 {k:<20} | index={v}");
        }

       cursor = entries.last().unwrap().0.clone();
            println!("   ↪️ Cursor (last entry mode) = \"{cursor}\"\n");

        if entries.len() < batch_size as usize {
            println!("✅ ASC scan completed after {batch_no} batches.\n");
            break;
        }
    }

    // ============================================================
    // 🔽 SCAN TRANSACTION COLLECTION (DESCENDING)
    // ============================================================
    println!("\n🔽 Scanning `{txn_col}` in DESCENDING order (5 docs per batch)");
    println!("------------------------------------------------------------");

    let mut cursor = String::new();
    for batch_no in 1..=10 {
        let res = db.scan(txn_col, batch_size, &cursor, "d").await?;
        let obj = res.as_object().cloned().unwrap_or_default();
        if obj.is_empty() {
            println!("⏹️  No more docs (DESC) after batch {batch_no}");
            break;
        }

        let mut entries: Vec<(String, i64)> = obj.iter()
            .filter_map(|(k, v)| v["index"].as_i64().map(|i| (k.clone(), i)))
            .collect();
        entries.sort_by_key(|(_, idx)| -(*idx));

        let start = entries.first().unwrap().1;
        let end = entries.last().unwrap().1;

        println!("🧩 DESC Batch {batch_no:02} → index {start} → {end}");
        for (k, v) in &entries {
            println!("   🔹 {k:<20} | index={v}");
        }

        cursor = entries.last().unwrap().0.clone();
            println!("   ↪️ Cursor (last entry mode) = \"{cursor}\"\n");

        if entries.len() < batch_size as usize {
            println!("✅ DESC scan completed after {batch_no} batches.\n");
            break;
        }
    }

    // ============================================================
    // 🧹 CLEANUP
    // ============================================================
    println!("\n🧹 Cleaning up...");
    let inbox_ids: Vec<String> = inbox_docs.keys().cloned().collect();
    let txn_ids: Vec<String> = txn_docs.keys().cloned().collect();
    db.batch_delete(inbox_col, &json!(inbox_ids)).await?;
    db.batch_delete(txn_col, &json!(txn_ids)).await?;
    println!("✅ All docs deleted successfully.\n");

    println!("🎯 Test complete — full scan flow verified.\n============================================================");
    Ok(())
}
