use osunbitdb::{OsunbitDB, json, increment, remove, array_union, array_remove};

#[tokio::test]
async fn raw_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 [START] RAW TEST INITIALIZED");

    // --------------------------
    // 🔗 Connect to cluster
    // --------------------------
    println!("🌐 Connecting to TiKV PD...");
    let db = OsunbitDB::new(&["http://127.0.0.1:2379"]).await?;
    println!("✅ Connected successfully.\n");

    // --------------------------
    // 🧍 Basic CRUD
    // --------------------------
    println!("===== 🧍 BASIC CRUD TEST =====");
    let user = json!({ "id": "u1", "name": "Alice", "age": 25 });

    println!("➕ Adding user u1...");
    db.add("users", "u1", &user).await?;
    println!("✅ Added user u1.");

    let fetched = db.get("users", "u1").await?.unwrap();
    println!("📦 Retrieved user u1: {:?}", fetched);
    assert_eq!(fetched["name"], "Alice");
    assert_eq!(fetched["age"], 25);

    println!("🧾 Updating user u1 (age -> 26, active -> true)...");
    db.update("users", "u1", &json!({ "age": 26, "active": true })).await?;
    let updated = db.get("users", "u1").await?.unwrap();
    println!("📦 Updated user u1: {:?}", updated);
    assert_eq!(updated["age"], 26);
    assert_eq!(updated["active"], true);

    println!("🗑️ Deleting user u1...");
    db.delete("users", "u1").await?;
    let deleted = db.get("users", "u1").await?;
    println!("📦 After delete (should be None): {:?}", deleted);
    assert!(deleted.is_none());

    // --------------------------
    // 📂 Collections & Subcollections
    // --------------------------
    println!("\n===== 📂 COLLECTION TEST =====");
    println!("➕ Adding message to users:u1:inbox...");
    db.add("users:u1:inbox", "m1", &json!({
        "title": "Hello",
        "body": "First message"
    })).await?;
    println!("✅ Message m1 added.");

    let msg = db.get("users:u1:inbox", "m1").await?.unwrap();
    println!("📦 Inbox message fetched: {:?}", msg);
    assert_eq!(msg["title"], "Hello");

    println!("➕ Adding sub-message in users:u1:inbox:group1...");
    db.add("users:u1:inbox:group1", "g1msg", &json!({
        "title": "Group message"
    })).await?;
    let submsg = db.get("users:u1:inbox:group1", "g1msg").await?.unwrap();
    println!("📦 Subcollection message fetched: {:?}", submsg);
    assert_eq!(submsg["title"], "Group message");

    // --------------------------
    // 🔄 Increment / Remove / Array Ops
    // --------------------------
    println!("\n===== 🔄 FIELD UPDATE TEST =====");
    println!("➕ Creating user u2...");
    db.add("users", "u2", &json!({"balance": 100, "role": "admin"})).await?;

    println!("💰 Incrementing balance by 25...");
    db.update("users", "u2", &json!({
        "balance": increment(25)
    })).await?;
    let after_inc = db.get("users", "u2").await?.unwrap();
    println!("📦 After increment: {:?}", after_inc);
    assert_eq!(after_inc["balance"], 125);

    println!("💸 Decrementing balance by 5...");
    db.update("users", "u2", &json!({
        "balance": increment(-5)
    })).await?;
    let after_dec = db.get("users", "u2").await?.unwrap();
    println!("📦 After decrement: {:?}", after_dec);
    assert_eq!(after_dec["balance"], 120);

    println!("🚮 Removing field 'role'...");
    db.update("users", "u2", &json!({
        "role": remove()
    })).await?;
    let after_remove = db.get("users", "u2").await?.unwrap();
    println!("📦 After field remove: {:?}", after_remove);
    assert!(after_remove.get("role").is_none());

    println!("🧱 Updating nested fields (profile.*)...");
    db.update("users", "u2", &json!({
        "profile.points": increment(5)
    })).await?;
    db.update("users", "u2", &json!({
        "profile.badges": remove()
    })).await?;

    println!("🏷️ Array operations (union/remove)...");
    db.update("users", "u2", &json!({
        "tags": array_union(json!(["rust", "db"]))
    })).await?;
    db.update("users", "u2", &json!({
        "tags": array_remove(json!(["rust"]))
    })).await?;
    let after_arrays = db.get("users", "u2").await?.unwrap();
    let tags = after_arrays["tags"].as_array().unwrap();
    println!("📦 After array ops: {:?}", tags);
    assert_eq!(tags, &vec![json!("db")]);

    // --------------------------
    // ⏰ ExpiryAt test
    // --------------------------
    println!("\n===== ⏰ EXPIRY TEST =====");
    let exp_doc = json!({
        "id": "exp1",
        "name": "WillExpire",
        "expiryAt": "02-10-2030"
    });
    db.update("sessions", "exp1", &exp_doc).await?;
    println!("✅ Added expiry doc sessions:exp1");

    let expfetched = db.get("sessions", "exp1").await?.unwrap();
    println!("📦 Expiry fetched: {:?}", expfetched);
    assert_eq!(expfetched["expiryAt"], "02-10-2030");

    // --------------------------
    // ✅ Final confirmation
    // --------------------------
    println!("\n🎉 ✅ All CRUD, array, expiry, and transaction tests passed!");
    Ok(())
}
