require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");

const app = express();
const port = process.env.PORT || 5000;

// Database configuration
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

// Test database connection
async function testConnection() {
  try {
    const client = await pool.connect();
    await client.query("SELECT 1");
    client.release();
    return true;
  } catch (error) {
    console.error("Database connection error:", error);
    return false;
  }
}

// Routes
app.get("/", (req, res) => {
  res.json({ message: "Welcome to the PostgreSQL Node.js API" });
});

app.get("/health", async (req, res) => {
  try {
    const isConnected = await testConnection();
    if (isConnected) {
      res.json({ status: "healthy", database: "connected" });
    } else {
      res
        .status(500)
        .json({ status: "unhealthy", error: "Database connection failed" });
    }
  } catch (error) {
    res.status(500).json({ status: "unhealthy", error: error.message });
  }
});

// Start server
app.listen(port, "0.0.0.0", () => {
  console.log(`Server is running on port ${port}`);
});
