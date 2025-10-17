const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// MSSQL Configuration
const sqlConfig = {
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DATABASE,
  user: process.env.MSSQL_USERNAME,
  password: process.env.MSSQL_PASSWORD,
  port: parseInt(process.env.MSSQL_PORT || '1433'),
  options: {
    encrypt: process.env.MSSQL_ENCRYPT === 'true',
    trustServerCertificate: process.env.MSSQL_TRUST_SERVER_CERTIFICATE === 'true',
    connectTimeout: 30000,
    requestTimeout: 30000,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// Connection pool
let pool = null;

// Initialize connection pool
async function initializePool() {
  try {
    pool = await sql.connect(sqlConfig);
    console.log('Connected to MSSQL database');
    return pool;
  } catch (err) {
    console.error('Database connection failed:', err);
    throw err;
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'mssql-bridge',
    connected: pool && pool.connected 
  });
});

// Root endpoint for health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'mssql-bridge',
    message: 'MSSQL Bridge is running',
    connected: pool && pool.connected 
  });
});

// Main query endpoint
app.post('/', async (req, res) => {
  try {
    const { query, parameters = {} } = req.body;

    if (!query) {
      return res.status(400).json({ 
        error: 'Query is required' 
      });
    }

    // Ensure pool is initialized
    if (!pool || !pool.connected) {
      pool = await initializePool();
    }

    console.log('Executing query:', query.substring(0, 100) + '...');

    // Create request
    const request = pool.request();

    // Add parameters
    Object.entries(parameters).forEach(([key, value]) => {
      request.input(key, value);
    });

    // Execute query
    const result = await request.query(query);
    
    console.log(`Query executed successfully, returned ${result.recordset.length} rows`);

    // Return in the format expected by mssql-connect
    res.json(result.recordset);

  } catch (error) {
    console.error('Query execution error:', error);
    res.status(500).json({ 
      error: error.message,
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing MSSQL connection');
  if (pool) {
    await pool.close();
  }
  process.exit(0);
});

// Start server
app.listen(PORT, async () => {
  console.log(`MSSQL Bridge listening on port ${PORT}`);
  try {
    await initializePool();
  } catch (err) {
    console.error('Failed to initialize database connection');
  }
});
