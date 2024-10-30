const mysql = require('mysql2');

// Set up MySQL connection pool
const pool = mysql.createPool({
    host: 'srv1391.hstgr.io',
    user: 'u858543158_arpita',
    password: '2n:O!5:V',
    database: 'u858543158_arpitaDb',
    waitForConnections: true,
    connectionLimit: 10,  // Set the number of concurrent connections
    queueLimit: 0
});

const express = require('express');
const app = express();
app.use(express.json());

const cors = require('cors');
app.use(cors());

// Function to get current time in Indian Time Zone (IST)
function getIndianTime() {
    const indianTimeOffset = 5.5 * 60 * 60 * 1000;  // Offset for IST in milliseconds
    const currentTime = new Date();
    return new Date(currentTime.getTime() + indianTimeOffset).toISOString().slice(0, 19).replace('T', ' ');
}

// Function to save history
function saveHistoryData(tableName, csvData, exportedRecordCount, res) {
    const indianTime = getIndianTime();  // Get the current time in IST
    const insertQuery = `INSERT INTO \`${tableName}\` (csv_data, created_at, exportedrecordscount) VALUES (?, ?, ?)`;
    
    pool.query(insertQuery, [csvData, indianTime, exportedRecordCount], (err, result) => {
        if (err) {
            console.error('Error inserting data into history table:', err.message);
            return res.status(500).send('Error inserting data into history table.');
        }
        res.send('Export history saved successfully.');
    });
}


// Route to check/create history table and store export history
app.post('/saveExportHistory', (req, res) => {
    const { apikey, dataToExport } = req.body;
    const tableName = `${apikey}_history`;  // Construct table name
    const exportedRecordCount = dataToExport.length; // Count of exported records

    const checkTableQuery = `SHOW TABLES LIKE ?`;

    pool.query(checkTableQuery, [tableName], (err, result) => {
        if (err) {
            console.error('Error checking table:', err.message);
            return res.status(500).send('Error checking table.');
        }

        if (result.length === 0) {
            // Table does not exist, create it
            const createTableQuery = `
                CREATE TABLE \`${tableName}\` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    csv_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    exportedrecordscount INT NOT NULL
                )`;
            pool.query(createTableQuery, (err, result) => {
                if (err) {
                    console.error('Error creating table:', err.message);
                    return res.status(500).send('Error creating table.');
                }

                // Insert export history after creating table
                saveHistoryData(tableName, JSON.stringify(dataToExport), exportedRecordCount, res);
            });
        } else {
            // Table exists, insert export history
            saveHistoryData(tableName, JSON.stringify(dataToExport), exportedRecordCount, res);
        }
    });
});


// API to fetch all records from the history table based on apikey
app.get('/getExportHistory', (req, res) => {
    const apiKey = req.headers['x-api-key'];  // apikey from headers
    const tableName = `${apiKey}_history`;

    const selectQuery = `SELECT id, csv_data, created_at, exportedrecordscount FROM \`${tableName}\``;

    pool.query(selectQuery, (err, results) => {
        if (err) {
            console.error('Error fetching records:', err.message);
            return res.status(500).send('Error fetching records.');
        }

        res.json(results);  // Send the results as JSON to the frontend
    });
});


// Function to check if a table exists
const checkTableExists = (tableName, callback) => {
    pool.query(
        `SHOW TABLES LIKE ?`,
        [tableName],
        (error, results) => {
            if (error) {
                return callback(error);
            }
            callback(null, results.length > 0);
        }
    );
};

// API to export csv_data as CSV file based on created_at timestamp
app.get('/downloadCsv/:api_key/:id', (req, res) => {
    const apiKey = req.params.api_key;
    const id = req.params.id;  // Get the id from the request params
    const tableName = `${apiKey}_history`;

    // Fields to exclude from the CSV export
    const excludeFields = [
        'updated', 'emailStatus', 'companyStatus', 'jobTitleStatus', 'saved', 
        'headquarter_address', 'head_city', 'head_state', 'campaign_id', 'maskedEmail'
    ];

    // Check if the table exists
    checkTableExists(tableName, (error, exists) => {
        if (error) {
            console.error('Error checking table existence:', error);
            return res.status(500).json({ error: 'Internal Server Error' });
        }
        
        if (!exists) {
            return res.status(404).json({ error: 'Table not found' });
        }

        // If table exists, query the CSV data using id
        pool.query(`SELECT csv_data FROM ?? WHERE id = ?`, [tableName, id], (error, results) => {
            if (error) {
                console.error('Database query failed:', error);
                return res.status(500).json({ error: 'Database query failed.' });
            }

            if (results.length > 0) {
                const jsonData = JSON.parse(results[0].csv_data);

                // Get headers dynamically from the first object in the JSON data
                let headers = Object.keys(jsonData[0]);

                // Filter out the fields that need to be excluded
                headers = headers.filter(header => !excludeFields.includes(header));

                let csvContent = headers.join(',') + '\n'; // Add the filtered headers as the first row

                // Convert JSON data into CSV format (one JSON object per row, with only values for allowed headers)
                jsonData.forEach(row => {
                    const csvRow = headers.map(header => row[header] || '').join(',');
                    csvContent += csvRow + '\n';
                });

                // Set headers for CSV download
                res.setHeader('Content-Type', 'text/csv');
                res.setHeader('Content-Disposition', 'attachment; filename=data.csv');
                res.send(csvContent);  // Send the CSV content
            } else {
                res.status(404).json({ error: 'No data found for the given id.' });
            }
        });
    });
});

// Start the server
const port = process.env.PORT || 4000;
const host = '192.168.0.7'; // Specify the host IP address

app.listen(port, host, () => {
    console.log(`Server started on http://${host}:${port}`);
});


//  exported data count api starts here 
// // First database connection (arpitaDb)
// const firstDbPool = mysql.createPool({
//     host: 'srv1391.hstgr.io',
//     user: 'u858543158_arpita',
//     password: '2n:O!5:V',
//     database: 'u858543158_arpitaDb',
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0
// });

// // Second database connection (technopour)
// async function createSecondDbPool() {
//     return await mysql.createPool({
//         host: 'srv1391.hstgr.io',
//         user: 'u858543158_technopour',
//         password: 'Wvh1z]SL#3',
//         database: 'u858543158_33zBrmCUqoJ7',
//         waitForConnections: true,
//         connectionLimit: 10,
//         queueLimit: 0
//     });
// }

// const secondDbPool = createSecondDbPool();

// // Function to sum exportedrecordscount from first database
// async function getExportedRecordsTotal(api_key) {
//     const tableName = `${api_key}_history`; // Assuming history table is named with api_key + '_history'

//     const query = `SELECT SUM(exportedrecordscount) AS total_exported_count FROM \`${tableName}\``;
//     const [rows] = await firstDbPool.query(query);
//     return rows[0]?.total_exported_count || 0;
// }

// // Function to update exporteddatacount in the user table of the second database
// async function updateExportedDataCount(api_key, totalExportedCount) {
//     const updateQuery = `
//         UPDATE user
//         SET exporteddatacount = ?
//         WHERE email_hash = ?
//     `;

//     const [result] = await (await secondDbPool).query(updateQuery, [totalExportedCount, api_key]);

//     return result.affectedRows > 0;
// }


// app.post('/update-export-count', async (req, res) => {
//     const { api_key } = req.body;

//     if (!api_key) {
//         return res.status(400).json({ error: 'API key is required' });
//     }

//     try {
//         // Step 1: Get the total exported records count from the first database
//         const totalExportedCount = await getExportedRecordsTotal(api_key);

//         // Step 2: Update the exporteddatacount in the user table in the second database
//         const updateSuccess = await updateExportedDataCount(api_key, totalExportedCount);

//         if (updateSuccess) {
//             return res.status(200).json({ message: 'Exported data count updated successfully' });
//         } else {
//             return res.status(404).json({ error: 'User not found or no records updated' });
//         }
//     } catch (error) {
//         console.error('Error updating exported data count:', error);
//         return res.status(500).json({ error: 'Internal server error' });
//     }
// });

