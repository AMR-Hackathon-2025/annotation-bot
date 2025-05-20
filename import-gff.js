#!/usr/bin/env node

/**
 * GFF Folder Import Script
 * 
 * This script scans a specified folder for GFF files,
 * processes each file, and imports the data into the PostgreSQL database.
 * 
 * Usage: node import_gff_folder.js /path/to/gff/folder
 */

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const readline = require('readline');

// Check command line arguments
if (process.argv.length < 3) {
  console.error('Usage: node import_gff_folder.js /path/to/gff/folder');
  process.exit(1);
}

// Get the folder path from command-line argument
const folderPath = process.argv[2];

// Verify the folder exists
if (!fs.existsSync(folderPath) || !fs.lstatSync(folderPath).isDirectory()) {
  console.error(`Error: The specified path "${folderPath}" is not a valid directory.`);
  process.exit(1);
}

// Database configuration - update with your credentials
const pool = new Pool({
  connectionString: process.env.POSTGRES_DB_URL || 'postgres://username:password@localhost:5432/gff_annotations',
});

// Function to check if a file is a GFF file
function isGffFile(filename) {
  const lowerFilename = filename.toLowerCase();
  return lowerFilename.endsWith('.gff') || lowerFilename.endsWith('.gff3');
}

/**
 * Parse GFF file and store data in PostgreSQL database
 * @param {string} filePath - Path to the GFF file
 * @returns {Promise<number>} - ID of the file in the database
 */
async function processGffFile(filePath) {
  const fileName = path.basename(filePath);
  const fileStats = await fs.promises.stat(filePath);
  const fileSize = fileStats.size;
  
  console.log(`Processing file: ${fileName} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);
  
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Insert file metadata
    const fileMetadata = {
      filename: fileName,
      original_path: filePath,
      file_size: fileSize,
      version: null,
      genome_build: null,
      genome_build_accession: null,
      annotation_date: null,
      annotation_source: null
    };
    
    // Check if file with the same name already exists in database
    const existingFile = await client.query(
      'SELECT file_id FROM gff_files WHERE filename = $1',
      [fileName]
    );
    
    let fileId;
    
    if (existingFile.rows.length > 0) {
      // File exists, update instead of insert
      fileId = existingFile.rows[0].file_id;
      console.log(`File ${fileName} already exists in database with ID ${fileId}. Updating...`);
      
      // Delete existing features and sequence regions for this file
      await client.query('DELETE FROM gff_features WHERE file_id = $1', [fileId]);
      await client.query('DELETE FROM sequence_regions WHERE file_id = $1', [fileId]);
      
      // Update file metadata
      await client.query(
        'UPDATE gff_files SET original_path = $1, file_size = $2, upload_date = CURRENT_TIMESTAMP WHERE file_id = $3',
        [fileMetadata.original_path, fileMetadata.file_size, fileId]
      );
    } else {
      // Insert new file record and get the ID
      const fileInsertResult = await client.query(
        'INSERT INTO gff_files (filename, original_path, file_size, version, genome_build, genome_build_accession, annotation_date, annotation_source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING file_id',
        [fileMetadata.filename, fileMetadata.original_path, fileMetadata.file_size, fileMetadata.version, fileMetadata.genome_build, fileMetadata.genome_build_accession, fileMetadata.annotation_date, fileMetadata.annotation_source]
      );
      
      fileId = fileInsertResult.rows[0].file_id;
    }
    
    // Read the file line by line
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let lineCount = 0;
    let featureCount = 0;
    let sequenceRegionCount = 0;
    let batchValues = [];
    const BATCH_SIZE = 1000; // Insert features in batches for better performance
    
    for await (const line of rl) {
      lineCount++;
      
      // Status update every 100k lines
      if (lineCount % 100000 === 0) {
        console.log(`Processed ${lineCount.toLocaleString()} lines...`);
      }
      
      // Skip empty lines
      if (!line.trim()) {
        continue;
      }
      
      // Process header lines
      if (line.startsWith('#')) {
        if (line.startsWith('##gff-version')) {
          const version = line.split(' ')[1];
          await client.query('UPDATE gff_files SET version = $1 WHERE file_id = $2', [version, fileId]);
        } else if (line.startsWith('#!genome-build')) {
          const genomeBuild = line.split(' ')[1];
          await client.query('UPDATE gff_files SET genome_build = $1 WHERE file_id = $2', [genomeBuild, fileId]);
        } else if (line.startsWith('#!genome-build-accession')) {
          const accession = line.split(' ')[1];
          await client.query('UPDATE gff_files SET genome_build_accession = $1 WHERE file_id = $2', [accession, fileId]);
        } else if (line.startsWith('#!annotation-date')) {
          const annotDate = line.split(' ')[1];
          await client.query('UPDATE gff_files SET annotation_date = $1 WHERE file_id = $2', [annotDate, fileId]);
        } else if (line.startsWith('#!annotation-source')) {
          const source = line.replace('#!annotation-source ', '');
          await client.query('UPDATE gff_files SET annotation_source = $1 WHERE file_id = $2', [source, fileId]);
        } else if (line.startsWith('##sequence-region')) {
          const parts = line.split(' ');
          if (parts.length >= 4) {
            await client.query(
              'INSERT INTO sequence_regions (file_id, seqid, start, "end") VALUES ($1, $2, $3, $4)',
              [fileId, parts[1], parseInt(parts[2]), parseInt(parts[3])]
            );
            sequenceRegionCount++;
          }
        } else if (line.startsWith('##species')) {
          const match = line.match(/##species\s+(.+)/);
          if (match && match[1]) {
            const speciesUrl = match[1];
            // Find the last sequence region added for this file and update it
            await client.query(
              'UPDATE sequence_regions SET species_url = $1 WHERE region_id = (SELECT MAX(region_id) FROM sequence_regions WHERE file_id = $2)',
              [speciesUrl, fileId]
            );
          }
        }
        continue;
      }
      
      // Process data lines
      const columns = line.split('\t');
      if (columns.length < 9) continue; // Skip malformed lines
      
      const [seqid, source, type, start, end, score, strand, phase, attributesStr] = columns;
      
      // Parse attributes
      const attributesObj = {};
      if (attributesStr) {
        const attrPairs = attributesStr.split(';');
        for (const pair of attrPairs) {
          if (!pair.includes('=')) {
            const colonSplit = pair.split(':');
            if (colonSplit.length === 2) {
              attributesObj[colonSplit[0].trim()] = colonSplit[1].trim();
            }
          } else {
            const [key, value] = pair.split('=');
            if (key && value) {
              attributesObj[key.trim()] = value.trim();
            }
          }
        }
      }
      
      // Add to batch
      batchValues.push([
        fileId, 
        seqid, 
        source, 
        type, 
        parseInt(start) || 1, 
        parseInt(end) || 1, 
        score === '.' ? null : score, 
        strand === '.' ? null : strand, 
        phase === '.' ? null : phase, 
        JSON.stringify(attributesObj)
      ]);
      
      featureCount++;
      
      // Execute batch insert when batch is full
      if (batchValues.length >= BATCH_SIZE) {
        await insertFeatureBatch(client, batchValues);
        batchValues = [];
      }
    }
    
    // Insert any remaining features
    if (batchValues.length > 0) {
      await insertFeatureBatch(client, batchValues);
    }
    
    await client.query('COMMIT');
    console.log(`GFF file processed successfully:`);
    console.log(`- File ID: ${fileId}`);
    console.log(`- Lines processed: ${lineCount.toLocaleString()}`);
    console.log(`- Features imported: ${featureCount.toLocaleString()}`);
    console.log(`- Sequence regions: ${sequenceRegionCount}`);
    
    return fileId;
    
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error processing GFF file:', err);
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Insert a batch of features into the database
 * @param {Object} client - PostgreSQL client
 * @param {Array} featureBatch - Array of feature data arrays
 */
async function insertFeatureBatch(client, featureBatch) {
  // Prepare the SQL query with placeholders for all values
  let placeholders = [];
  let values = [];
  let index = 1;
  
  for (let i = 0; i < featureBatch.length; i++) {
    const row = featureBatch[i];
    // Generate placeholders for this row ($1, $2, $3, ...)
    const rowPlaceholders = [];
    for (let j = 0; j < row.length; j++) {
      rowPlaceholders.push(`$${index}`);
      values.push(row[j]);
      index++;
    }
    placeholders.push(`(${rowPlaceholders.join(', ')})`);
  }
  
  const query = `
    INSERT INTO gff_features 
    (file_id, seqid, source, type, start, "end", score, strand, phase, attributes) 
    VALUES ${placeholders.join(', ')}
  `;
  
  await client.query(query, values);
}

/**
 * Main function to scan folder and process GFF files
 */
async function processGffFolder() {
  try {
    console.log(`Scanning folder: ${folderPath}`);
    
    // List all files in the directory
    const files = await fs.promises.readdir(folderPath);
    const gffFiles = files.filter(isGffFile);
    
    console.log(`Found ${gffFiles.length} GFF file(s)`);
    
    // Process each GFF file sequentially
    for (let i = 0; i < gffFiles.length; i++) {
      const file = gffFiles[i];
      const filePath = path.join(folderPath, file);
      
      console.log(`\nProcessing file ${i + 1} of ${gffFiles.length}: ${file}`);
      
      try {
        await processGffFile(filePath);
      } catch (err) {
        console.error(`Failed to process file ${file}:`, err);
      }
    }
    
    console.log('\nAll files processed.');
  } catch (err) {
    console.error('Error scanning folder:', err);
  } finally {
    // Close the database pool
    await pool.end();
  }
}

// Run the main function
processGffFolder().then(() => {
  console.log('Script completed.');
}).catch(err => {
  console.error('Script failed:', err);
  process.exit(1);
});